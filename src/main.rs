use clap::{Arg, App};
use std::io::prelude::*;
use std::{
    fs::File,
    io::{BufWriter, Write, SeekFrom},
};
use std::fs::OpenOptions;
use std::path::Path;
use std::fs;
use std::ptr::replace;
use std::io::Seek;

use regex::Regex;
use std::time::{Duration, Instant};

use console::{style, Emoji};
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use futures::{stream, StreamExt, TryFutureExt}; // 0.3.5
use tokio; // 0.2.21, features = ["macros"]
use glob::glob;
use serde_json::{Result, Value};
use std::collections::HashMap;

#[macro_use]
use serde_json::json;

#[derive(Hash, Eq, PartialEq, Debug)]
struct Work {
    file_name: String,
    keyword: String,
    batch_id: u64,
    seek_to: u64,
    bytes_to_read: u64,
    output_dir: String,
}

impl Work {
    /// Creates a new Work Peice
    fn new(file_name: &str, batch_id: u64, seek_to: u64,bytes_to_read: u64, output_dir: String, keyword: String) -> Work {
        Work { file_name: file_name.to_string(), keyword: keyword.to_string(), batch_id: batch_id, seek_to: seek_to, bytes_to_read: bytes_to_read, output_dir:output_dir }
    }
}


#[tokio::main]
async fn main() {
    println!("Welcome to Qik FIND (searches a folder of .txt files for a match of keywords and produce a massive.json file with all results!");
    let matches = App::new("QikFind For Large Texts")
        .version("2.1 (19 December 22)")
        .author("")
        .help("crawl over a large set of text files (.txt) to find context sentence per keyword, keywords must be provided in the keywords file seprated by **")
        .arg(Arg::new("search-dir")
            .short('d')
            .long("search-dir")
            .value_name("search_dir")
            .help("Directory containing the uncompressed text files such as /home/vera/data/, ...")
            .takes_value(true)
            .required(true)
            .default_value("./")
        )
        .arg(Arg::new("output-dir")
            .help("Sets the directory in which we write the found records of interest.")
            .required(true)
            .short('o')
            .long("output-dir")
            .value_name("output_dir")
            .takes_value(true)
            .default_value("./output/")
        )
        .arg(Arg::new("keyword-file")
            .help("Set a keyword or Multiple Keywords to look for in text (kw1**kw2**kw3) ...")
            .required(true)
            .short('z')
            .multiple(true)
            .long("keyword-file")
            .value_name("keyword-file")
            .takes_value(true)
            .default_value("islam,quran")
        )
        .arg(Arg::new("threads-per-core")
            .help("Sets the number of threads per core")
            .required(true)
            .short('t')
            .long("threads-per-core")
            .value_name("threads_per_core")
            .takes_value(true)
            .default_value("8")
        )
        .arg(Arg::new("batch-size-in-bytes")
            .help("Sets the batch size in bytes, defaults to 10240 (10 * 1024) Bytes or 10 KB")
            .required(true)
            .short('b')
            .long("batch-size-in-bytes")
            .value_name("batch_size_in_bytes")
            .takes_value(true)
            .default_value("10240")
        )
        .arg(Arg::new("not-dry-run")
            .help("Finds out how much work ahead of us without actually running through the files")
            .short('n')
            .long("not-dry-run")
        )
        .arg(Arg::new("verbose")
            .help("Prints step by step [info] messages, useful for debugging but slower and prints alot of stuff on your temrnial")
            .short('v')
            .long("verbose")
        )
        
        
        .get_matches();

        let mut dry_run = true;
        if matches.is_present("not-dry-run") {
            dry_run = false;
        }

        let mut vvv = false;
        if matches.is_present("verbose") {
            vvv = true;
        }
    let output_dir = matches.value_of("output-dir").unwrap();

    let file_name = matches.value_of("keyword-file").unwrap();
    let mut keywords_ssv = &fs::read_to_string(file_name).unwrap();

    let mut split = keywords_ssv.split("**");
    let mut keys:Vec<String> = split.map(|s| s.to_string()).collect();
    println!("[info] we have {} keywords obtained",keys.len());
    if !output_dir.ends_with("/")
    {
        println!("[Warn] Provided output dir must end with slash /");
        println!("[Warn] Turning back to dry run!!");
        dry_run = true;
    }else {
    	for k in keys.clone(){
            let result_hash = k.as_str();
    		let keyword_path = format!("{}{}",output_dir,result_hash);
        	match fs::create_dir_all(keyword_path.clone()) {
            	Ok(f)  => {
                    if vvv {
                        println!("[info] created outdir: {:?}",keyword_path)
                    }
                },
            	Err(e) => {
                    println!("[Error] Failed to create output dir: {:?}", e);
                }
        	}
    	}
    	
    }
    let ntpc = matches.value_of("threads-per-core").unwrap();
    let search_dir = matches.value_of("search-dir").unwrap();
    let num_threads_per_core = ntpc.parse::<usize>().unwrap();
    let bsib = matches.value_of("batch-size-in-bytes").unwrap().to_string();
    let mut batch_size_in_bytes = 100*1024; // almost sane default
    let mut multiplier = 1;
    if bsib.contains(" K") || bsib.contains(" k") || bsib.contains(" M") || bsib.contains(" m") || bsib.contains(" G") || bsib.contains(" g") {
    	let mut size_part:Vec<String> = bsib.split(" ").map(|s| s.to_string()).collect();
    	match size_part[1].as_str() {
    		"K" | "k" => multiplier=1024,
    		"M" | "m" => multiplier=1024*1024,
    		"G" | "g" => multiplier=1024*1024*1024,
    		_ => {},
    	}
    	batch_size_in_bytes = size_part[0].parse::<u64>().unwrap()*multiplier;
    } else {
    	batch_size_in_bytes = bsib.parse::<u64>().unwrap();
    }
    let mut list_of_works: Vec<Work>=Vec::new();
    let mut total_number_of_jobs: u64 = 0;
    let num = num_cpus::get();
    let PARALLEL_REQUESTS: usize = num*num_threads_per_core;
    for entry in fs::read_dir(search_dir).unwrap() {

        let entry = entry.unwrap();
        let path = entry.path();
        let file_name = entry.file_name();
        let f_name = file_name.to_str().unwrap();

        if f_name.ends_with(".txt") || !f_name.is_empty() {
            println!("Grabbed {:?}", f_name);
            let path_str = path.as_path().display().to_string();
            let mut f = File::open(path).unwrap();
            let metadata = f.metadata().unwrap();
            let file_size_in_bytes = metadata.len();
            if vvv {println!("[info] metadata file size >>> {:?}",file_size_in_bytes)};
            let number_of_batches = file_size_in_bytes / batch_size_in_bytes;
            if vvv {println!("[info] file {:?} will be shredded into {} batches",path_str,number_of_batches);}
            for i in 0..number_of_batches {
                for k in keys.clone(){
                    let result_hash = k.as_str();
                    let keyword_path = format!("{}{}",output_dir,result_hash);
                    list_of_works.push(Work::new(&path_str,i,i*batch_size_in_bytes, batch_size_in_bytes, output_dir.to_string(), result_hash.to_string()));
                    
                }
                
            }
            total_number_of_jobs=total_number_of_jobs+number_of_batches*(keys.len() as u64);
        }


    }
    
    let started = Instant::now();
    let pb = ProgressBar::new(total_number_of_jobs);
    let tpb = ProgressBar::new(total_number_of_jobs*batch_size_in_bytes);
    //let total_time = ProgressBar::new(24*60*60);
    tpb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .progress_chars("#>-"));
    let bodies = stream::iter(list_of_works)
        .map(|work_piece| {
            let vv = vvv.clone();
            tokio::spawn(async move {
                scan_for_keywords(1, work_piece,vv).await
            })
        })
        .buffer_unordered(PARALLEL_REQUESTS);

    bodies
        .for_each(|b| async {
            // b + 2;
            match b {
                Ok(Ok((bb,t))) =>{tpb.inc(bb);if vvv {print!(">>{} in {}<<",bb,t)}},
                Ok(Err(e)) => {/*eprintln!("Error {:?}", e)*/},
                Err(e) => {/*eprintln!("Got a tokio::JoinError: {}", e)*/},
            }
        })
        .await;
    
        tpb.finish();

        println!(
            "{} Taking a breath, phew...",
            style("[4/4]").bold().dim()
        );
        println!("Done in {}", HumanDuration(started.elapsed()));
        println!(
            "{} Combining Results ...",
            style("[4.5/4]").bold().dim()
        );

        let mut ents: HashMap<String,Vec<String>> = HashMap::new();
        
        for kw in glob(format!("{}/**",output_dir).as_str()).expect("Failed to read glob pattern") {
            //println!("{:?}",kw);
            let mut revs: Vec<String>=Vec::new();
            let kw = kw.unwrap().file_name().unwrap().to_str().unwrap().to_string();
            if kw == "..".to_string() || kw == "...".to_string()  || kw == ".".to_string() {continue}
            //println!("{:?}",kw);
            for entry in glob(format!("{}/{}/*.txt",output_dir,kw).as_str()).expect("Failed to read glob pattern")
            {
                //println!("{:?}",entry);
                let e = entry.unwrap();
                if e.to_str().unwrap() == ".." || e.to_str().unwrap() == "." {continue}
                let path = Path::new(&e);
                let f_name = path.file_name().unwrap().to_str().unwrap();
                //let p_name = path.parent().unwrap().file_name().unwrap().to_str().unwrap();
                if f_name.ends_with(".json") || !f_name.is_empty() {
                    //let path_str = path.as_path().display().to_string();
                    let mut f = File::open(path).unwrap();
                    let metadata = f.metadata().unwrap();
                    let file_size_in_bytes = metadata.len();
                    let mut buffer = vec![0; file_size_in_bytes as usize];
                    let a = f.read(&mut buffer).unwrap();
                    let s = String::from_utf8(buffer).unwrap();
                    revs.push(s);
                    
                    //println!("{} ===> {:?}",p_name,revs);
                }
            }
            ents.insert(kw, revs);
        

            
        }
        //println!("{:?}",ents);
        let j = serde_json::to_string_pretty(&ents).unwrap();
        //println!("{:?}",j);
        let mut write_file_j = OpenOptions::new().write(true)
                            .create(true)
                            .append(true)
                            .open(format!("{}/{}.{}", output_dir, "massive", "json")).unwrap();

        write!(write_file_j,"{}",j);

}

async fn scan_for_keywords(num:i32,work:Work, v:bool) -> Result<(u64,u64)>{
	
    //let batch_time = Instant.now();
    let file_name_as_out_part_11 = work.file_name.clone();
    let file_name_slug = Path::new(& file_name_as_out_part_11).file_name().unwrap().to_str().unwrap();

    // let mut file_name_parts11:Vec<&str> = file_name_as_out_part_11.split("/").collect();
    // let mut file_name_slug = file_name_parts11.last();
    let file_name_as_out_part_2 = work.seek_to.clone().to_string();
    let file_name_as_out = format!("{}-{}-{}",file_name_slug,file_name_as_out_part_2,work.seek_to+work.bytes_to_read);
    if v { println!("[info] [In Thread] Looking for keywords:{:?}, in file: {} at segment [ {} <-> {} ]"
    ,work.keyword,work.file_name, work.seek_to, work.seek_to+work.bytes_to_read);}
    let mut f = File::open(work.file_name).unwrap();
    f.seek(SeekFrom::Start(work.seek_to)).unwrap();
    let mut buffer = vec![0; work.bytes_to_read as usize];
    let a = f.read(&mut buffer).unwrap();
    let s = String::from_utf8(buffer).unwrap();
    let mut lines:Vec<&str> = s.split("\n").collect();
    let mut count_valid = 0;
    for line in lines {
        let ss = String::from(line);
        let mut chunks:Vec<&str> = ss.split(" ").collect();
        chunks.truncate(80);
        // println!("{:?} ****************************************** {:?}",ss,chunks.join(" "));
        let mut oo_line = chunks.join(" ");
            // println!("{:?}",oo_line);
            
                // let parsed_line: Value = p;
                count_valid-=-1;
                
                    let result_hash = work.keyword.as_str();//Sha3_256::digest(k.as_bytes());
                    let keyword_path = format!("{}{}",work.output_dir,result_hash);
                    
                    
                    let re = Regex::new(format!(" {} ",result_hash).as_str()).unwrap();
                    let text = oo_line.as_str();
                    if re.is_match(text) {
                        if v {println!("[info] [In Thread] Found a hit line >>>> {:?}",oo_line)};
                        let mut write_file = OpenOptions::new().write(true)
                            .create(true)
                            .append(true)
                            .open(format!("{}/{}.{}", keyword_path, file_name_as_out, "txt")).unwrap();

                        write!(write_file,"{}",text);
                        return Ok((work.bytes_to_read,1))
                    }
                    // if parsed_line["text"].to_string().contains(format!(" {} ",).as_str()) { // need to find with regex
                        
                    // }
                
            
        };
        
    return Ok((work.bytes_to_read,1))
}
