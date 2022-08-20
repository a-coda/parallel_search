extern crate walkdir;
extern crate regex;
extern crate itertools;
extern crate crossbeam_channel as channel;
 
use itertools::Itertools;
use regex::Regex;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use walkdir::WalkDir;
 
const NCONSUMERS: usize = 10;
 
fn main() {
    let args: Vec<String> = env::args().collect();
    match args.len() {
        // no arguments passed (only program itself)
        1 => {
            println!("Usage: cargo run index <map_directory> <source_directory>*");
            println!("Usage: cargo run search <term>*");
        },
        _ => {
            let command         = &args[1];
            let directory       = args[2].to_string();
            let params          = args[3..].to_vec();
            let mut map         = PersistentMultiMap::new(directory);
            match &command[..] {
                "index" => { map.create_index(params); }
                "search" => { map.search(params); }
                _ => { println!("Usage: unknown command: {}", command) }
            }
        }
    }
}
 
fn walk_files(directories: Vec<String>, queue_s: &channel::Sender<Option<(u32,std::sync::Arc<PathBuf>)>>) {
    let mut progress = 0;
    for dir in directories {
        for entry in WalkDir::new(dir) {
            if let Ok(dir_entry) = entry {
                if let Ok(metadata) = dir_entry.metadata() {
                    if metadata.is_file() {
                        progress += 1;
                        let arc_path = Arc::new(dir_entry.path().to_path_buf());
                        queue_s.send(Some((progress, Arc::clone(&arc_path))));
                    }
                }
            }
        }
    }
}
 
// ----------------------------------------------------------------------
 
#[derive(Clone)]
struct PersistentMultiMap {
    directory: PathBuf
}
 
impl PersistentMultiMap {
    fn search(&self, terms: Vec<String>) {
        let result = terms.iter()
            .map(|term| if term.starts_with("-") {(false, &term[1..])} else {(true, &term[..])})
            .map(|(includes, term)| (includes, self.get(term)))
            .fold1(|(_, mut r1), (includes, r2)| { r1.retain(|r| {if includes {r2.contains(r)} else {!r2.contains(r)}}); (true, r1)});
        if let Some((_, answers)) = result {
            for value in answers {
                println!("{}", value);
            }
        }
    }
 
    fn create_index(&mut self, source_directories: Vec<String>) {
        let (queue_s, queue_r) = channel::bounded(100);
        let arc_queue_r = Arc::new(queue_r);
        let mut handles : Vec<JoinHandle<_>> = Vec::new();
        for _ in 0..NCONSUMERS {
            let next_arc_queue_r = Arc::clone(&arc_queue_r);
            let mut next_self = self.clone();
            handles.push(thread::spawn(move || {
                loop {
                    let result: Option<(u32, std::sync::Arc<PathBuf>)> = next_arc_queue_r.recv().unwrap();
                    if let Some((progress, path)) = result { 
                        next_self.visit(progress, &path);
                    }
                    else {
                        break;
                    }
                }
            }));
        };
        walk_files(source_directories, &queue_s);
        for _ in 0..NCONSUMERS {
            queue_s.send(None);
        }
        for h in handles {
            h.join().unwrap();
        }
        self.summarize()
    }
 
    fn new(directory: String) -> PersistentMultiMap {
        fs::create_dir_all(&directory).expect(&format!("directory cannot be created: {}", directory)); 
        PersistentMultiMap{ directory: PathBuf::from(directory) }
    }
 
    fn get_path(&self, key: &str) -> PathBuf {
        self.directory.join(key.to_lowercase())
    }
 
    fn get(&self, key: &str) -> HashSet<String> {
        let mut values = HashSet::new();
        let key_path = self.get_path(&key);
        if key_path.exists() {
            let reader = BufReader::new(File::open(&key_path).expect(&format!("key file does not exist: {}", key)));
            for line in reader.lines() {
                if let Ok(value) = line {
                    values.insert(value);
                }
            }
        }
        values
    }
 
    fn add(&self, key: &str, value: &str) {
        match OpenOptions::new().create(true).append(true).open(&self.get_path(key)) {
            Ok(mut key_file) => {
                if let Err(error) = write!(key_file, "{}\n", value) {
                    println!("Warning: could not write to: {}: {}", key, error);
                }
            }
            Err(error) => {
                println!("Warning: could not append to: {}: {}", key, error);
            }
        }
    }
 
    fn visit(&mut self, progress: u32, path: &Path) {
        if progress % 100 == 0 {
            println!("progress = {}", progress);
        }
        if let Some(source_word_file) = path.to_str() {
            let mut source_file = File::open(path).expect(&format!("source file does not exist: {}", source_word_file));
            let mut source_text = String::new();
            if source_file.read_to_string(&mut source_text).is_ok() {
                let re = Regex::new(r"\W+").expect("illegal regex");
                let source_words: HashSet<&str> = re.split(&source_text).collect();
                for source_word in source_words {
                    if ! source_word.is_empty() {
                        self.add(source_word, source_word_file);
                    }
                }
            }
        }
    }
 
    fn summarize(&self) {
        println!("keys: {}", fs::read_dir(&self.directory).expect(&format!("directory cannot be read: {}", self.directory.to_str().unwrap())).count())
    }
}
