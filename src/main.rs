use flatbuffers::*;
use std::fs;

use bytes::Bytes;
use hashbrown::HashMap;
use rayon::prelude::*;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use clap::Parser;
use std::path::PathBuf;

use memmap2;
use std::thread;


#[derive(Deserialize)]
struct CatalogEntry {
    timestamp: String,
    platform: String,
}

#[allow(dead_code, unused_imports)]
mod schema_generated;
use schema_generated::rman::Manifest;



#[derive(Parser, Debug)]
#[command(name = "Manifest Analyzer")]
#[command(about = "Analyzes Riot RMAN manifests for chunk/file relationships", long_about = None)]
struct Args {
    
    #[arg(default_value = "./manifests")]
    manifest_dir: PathBuf,

   
    #[arg(default_value = ".")]
    out_dir: PathBuf,

   
    #[arg(long, default_value_t = false)]
    no_manifest_tracking: bool,
}

pub struct ManifestDeliverer {
    rx: std::sync::mpsc::Receiver<(u64, bytes::Bytes)>,
    count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl ManifestDeliverer {
    pub fn from_directory(dir_path: &str) -> Self {
        let (tx, rx) = mpsc::sync_channel(20); // Buffer 20 manifests in RAM
        let count = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
    
        let thread_count = Arc::clone(&count);
        let thread_closed = Arc::clone(&closed);
        let path = dir_path.to_string();
    
        thread::spawn(move || {
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    
                   
                    if path.extension().map_or(false, |ext| ext == "manifest") {
                        
                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            if let Ok(manifest_id) = u64::from_str_radix(stem, 16) {
                                if let Ok(data) = fs::read(&path) {
                                    
                                    tx.send((manifest_id, Bytes::from(data))).unwrap();
                                    thread_count.fetch_add(1, Ordering::SeqCst);
                                }
                            }
                        }
                    }
                }
            }
            
            thread_closed.store(true, Ordering::SeqCst);
            println!("Directory deliverer finished scanning.");
        });
    
        Self { rx, count, closed }
    }


    pub fn new_catalog_test() -> Self {
        let (tx, rx) = mpsc::sync_channel(5); // Buffer 5 manifests to prevent RAM bloat
        let count = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));

        let thread_count = Arc::clone(&count);
        let thread_closed = Arc::clone(&closed);

        thread::spawn(move || {
            let client = reqwest::blocking::Client::new();
            let catalog_url = "https://raw.githubusercontent.com/RiotArchiveProject/catalog-download-script/main/catalog.json";

            println!("Fetching catalog...");

            let full_catalog: std::collections::HashMap<
                String,
                std::collections::HashMap<String, CatalogEntry>,
            > = client.get(catalog_url).send().unwrap().json().unwrap();

            let lol_data = full_catalog.get("lol").expect("Product 'lol' not found");

            let mut entries: Vec<_> = lol_data
                .iter()
                .filter(|(_, e)| e.platform == "Windows" || e.platform == "Neutral")
                .collect();

            entries.sort_unstable_by(|a, b| b.1.timestamp.cmp(&a.1.timestamp));

            for (hash, _) in entries.into_iter().take(100) {
                let manifest_id = u64::from_str_radix(&hash, 16).unwrap();

                let url = format!(
                    "https://lol.secure.dyn.riotcdn.net/channels/public/releases/{}.manifest",
                    hash
                );

                println!("Downloading manifest: {}...", hash);
                if let Ok(resp) = client.get(url).send() {
                    if let Ok(data) = resp.bytes() {
                        tx.send((manifest_id, data)).unwrap();
                        thread_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }

            thread_closed.store(true, Ordering::SeqCst);
            println!("Catalog deliverer finished queuing.");
        });

        Self { rx, count, closed }
    }
    pub fn new_test() -> Self {
        let (tx, rx) = mpsc::sync_channel(1);
        let count = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));

        let manifest_id = u64::from_str_radix("85CF8AF275ECF57B", 16).unwrap();
        if let Ok(data) = fs::read("85CF8AF275ECF57B.manifest") {
            let bytes = Bytes::from(data);
            tx.send((manifest_id, bytes)).unwrap();
            count.fetch_add(1, Ordering::SeqCst);
        }
        closed.store(true, Ordering::SeqCst);

        Self { rx, count, closed }
    }

    pub fn remaining(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn is_finished(&self) -> bool {
        self.closed.load(Ordering::Relaxed) && self.remaining() == 0
    }
}

impl Iterator for ManifestDeliverer {
    type Item = (u64, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok().map(|data| {
            self.count.fetch_sub(1, Ordering::Relaxed);
            data
        })
    }
}

pub fn parse_rman(data: bytes::Bytes) -> (usize, bytes::Bytes) {
    let header_size = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

    let compressed_size = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;

    let uncompressed_size = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;

    let compressed_slice = data.slice(header_size..(header_size + compressed_size));

    /*let debug_slice = &data[0..32];
    println!("RMAN Header Bytes: {:02X?}", debug_slice);


    println!("Parsed Uncompressed: {}, Compressed: {}, Header: {}",
             uncompressed_size, compressed_size, header_size);
    */
    (uncompressed_size, compressed_slice)
}

thread_local! {
    static DECOMPRESSOR: std::cell::RefCell<zstd::bulk::Decompressor<'static>> =
        std::cell::RefCell::new(zstd::bulk::Decompressor::new().unwrap());
}

pub fn decompress_manifest(compressed_data: &[u8], _uncompressed_size: usize) -> Vec<u8> {
    /*DECOMPRESSOR.with(|der| {
        let mut der = der.borrow_mut();

        let mut out = vec![0u8; uncompressed_size];

        der.decompress_to_buffer(compressed_data, &mut out)
            .expect(format!("Decompression failed {}",uncompressed_size).as_str());

        out
    })*/
    zstd::decode_all(compressed_data).expect("should be decompressable")
}

#[derive(Debug, Clone)]
pub struct ChunkEntry {
    pub count: u64,
    pub manifests: u64,
    pub last: u64,
}

pub fn write_results(
    map: &mut HashMap<i64, ChunkEntry>,
    acid_path: &str,
    _access_path: &str,
) -> std::io::Result<()> {
    let mut sorted: Vec<(i64, ChunkEntry)> = map.iter().map(|(&k, v)| (k, v.clone())).collect();
    sorted.sort_unstable_by(|a, b| b.1.count.cmp(&a.1.count));

    let mut acid_writer = BufWriter::with_capacity(128 * 1024, File::create(acid_path)?);
    //let mut access_writer = BufWriter::with_capacity(1024 * 1024, File::create(access_path)?);

    let mut current_offset: u64 = 0;

    for (chunk_id, entry) in sorted {
        let num_elements = entry.manifests; //entry.manifest_vec.len() as u64;
        let last = entry.last;
        acid_writer.write_all(&chunk_id.to_le_bytes())?;
        acid_writer.write_all(&entry.count.to_le_bytes())?;
        acid_writer.write_all(&current_offset.to_le_bytes())?; // The bigarrayid
        acid_writer.write_all(&last.to_le_bytes())?;

        /*for manifest_id in entry.manifest_vec {
            access_writer.write_all(&manifest_id.to_le_bytes())?;
        }*/

        current_offset += num_elements;
    }

    acid_writer.flush()?;
    //access_writer.flush()?;

    Ok(())
}

pub fn main() {
    let args = Args::parse();
    if !args.out_dir.exists() {
        fs::create_dir_all(&args.out_dir).expect("Failed to create output directory");
    }
    let acid_path = args.out_dir.join("chunks.acid");
    let access_path = args.out_dir.join("manifests.access");

    let deliverer = ManifestDeliverer::from_directory(args.manifest_dir.to_str().unwrap());
    let mut aggregated_map = deliverer
        .par_bridge()
        .fold(
            || hashbrown::HashMap::<i64, ChunkEntry>::with_capacity(1_000_000),
            |mut map, raw_data| {
                let (uncompressed_size, stripped) = parse_rman(raw_data.1);

                let decompressed = decompress_manifest(&stripped, uncompressed_size);

                let manifest = unsafe { flatbuffers::root_unchecked::<Manifest>(&decompressed) };

                if let Some(files) = manifest.files() {
                    for file in files {
                        if let Some(ids) = file.chunk_ids() {
                            for id in ids {
                                let entry = map.entry(id).or_insert(ChunkEntry {
                                    count: 0,
                                    manifests: 1,
                                    last: raw_data.0,
                                });
                                entry.count += 1;
                                if entry.last != raw_data.0 {
                                    entry.manifests += 1;
                                    entry.last = raw_data.0
                                }

                                /*if(*entry.manifest_vec.last().expect("cannot be empty")!=raw_data.0){
                                    entry.manifest_vec.push(raw_data.0);
                                }*/
                            }
                        }
                    }
                }
                if let Some(bundles) = manifest.bundles() {
                    for bundle in bundles {
                        if let Some(chunks) = bundle.chunks() {
                            for chunk in chunks {
                                let id = chunk.id();
                                let entry = map.entry(id).or_insert(ChunkEntry {
                                    count: 0,
                                    manifests: 1, //manifest_vec: vec![raw_data.0],
                                    last: raw_data.0,
                                });
                                //entry.count += 1;
                                if entry.last != raw_data.0 {
                                    entry.manifests += 1;
                                    entry.last = raw_data.0
                                }

                                /*if(*entry.manifest_vec.last().expect("cannot be empty")!=raw_data.0){
                                    entry.manifest_vec.push(raw_data.0);
                                }*/
                            }
                        }
                    }
                }
                map
            },
        )
        .reduce(
            || hashbrown::HashMap::new(),
            |mut m1, m2| {
                for (id, entry) in m2 {
                    let e = m1.entry(id).or_insert(ChunkEntry {
                        count: 0,
                        manifests: 0,
                        last: 0,
                    });
                    e.count += entry.count;
                    e.manifests += entry.manifests;
                    e.last = entry.last; //e.manifest_vec.append(&mut entry.manifest_vec.clone());
                }
                m1
            },
        );

    let a = write_results(&mut aggregated_map,acid_path.to_str().unwrap(), access_path.to_str().unwrap()).is_err();
    if a {
        println!("chunks.acid disk write failed");
    }

    if !args.no_manifest_tracking{
    let mut sorted_refs: Vec<(i64, u64,u64)> = aggregated_map
        .iter()
        .map(|(&id, entry)| (id, entry.manifests,entry.count))
        .collect();
    sorted_refs.sort_unstable_by(|a, b| b.2.cmp(&a.2)); // Match your acid sorting

    let mut current_offset: u64 = 0;
    // Key: ChunkID, Value: (BaseOffset, AtomicWriteCounter)
    let mut shadow_map = hashbrown::HashMap::with_capacity(aggregated_map.len());

    for (id, manifests_count,_) in sorted_refs {
        shadow_map.insert(id, (current_offset, std::sync::atomic::AtomicU32::new(0)));
        current_offset += manifests_count;
    }
    let total_elements = current_offset;
    let shadow_map = std::sync::Arc::new(shadow_map);

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(access_path.to_str().unwrap())
        .expect("should eb able to create file");

    file.set_len(total_elements * 8).expect("should be able to set filesize");

    let mmap = unsafe { memmap2::MmapMut::map_mut(&file).expect("should eb able to memmap file") };
    let mmap_ptr = mmap.as_ptr() as usize;

    let deliverer_p2 = ManifestDeliverer::from_directory(args.manifest_dir.to_str().unwrap());
    deliverer_p2
        .par_bridge()
        .for_each(|(manifest_id, raw_data)| {
            let (uncompressed_size, stripped) = parse_rman(raw_data);
            let decompressed = decompress_manifest(&stripped, uncompressed_size);
            let manifest = unsafe { flatbuffers::root_unchecked::<Manifest>(&decompressed) };

            let mut seen = hashbrown::HashSet::new();

            let mut write_op = |id: i64| {
                if seen.insert(id) {
                    if let Some((base_offset, current_pos)) = shadow_map.get(&id) {
                        let rel_idx =
                            current_pos.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let abs_idx = base_offset + rel_idx as u64;

                        unsafe {
                            let ptr = mmap_ptr as *mut u64;
                            ptr.add(abs_idx as usize).write(manifest_id);
                        }
                    }
                }
            };

            if let Some(files) = manifest.files() {
                for f in files {
                    if let Some(ids) = f.chunk_ids() {
                        for id in ids {
                            write_op(id);
                        }
                    }
                }
            }
            if let Some(bundles) = manifest.bundles() {
                for b in bundles {
                    if let Some(chunks) = b.chunks() {
                        for c in chunks {
                            write_op(c.id());
                        }
                    }
                }
            }
        });
    }
}
