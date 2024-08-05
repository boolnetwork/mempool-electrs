use rayon::prelude::*;

#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::encode::{deserialize, Decodable};
#[cfg(feature = "liquid")]
use elements::encode::{deserialize, Decodable};

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::thread;
use bitcoin::BlockHeader;
use bitcoin::consensus::encode::Error::UnsupportedSegwitFlag;
use byteorder::{BigEndian, ReadBytesExt};

use crate::chain::{Block, BlockHash};
use crate::chain::Network::Fractal;
use crate::daemon::Daemon;
use crate::errors::*;
use crate::util::{spawn_thread, HeaderEntry, SyncChannel};


#[derive(Clone, Copy, Debug)]
pub enum FetchFrom {
    Bitcoind,
    BlkFiles,
}

pub fn start_fetcher(
    from: FetchFrom,
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    let fetcher = match from {
        FetchFrom::Bitcoind => bitcoind_fetcher,
        FetchFrom::BlkFiles => blkfiles_fetcher,
    };
    fetcher(daemon, new_headers)
}

pub struct BlockEntry {
    pub block: Block,
    pub entry: HeaderEntry,
    pub size: u32,
}

type SizedBlock = (Block, u32);

pub struct Fetcher<T> {
    receiver: crossbeam_channel::Receiver<T>,
    thread: thread::JoinHandle<()>,
}

impl<T> Fetcher<T> {
    fn from(receiver: crossbeam_channel::Receiver<T>, thread: thread::JoinHandle<()>) -> Self {
        Fetcher { receiver, thread }
    }

    pub fn map<F>(self, mut func: F)
        where
            F: FnMut(T),
    {
        for item in self.receiver {
            func(item);
        }
        self.thread.join().expect("fetcher thread panicked")
    }
}

fn bitcoind_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    if let Some(tip) = new_headers.last() {
        debug!("{:?} ({} left to index)", tip, new_headers.len());
    };
    let daemon = daemon.reconnect()?;
    let chan = SyncChannel::new(1);
    let sender = chan.sender();
    Ok(Fetcher::from(
        chan.into_receiver(),
        spawn_thread("bitcoind_fetcher", move || {
            for entries in new_headers.chunks(100) {
                let blockhashes: Vec<BlockHash> = entries.iter().map(|e| *e.hash()).collect();
                let blocks = daemon
                    .getblocks(&blockhashes)
                    .expect("failed to get blocks from bitcoind");
                assert_eq!(blocks.len(), entries.len());
                let block_entries: Vec<BlockEntry> = blocks
                    .into_iter()
                    .zip(entries)
                    .map(|(block, entry)| BlockEntry {
                        entry: entry.clone(), // TODO: remove this clone()
                        size: block.size() as u32,
                        block,
                    })
                    .collect();
                assert_eq!(block_entries.len(), entries.len());
                sender
                    .send(block_entries)
                    .expect("failed to send fetched blocks");
            }
        }),
    ))
}

fn blkfiles_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    let magic = daemon.magic();
    let blk_files = daemon.list_blk_files()?;

    let chan = SyncChannel::new(1);
    let sender = chan.sender();

    let mut entry_map: HashMap<BlockHash, HeaderEntry> =
        new_headers.into_iter().map(|h| (*h.hash(), h)).collect();

    let parser = if daemon.network().eq(&Fractal) {
        blkfiles_parser_fractal(blkfiles_reader(blk_files), magic)
    } else {
        blkfiles_parser(blkfiles_reader(blk_files), magic)
    };

    Ok(Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_fetcher", move || {
            parser.map(|sizedblocks| {
                let block_entries: Vec<BlockEntry> = sizedblocks
                    .into_iter()
                    .filter_map(|(block, size)| {
                        let blockhash = block.block_hash();
                        entry_map
                            .remove(&blockhash)
                            .map(|entry| BlockEntry { block, entry, size })
                            .or_else(|| {
                                trace!("skipping block {}", blockhash);
                                None
                            })
                    })
                    .collect();
                trace!("fetched {} blocks", block_entries.len());
                sender
                    .send(block_entries)
                    .expect("failed to send blocks entries from blk*.dat files");
            });
            if !entry_map.is_empty() {
                panic!(
                    "failed to index {} blocks from blk*.dat files",
                    entry_map.len()
                )
            }
        }),
    ))
}

fn blkfiles_reader(blk_files: Vec<PathBuf>) -> Fetcher<Vec<u8>> {
    let chan = SyncChannel::new(1);
    let sender = chan.sender();

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_reader", move || {
            for path in blk_files {
                trace!("reading {:?}", path);
                let blob = fs::read(&path)
                    .unwrap_or_else(|e| panic!("failed to read {:?}: {:?}", path, e));
                sender
                    .send(blob)
                    .unwrap_or_else(|_| panic!("failed to send {:?} contents", path));
            }
        }),
    )
}

fn blkfiles_parser(blobs: Fetcher<Vec<u8>>, magic: u32) -> Fetcher<Vec<SizedBlock>> {
    let chan = SyncChannel::new(1);
    let sender = chan.sender();

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_parser", move || {
            blobs.map(|blob| {
                trace!("parsing {} bytes", blob.len());
                let blocks = parse_blocks(blob, magic).expect("failed to parse blk*.dat file");
                sender
                    .send(blocks)
                    .expect("failed to send blocks from blk*.dat file");
            });
        }),
    )
}

fn blkfiles_parser_fractal(blobs: Fetcher<Vec<u8>>, magic: u32) -> Fetcher<Vec<SizedBlock>> {
    let chan = SyncChannel::new(1);
    let sender = chan.sender();

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_parser", move || {
            blobs.map(|blob| {
                trace!("parsing {} bytes", blob.len());
                let blocks = parse_blocks_fractal(blob, magic).expect("failed to parse blk*.dat file");
                sender
                    .send(blocks)
                    .expect("failed to send blocks from blk*.dat file");
            });
        }),
    )
}

fn parse_blocks(blob: Vec<u8>, magic: u32) -> Result<Vec<SizedBlock>> {
    let mut cursor = Cursor::new(&blob);
    let mut slices = vec![];
    let max_pos = blob.len() as u64;

    while cursor.position() < max_pos {
        let offset = cursor.position();
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic != value {
                    cursor.set_position(offset + 1);
                    continue;
                }
            }
            Err(_) => break, // EOF
        };
        let block_size = u32::consensus_decode(&mut cursor).chain_err(|| "no block size")?;
        let start = cursor.position();
        let end = start + block_size as u64;

        // If Core's WriteBlockToDisk ftell fails, only the magic bytes and size will be written
        // and the block body won't be written to the blk*.dat file.
        // Since the first 4 bytes should contain the block's version, we can skip such blocks
        // by peeking the cursor (and skipping previous `magic` and `block_size`).
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic == value {
                    cursor.set_position(start);
                    continue;
                }
            }
            Err(_) => break, // EOF
        }
        slices.push((&blob[start as usize..end as usize], block_size));
        cursor.set_position(end);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(0) // CPU-bound
        .thread_name(|i| format!("parse-blocks-{}", i))
        .build()
        .unwrap();
    Ok(pool.install(|| {
        slices
            .into_par_iter()
            .map(|(slice, size)| (deserialize(slice).expect("failed to parse Block"), size))
            .collect()
    }))
}

fn parse_blocks_fractal(blob: Vec<u8>, magic: u32) -> Result<Vec<SizedBlock>> {
    let mut cursor = Cursor::new(&blob);
    let mut slices = vec![];
    let max_pos = blob.len() as u64;

    while cursor.position() < max_pos {
        let offset = cursor.position();
        match ReadBytesExt::read_u32::<BigEndian>(&mut cursor) {
            Ok(value) => {
                if magic != value {
                    cursor.set_position(offset + 1);
                    continue;
                }
            }
            Err(_) => break, // EOF
        }
        let block_size = u32::consensus_decode(&mut cursor).chain_err(|| "no block size")?;
        println!("block_size {}", block_size);

        let auxpow_size = u32::consensus_decode(&mut cursor).chain_err(|| "no auxpow size")?;
        println!("auxpow_size {}", auxpow_size);

        let start = cursor.position();
        println!("start {}", start);

        let header_end = start + 80;
        println!("header_end {}", header_end);

        let ntx_start = header_end + auxpow_size as u64;
        println!("ntx_start {}", ntx_start);

        let end = start + block_size as u64;
        println!("end {}", end);

        // If Core's WriteBlockToDisk ftell fails, only the magic bytes and size will be written
        // and the block body won't be written to the blk*.dat file.
        // Since the first 4 bytes should contain the block's version, we can skip such blocks
        // by peeking the cursor (and skipping previous `magic` and `block_size`).
        match ReadBytesExt::read_u32::<BigEndian>(&mut cursor) {
            Ok(value) => {
                if magic == value {
                    cursor.set_position(start);
                    continue;
                }
            }
            Err(_) => break, // EOF
        }

        let mut block_data = vec![];
        println!("block_data len: {}", header_end -start + (end - ntx_start));
        block_data.extend_from_slice(&blob[start as usize..header_end as usize]);
        block_data.extend_from_slice(&blob[ntx_start as usize..end as usize]);
        slices.push((block_data, block_size));
        cursor.set_position(end);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(0) // CPU-bound
        .thread_name(|i| format!("parse-blocks-{}", i))
        .build()
        .unwrap();

    Ok(pool.install(|| {
        slices
            .into_par_iter()
            .map(|(slice, size)| (deserialize(&slice).expect("failed to parse Block"), size))
            .collect()
    }))
}


#[cfg(test)]
mod test {
    use std::fs;
    use std::io::{self, Cursor, Read};
    use bitcoin::consensus::Decodable;
    use byteorder::{BigEndian, ReadBytesExt};
    use crate::new_index::fetch::parse_blocks_fractal;

    fn parse_blocks_magic(blob: Vec<u8>, expected_magic: u32) -> io::Result<()> {
        let mut cursor = Cursor::new(blob);
        let max_pos = cursor.get_ref().len() as u64;

        while cursor.position() < max_pos {
            let offset = cursor.position();
            let magic = cursor.read_u32::<BigEndian>()?;

            if magic == expected_magic {
                println!("Found expected magic: {:x} at offset {}", magic, offset);
            } else {
                // println!("Unexpected magic: {:x} at offset {}, expected: {:x}", magic, offset, expected_magic);
                cursor.set_position(offset + 1);
                continue;
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_blk() {
        let blob = fs::read("../fractald-release/fractald-docker/data/blocks/blk00002.dat").unwrap();
        println!("blob len: {}", blob.len());
        // parse_blocks_magic(blob, 0xE8ADA3C8).unwrap();
        let result = parse_blocks_fractal(blob, 0xe8ada3c8).unwrap();
        println!("{:?}", result);
    }

    #[test]
    fn test_u32()  {
        let mut a = 1u32.to_le_bytes().to_vec();
        let mut b = 2u32.to_le_bytes().to_vec();
        a.append(&mut b);
        println!("{a:?}");
        let mut cursor = Cursor::new(&a);
        let r1 = u32::consensus_decode(&mut cursor).unwrap();
        let r2 = u32::consensus_decode(&mut cursor).unwrap();
        println!("{}, {}", r1, r2);

        let mut a2 = 1u32.to_be_bytes().to_vec();
        let mut b2 = 2u32.to_be_bytes().to_vec();
        a2.append(&mut b2);
        let mut cursor = Cursor::new(&a2);
        let r1 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
        let r2 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
        println!("{}, {}", r1, r2);
    }
}
