#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::encode::{deserialize, Decodable};
#[cfg(feature = "liquid")]
use elements::encode::{deserialize, Decodable};

use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::thread;
use std::fs;
#[cfg(not(feature = "liquid"))]
use crate::chain::Network::{Fractal, FractalTestnet};
use crate::chain::{Block, BlockHash};
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

#[derive(Clone)]
pub struct BlockEntry {
    pub block: Block,
    pub entry: HeaderEntry,
    pub size: u32,
}

type SizedBlock = (Block, u32);

use rayon::prelude::*;
use std::sync::mpsc as crossbeam_channel;

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
                #[cfg(not(feature = "liquid"))]
                    let blocks = match daemon.network() {
                    Fractal | FractalTestnet => daemon
                        .get_fractal_bocks(&blockhashes)
                        .expect("failed to get blocks from bitcoind"),
                    _ => daemon
                        .getblocks(&blockhashes)
                        .expect("failed to get blocks from bitcoind"),
                };

                #[cfg(feature = "liquid")]
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

    #[cfg(not(feature = "liquid"))]
        let parser = if daemon.network().eq(&Fractal) || daemon.network().eq(&FractalTestnet) {
        blkfiles_parser_fractal(blkfiles_reader(blk_files), magic)
    } else {
        blkfiles_parser(blkfiles_reader(blk_files), magic)
    };
    #[cfg(feature = "liquid")]
        let parser = blkfiles_parser(blkfiles_reader(blk_files), magic);

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
                let blocks =
                    parse_blocks_fractal(blob, magic).expect("failed to parse blk*.dat file");
                sender
                    .send(blocks)
                    .expect("failed to send blocks from blk*.dat file");
            });
        }),
    )
}

pub fn sgx_parse_blocks(blob: Vec<u8>, magic: u32) -> Result<Vec<SizedBlock>> {
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

    let data: Vec<SizedBlock> = slices
        .into_iter()
        .map(|(slice, size)| (deserialize(slice).expect("failed to parse Block"), size))
        .collect();

    Ok(data)
}

pub(crate) fn parse_blocks(blob: Vec<u8>, magic: u32) -> Result<Vec<SizedBlock>> {
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
        let auxpow_size = u32::consensus_decode(&mut cursor).chain_err(|| "no auxpow size")?;
        let start = cursor.position();
        let header_end = start + 80;
        let ntx_start = header_end + auxpow_size as u64;
        let end = start + block_size as u64;

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
    use crate::new_index::fetch::parse_blocks_fractal;
    use bitcoin::consensus::{deserialize, Decodable};
    use bitcoin::{Block, BlockHeader};
    use byteorder::{BigEndian, ReadBytesExt};
    use std::fs;
    use std::io::{self, Cursor, Read};

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
        let blob =
            fs::read("../fractald-release/fractald-docker/data/blocks/blk00000.dat").unwrap();
        println!("blob len: {}", blob.len());
        // parse_blocks_magic(blob, 0xE8ADA3C8).unwrap();
        let result = parse_blocks_fractal(blob, 0xe8ada3c8).unwrap();
        println!("{:?}", result);
    }

    #[test]
    fn test_u32() {
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

    #[test]
    fn test_block_hex() {
        let h = "00012420cd6cc866b7eabfea6c0f663e2795741004c53d45add9327f88545a0200000000b7e144e0ea3a4ae265e3017f740b5b92f363ed7dd021ba5a6c5e30f284011af74896b166f123011a0000000001000000010000000000000000000000000000000000000000000000000000000000000000ffffffff5703240e0d102f6672616374616c626974636f696e2f2cfabe6d6d6602f269b3e9e186d0ad2bdaa862b88e39cab38e31e6f19814adfdc0751de8bf040000000100000010cd0764005222476e25d239d4d54b020000000000ffffffff02fa2c1e130000000017a914a15a657f4f8cf89859305239a0438391ca6a9fd8870000000000000000266a24aa21a9ed859bdf1cedd491f60cdb603eac78932caa2938aae14ae71c23d45a837d9a586f0000000000000000000000000000000000000000000000000000000000000000000000000c25f5eb4dfa0ba16a6a63cc130401437913c59dbc6bd45b50cd56c63388c20c508fb405a397915fa30a5cea92cbf1ad5af7fe4dc4503fca3a35ea20a99c6b1bf2195aa4706c9abeaffd75134b44598632b4db14a7d155c20c06fcb9a3bc9f417469e31045c38e6136dcf41057bed615eb908d02d33e7676cca460cd4746b6a31469328a5919e59df90041be6bbb6b36a0d4ffd336d52d3119aa020b36a4db7682a9924e488b760038bef5860124b4a69b5413be1200fcb45ca7ad3e26b655933465306f58f232e63ba8ed37477134db95a8507584e2ebb03855d13575fc485c033047392f8e6b41a6884ff0b05aa52dad378a7b637bc9335dd49b42aef35a388c8a37c1a9401037966fad75f42652d3b118d8f11f6379db8409b4296046fbd6fbc74d5fc324eca794316637d142eba8d9e7f814e0ea05cf9b0a86d6caefd375d12283b57af42f1fb35210bdd8b320038e7bd21ac0b4d5cfdb19553902a9f4b1bd21c640e4e4220b60e7ccb490355ef1e4e7c138c59d0cb1c90590c3211114958100000000020000000000000000000000000000000000000000000000000000000000000000e2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf90300000010007720e5053266ebbba2d67a1b49618d054b34159dfd849c5200000000000000000000344f2555645cf790d8a82bdf71eb4e2677698eb82924ad7ed8d01dc553f9b199ac96b166be1a03171098835301020000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff0402b40400ffffffff0200f90295000000001976a9142e5fe85515c2bee0eedb98c111c759f625528d1088ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf90120000000000000000000000000000000000000000000000000000000000000000000000000";
        let vec = hex::decode(&h).unwrap();
        let header = deserialize::<BlockHeader>(&vec[..80]).unwrap();
        println!("hash {:?}", header.block_hash());
        println!("header {:?}", header);
        let mut cursor = Cursor::new(&vec[80..]);
        let r1 = u32::consensus_decode(&mut cursor).unwrap();
        println!("{:?}", r1);
        let r2 = u32::consensus_decode(&mut cursor).unwrap();
        println!("{:?}", r2);
        // let block = deserialize::<Block>(&vec).unwrap();
        // println!("{:?}", block);
    }
}
