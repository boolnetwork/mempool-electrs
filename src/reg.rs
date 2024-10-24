use std::collections::HashMap;
use std::time::Instant;

use bitcoin::{Block, BlockHash, TxMerkleNode};

use crate::util::HeaderEntry;

use serde_json::Value;
use reqwest::{Url, blocking::Client};

use crate::new_index::{BlockEntry, FetchFrom};
#[cfg(not(feature = "liquid"))]
use crate::chain::Network::{Fractal, FractalTestnet};

lazy_static! {
    static ref HTTP_CLIENT: Client = Client::new();
    pub static ref SPV_METHODS: Vec<&'static str> = vec!["getbestblockhash", "getblockheader", "getblockhash"];
}

pub fn validate_tx_root(block: &Block, entry: &HeaderEntry) {
    let txhashroot = block.compute_merkle_root().unwrap_or_else(|| panic!("failed to compute root of txs of block {}", block.block_hash()));

    let sgx_txhashroot = TxMerkleNode::from_hash(entry.header().merkle_root.as_hash());

    assert_eq!(
        txhashroot,
        sgx_txhashroot,
        "Block tx hash root not match. blockhash {} header {}",
        block.block_hash(),
        entry.hash()
    );
}

// pub fn filter_requests(method: &str) -> Option<Value> {
//     if method == "getnetworkinfo" {
//         return Some(json!(
//             {"version":270000,
//             "subversion":"/Satoshi:27.0.0/",
//             "relayfee":0.00001000,}
//         ));
//     }
//
//     if method == "getmempoolinfo" {
//         return Some(json!(
//             {"loaded":true}
//         ));
//     }
//
//     None
// }

pub fn create_sgx_response<T: serde::Serialize>(value: T, sgx_enable: bool) -> String {
    let keytype = if sgx_enable {
        sgx_bool_registration_tool::KeyType::SGX
    } else {
        sgx_bool_registration_tool::KeyType::TEST
    };

    sgx_bool_registration_tool::create_sgx_response_v2(value, keytype)
}

pub fn seal_data(value: Vec<u8>) -> Vec<u8> {
    sgx_bool_registration_tool::sealing(value).unwrap()
}

pub fn unseal_data(value: Vec<u8>) -> Vec<u8> {
    sgx_bool_registration_tool::unsealing(value).unwrap()
}

pub fn request(addr: &str, _auth: String, req: &Value) -> crate::errors::Result<Value> {
    let url = Url::parse(addr).unwrap();
    if let Some(obj) = req.as_object() {
        if let Some(method) = obj.get("method"){
            if method.to_string().eq("getblock") {
                info!("{}",req)
            }
        }
    }
    let response: String = HTTP_CLIENT
        .post(url)
        .header("Content-Type", "application/json")
        //.header(AUTHORIZATION, auth)
        .body(req.to_string())
        .send()
        .map_err(|_| "failed to get response")?
        .text()
        .map_err(|_| "failed to get payload")?;
    let response =
        sgx_bool_registration_tool::verify_sgx_response_and_restore_origin_response_v2(response.clone(), String::new())
            .map_err(|e| format!("{e:?} {response}"))?;

    let result: Value = serde_json::from_str(&response).map_err(|_| "json error".to_string())?;
    Ok(result)
}

pub fn add_blocks(
    indexer: &crate::new_index::schema::Indexer,
    daemon: &crate::daemon::Daemon,
    new_headers: Vec<HeaderEntry>,
) -> crate::errors::Result<()> {
    match indexer.fetch_from() {
        FetchFrom::Bitcoind => {
            add_blocks_bitcoind(
                indexer,
                daemon,
                new_headers,
            )
        }
        FetchFrom::BlkFiles => {
            add_blocks_blkfiles(
                indexer,
                daemon,
                new_headers,
            )
        }
    }
}

pub fn add_blocks_bitcoind(
    indexer: &crate::new_index::schema::Indexer,
    daemon: &crate::daemon::Daemon,
    new_headers: Vec<HeaderEntry>,
) -> crate::errors::Result<()> {
    if let Some(tip) = new_headers.last() {
        debug!("{:?} ({} left to index)", tip, new_headers.len());
    };
    let daemon = daemon.reconnect()?;

    for entries in new_headers.chunks(100) {
        let blockhashes: Vec<BlockHash> = entries.iter().map(|e| *e.hash()).collect();
        let mut blocks = None;
        #[cfg(not(feature = "liquid"))]
        while blocks.is_none(){
            match match daemon.network() {
                Fractal | FractalTestnet => daemon
                    .get_fractal_bocks(&blockhashes)
                    .map_err(|_| "failed to get blocks from bitcoind"),
                _ => daemon
                    .getblocks(&blockhashes)
                    .map_err(|_| "failed to get blocks from bitcoind"),
            } {
                Ok(data) => {
                    blocks.replace(data);
                },
                Err(err) => {
                    error!("{}", err);
                }
            }
        };


        #[cfg(feature = "liquid")]
        blocks.replace(daemon
            .getblocks(&blockhashes)
            .expect("failed to get blocks from bitcoind"));

        let blocks = if blocks.is_none() {
            panic!("failed to get blocks from bitcoind");
        } else {
            blocks.unwrap()
        };

        assert_eq!(blocks.len(), entries.len());

        let block_entries: Vec<BlockEntry> = blocks
            .into_iter()
            .zip(entries)
            .map(|(block, entry)|
                {
                    crate::reg::validate_tx_root(&block, entry);
                    BlockEntry {
                        entry: entry.clone(), // TODO: remove this clone()
                        size: block.size() as u32,
                        block,
                    }
                })
            .collect();
        assert_eq!(block_entries.len(), entries.len());

        let start = Instant::now();
        indexer.sgx_add(&block_entries);
        debug!("sgx_add {} blocks cost: {:?}", block_entries.len(),Instant::now().duration_since(start));
    }

    Ok(())
}

pub fn add_blocks_blkfiles(
    indexer: &crate::new_index::schema::Indexer,
    daemon: &crate::daemon::Daemon,
    new_headers: Vec<HeaderEntry>,
) -> crate::errors::Result<()> {
    // fetch
    let magic = daemon.magic();
    let blk_files = daemon.list_blk_files()?;

    let mut entry_map: HashMap<bitcoin::hash_types::BlockHash, HeaderEntry> =
        new_headers.into_iter().map(|h| (*h.hash(), h)).collect();

    for path in blk_files {
        trace!("reading {:?}", path);
        let blob =
            std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {:?}: {:?}", path, e));

        trace!("parsing {} bytes", blob.len());
        let blocks = crate::new_index::fetch::sgx_parse_blocks(blob, magic)
            .expect("failed to parse blk*.dat file");

        let block_entries: Vec<crate::new_index::BlockEntry> = blocks
            .into_iter()
            .filter_map(|(block, size)| {
                let blockhash = block.block_hash();

                if entry_map.contains_key(&blockhash) {
                    crate::reg::validate_tx_root(&block, &entry_map[&blockhash]);
                    entry_map
                        .remove(&blockhash)
                        .map(|entry| crate::new_index::BlockEntry { block, entry, size })
                } else {
                    trace!("skipping block {}", blockhash);
                    None
                }
            })
            .collect();
        let start = Instant::now();
        indexer.sgx_add(&block_entries);
        debug!("sgx_add {} blocks cost: {:?}", block_entries.len(),Instant::now().duration_since(start));
    }

    if !entry_map.is_empty() {
        panic!(
            "failed to index {} blocks from blk*.dat files",
            entry_map.len()
        )
    }

    Ok(())
}

pub fn index(
    indexer: &crate::new_index::schema::Indexer,
    daemon: &crate::daemon::Daemon,
    new_headers: Vec<HeaderEntry>,
) -> crate::errors::Result<()> {
    // fetch
    match indexer.fetch_from() {
        FetchFrom::Bitcoind => {
            if let Some(tip) = new_headers.last() {
                debug!("{:?} ({} left to index)", tip, new_headers.len());
            };
            let daemon = daemon.reconnect()?;

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
                    .map(|(block, entry)| {
                        crate::reg::validate_tx_root(&block, entry);
                        BlockEntry {
                            entry: entry.clone(), // TODO: remove this clone()
                            size: block.size() as u32,
                            block,
                        }
                    })
                    .collect();
                assert_eq!(block_entries.len(), entries.len());

                let start = Instant::now();
                indexer.sgx_index(&block_entries);
                debug!("sgx_add {} blocks cost: {:?}", block_entries.len(),Instant::now().duration_since(start));
            }
        }
        FetchFrom::BlkFiles => {
            let magic = daemon.magic();
            let blk_files = daemon.list_blk_files()?;

            let mut entry_map: HashMap<bitcoin::hash_types::BlockHash, HeaderEntry> =
                new_headers.into_iter().map(|h| (*h.hash(), h)).collect();

            for path in blk_files {
                trace!("reading {:?}", path);
                let blob =
                    std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {:?}: {:?}", path, e));

                trace!("parsing {} bytes", blob.len());
                let blocks = crate::new_index::fetch::sgx_parse_blocks(blob, magic)
                    .expect("failed to parse blk*.dat file");

                let block_entries: Vec<crate::new_index::BlockEntry> = blocks
                    .into_iter()
                    .filter_map(|(block, size)| {
                        let blockhash = block.block_hash();

                        if entry_map.contains_key(&blockhash) {
                            crate::reg::validate_tx_root(&block, &entry_map[&blockhash]);
                            entry_map
                                .remove(&blockhash)
                                .map(|entry| crate::new_index::BlockEntry { block, entry, size })
                        } else {
                            trace!("skipping block {}", blockhash);
                            None
                        }
                    })
                    .collect();

                let start = Instant::now();
                indexer.sgx_index(&block_entries);
                debug!("index {} blocks cost: {:?}", block_entries.len(),Instant::now().duration_since(start));
            }

            if !entry_map.is_empty() {
                panic!(
                    "failed to index {} blocks from blk*.dat files",
                    entry_map.len()
                )
            }
        }
    }

    Ok(())
}
