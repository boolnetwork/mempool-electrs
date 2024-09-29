use std::collections::HashMap;

use bitcoin::{Block, TxMerkleNode};

use crate::util::HeaderEntry;

use reqwest::{blocking::Client, Url};
use serde_json::Value;

lazy_static! {
    static ref HTTP_CLIENT: Client = Client::new();
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

pub fn filter_requests(method: &str) -> Option<Value> {
    if method == "getnetworkinfo" {
        return Some(json!(
            {"version":270000,
            "subversion":"/Satoshi:27.0.0/",
            "relayfee":0.00001000,}
        ));
    }

    if method == "getmempoolinfo" {
        return Some(json!(
            {"loaded":true}
        ));
    }

    None
}

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

pub fn request(addr: String, auth: String, req: &Value) -> crate::errors::Result<Value> {
    let url = Url::parse(&addr).unwrap();

    let response: String = HTTP_CLIENT
        .post(url)
        .header("Content-Type", "application/json")
        .header(reqwest::header::AUTHORIZATION, auth)
        .body(req.to_string())
        .send()
        .expect("failed to get response")
        .text()
        .expect("failed to get payload");
    // let response =
    // sgx_bool_registration_tool::verify_sgx_response_and_restore_origin_response_v2(response, String::new())
    // .map_err(|e| format!("{e:?}"))?;

    let result: Value = serde_json::from_str(&response).map_err(|_| "json error".to_string())?;
    Ok(result)
}

pub fn add_blocks(
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
        let blocks = crate::new_index::fetch::parse_blocks(blob, magic)
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

        indexer.sgx_add(&block_entries);
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
    let magic = daemon.magic();
    let blk_files = daemon.list_blk_files()?;

    let mut entry_map: HashMap<bitcoin::hash_types::BlockHash, HeaderEntry> =
        new_headers.into_iter().map(|h| (*h.hash(), h)).collect();

    for path in blk_files {
        trace!("reading {:?}", path);
        let blob =
            std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {:?}: {:?}", path, e));

        trace!("parsing {} bytes", blob.len());
        let blocks = crate::new_index::fetch::parse_blocks(blob, magic)
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

        indexer.index(&block_entries);
    }

    if !entry_map.is_empty() {
        panic!(
            "failed to index {} blocks from blk*.dat files",
            entry_map.len()
        )
    }

    Ok(())
}
