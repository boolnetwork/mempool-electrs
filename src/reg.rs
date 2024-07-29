
use bitcoin::Block;

use crate::util::HeaderEntry;

use serde_json::Value;

pub fn validate_tx_root(block: &Block, entry: &HeaderEntry){

    let txhashroot = block.compute_merkle_root()
    .expect(&format!("failed to compute root of txs of block {}",block.block_hash()));
    
    let sgx_txhashroot = entry.header().merkle_root;
    
    assert_eq!(txhashroot, sgx_txhashroot, "Block tx hash root not match.");
}

pub fn filter_requests(method: &str) -> Option<Value>{
    info!("method {}",method);
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

    return None;
}

pub fn create_sgx_response<T: serde::Serialize>(value: T, sgx_enable: bool) -> String{
    let keytype = if sgx_enable {
        sgx_bool_registration_tool::KeyType::SGX
    } else {
        sgx_bool_registration_tool::KeyType::TEST
    };

    let value = sgx_bool_registration_tool::create_sgx_response_v2(
        value,
        keytype,
    );
    value
}