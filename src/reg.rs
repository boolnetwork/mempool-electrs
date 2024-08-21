use bitcoin::Block;

use crate::util::HeaderEntry;

use serde_json::Value;
use reqwest::{Url, blocking::Client};

pub fn validate_tx_root(block: &Block, entry: &HeaderEntry){

    let txhashroot = block.compute_merkle_root()
    .expect(&format!("failed to compute root of txs of block {}",block.block_hash()));
    
    let sgx_txhashroot = entry.header().merkle_root;
    
    assert_eq!(txhashroot, sgx_txhashroot, "Block tx hash root not match.");
}

pub fn filter_requests(method: &str) -> Option<Value>{
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

pub fn seal_data(value: Vec<u8>) -> Vec<u8>{
    sgx_bool_registration_tool::sealing(value).unwrap()
}

pub fn unseal_data(value: Vec<u8>) -> Vec<u8>{
    sgx_bool_registration_tool::unsealing(value).unwrap()
}

pub fn request(addr: String, auth: String ,req: &Value) -> crate::errors::Result<Value>{
    let client = Client::builder()
    .build()
    .unwrap();

    let url = Url::parse(&addr).unwrap();

    let response: String = client
    .post(url)
    .header("Content-Type", "application/json")
    .basic_auth("prz", Some("prz"))
    .header(reqwest::header::AUTHORIZATION, auth)
    .body(req.to_string())
    .send()
    .expect("failed to get response")
    .text()
    .expect("failed to get payload");
    let response = 
    sgx_bool_registration_tool::verify_sgx_response_and_restore_origin_response_v2(response, String::new())
    .map_err(|e| format!("{e:?}"))?;

    let result: Value = serde_json::from_str(&response).map_err(|_| format!("json error"))?;  
    Ok(result)  
}