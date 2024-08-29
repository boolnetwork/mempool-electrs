#![recursion_limit = "1024"]

extern crate rocksdb;

#[macro_use]
extern crate clap;
#[macro_use]
extern crate arrayref;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate lazy_static;

pub mod chain;
pub mod config;
pub mod daemon;
pub mod electrum;
pub mod errors;
pub mod metrics;
pub mod new_index;
pub mod rest;
pub mod signal;
pub mod util;
pub mod reg;

#[cfg(feature = "liquid")]
pub mod elements;

use jemallocator::Jemalloc;
#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;