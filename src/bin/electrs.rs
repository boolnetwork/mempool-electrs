extern crate error_chain;
#[macro_use]
extern crate log;

extern crate electrs;

use error_chain::ChainedError;
use std::process;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use electrs::{
    config::Config,
    daemon::Daemon,
    //electrum::RPC as ElectrumRPC,
    errors::*,
    metrics::Metrics,
    new_index::{precache, ChainQuery, FetchFrom, Indexer, Mempool, Query, Store},
    rest,
    signal::Waiter,
};

#[cfg(feature = "liquid")]
use electrs::elements::AssetRegistry;

fn fetch_from(config: &Config, store: &Store) -> FetchFrom {
    let mut jsonrpc_import = config.jsonrpc_import;
    if !jsonrpc_import {
        // switch over to jsonrpc after the initial sync is done
        jsonrpc_import = store.done_initial_sync();
    }

    if jsonrpc_import {
        // slower, uses JSONRPC (good for incremental updates)
        FetchFrom::Bitcoind
    } else {
        // faster, uses blk*.dat files (good for initial indexing)
        FetchFrom::BlkFiles
    }
}

fn run_server(config: Arc<Config>) -> Result<()> {
    let signal = Waiter::start();
    let metrics = Metrics::new(config.monitoring_addr);
    metrics.start();

    let daemon = Arc::new(Daemon::new(
        config.daemon_dir.clone(),
        config.blocks_dir.clone(),
        config.daemon_rpc_addr,
        config.cookie_getter(),
        config.network_type,
        config.sgx_enable,
        config.magic,
        signal.clone(),
        &metrics,
        config.spv_url.clone(),
    )?);
    let store = Arc::new(Store::open(&config.db_path.join("newindex"), &config));
    let mut indexer = Indexer::open(
        Arc::clone(&store),
        fetch_from(&config, &store),
        &config,
        &metrics,
    );

    #[cfg(not(feature = "liquid"))]
        let mut tip = if config.sgx_enable {
        indexer.sgx_update(&daemon)?
    } else {
        indexer.update(&daemon)?
    };
    #[cfg(feature = "liquid")]
        let mut tip = indexer.update(&daemon)?;


    let chain = Arc::new(ChainQuery::new(
        Arc::clone(&store),
        Arc::clone(&daemon),
        &config,
        &metrics,
    ));

    let mempool = Arc::new(RwLock::new(Mempool::new(
        Arc::clone(&chain),
        &metrics,
        Arc::clone(&config),
    )));
    loop {
        match Mempool::update(&mempool, &daemon) {
            Ok(_) => break,
            Err(e) => {
                warn!(
                    "Error performing initial mempool update, trying again in 5 seconds: {}",
                    e.display_chain()
                );
                signal.wait(Duration::from_secs(5), false)?;
            }
        }
    }

    #[cfg(feature = "liquid")]
        let asset_db = config.asset_db_path.as_ref().map(|db_dir| {
        let asset_db = Arc::new(RwLock::new(AssetRegistry::new(db_dir.clone())));
        AssetRegistry::spawn_sync(asset_db.clone());
        asset_db
    });

    let query = Arc::new(Query::new(
        Arc::clone(&chain),
        Arc::clone(&mempool),
        Arc::clone(&daemon),
        Arc::clone(&config),
        #[cfg(feature = "liquid")]
            asset_db,
    ));

    // TODO: configuration for which servers to start
    let rest_server = rest::start(Arc::clone(&config), Arc::clone(&query), &metrics);
    //let electrum_server = ElectrumRPC::start(Arc::clone(&config), Arc::clone(&query), &metrics);

    if let Some(ref precache_file) = config.precache_scripts {
        let precache_scripthashes = precache::scripthashes_from_file(precache_file.to_string())
            .expect("cannot load scripts to precache");
        precache::precache(
            Arc::clone(&chain),
            precache_scripthashes,
            config.precache_threads,
        );
    }

    loop {
        if let Err(err) = signal.wait(Duration::from_millis(config.main_loop_delay), true) {
            info!("stopping server: {}", err);

            electrs::util::spawn_thread("shutdown-thread-checker", || {
                let mut counter = 40;
                let interval_ms = 500;

                while counter > 0 {
                    electrs::util::with_spawned_threads(|threads| {
                        debug!("Threads during shutdown: {:?}", threads);
                    });
                    std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                    counter -= 1;
                }
            });

            rest_server.stop();
            // the electrum server is stopped when dropped
            break;
        }

        // Index new blocks
        let current_tip = daemon.getbestblockhash()?;
        if current_tip != tip {
            #[cfg(not(feature = "liquid"))]
            if config.sgx_enable {
                indexer.sgx_update(&daemon)?;
            } else {
                indexer.update(&daemon)?;
            }
            #[cfg(feature = "liquid")]
            indexer.update(&daemon)?;

            tip = current_tip;
        };

        // Update mempool
        if let Err(e) = Mempool::update(&mempool, &daemon) {
            // Log the error if the result is an Err
            warn!(
                "Error updating mempool, skipping mempool update: {}",
                e.display_chain()
            );
        }

        // Update subscribed clients
        //electrum_server.notify();
    }
    info!("server stopped");
    Ok(())
}

fn register_to_bool(config: Arc<Config>) -> Result<()> {
    if config.sgx_test {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            sgx_bool_registration_tool::register_sgx_test().await;
            let pk = hex::decode(&config.relate_device_id_test).unwrap();
            let mut list = sgx_bool_registration_tool::RELATEDEVICEIDS
                .read()
                .unwrap()
                .clone()
                .unwrap();
            list.push(pk);
            *sgx_bool_registration_tool::RELATEDEVICEIDS.write().unwrap() = Some(list);
        });
    } else {
        electrs::util::spawn_thread("register", move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = sgx_bool_registration_tool::register_sgx_2(
                    config.subclient_url.clone(),
                    config.warn_time.into(),
                    config.config_version,
                    config.device_owner.clone(),
                    config.watcher_device_id.clone(),
                    2u16,
                )
                    .await;
                std::thread::park();
            });
        });
    }
    std::thread::sleep(std::time::Duration::from_secs(8));
    Ok(())
}

fn main() {
    let config = Arc::new(Config::from_args());
    if config.sgx_enable {
        if let Err(e) = register_to_bool(config.clone()) {
            error!("register failed: {}", e.display_chain());
            process::exit(1);
        }
    }

    if let Err(e) = run_server(config) {
        error!("server failed: {}", e.display_chain());
        process::exit(1);
    }
    electrs::util::with_spawned_threads(|threads| {
        debug!("Threads before closing: {:?}", threads);
    });
}
