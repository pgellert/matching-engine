use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

use futures::future::try_join_all;
use rand::Rng;
use tokio::stream::StreamExt;
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{delay_for, Duration};
use tracing::{debug, error, info, warn};

use protobuf::matching_engine_rpc_client::MatchingEngineRpcClient;
use protobuf::*;

use crate::client_order_manager::ClientOrderManager;
use crate::protobuf::{Order, OrderAcknowledgement, OrderSide};

mod algos;
mod client_order_manager;
mod protobuf;
mod rpc;

const DEFAULT_BASE_PORT: u16 = 9100;

const LOOPING_PERIOD: Duration = Duration::from_millis(5);
const RETRY_TIMEOUT: Duration = Duration::from_millis(10000);
const N_LEADERS: u16 = 5;
const N_REQUESTS: u64 = 3000;
const N_CLIENTS: u64 = 3;
const TARGET_THROUGHPUT: u64 = 800; // RPS

static mut START: Option<Instant> = None;
static mut DONE: AtomicU64 = AtomicU64::new(0);

/// Starts a few mock clients to test that the replicated matching engine works correctly
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let mut threads = vec![];
    for client_id in 1..=N_CLIENTS {
        threads.push(tokio::spawn(async move {
            start_mock_client(client_id).await;
        }))
    }

    try_join_all(threads).await;
}

/// Starts a mock client that sends [N_REQUESTS] client orders and retries them if they time out.
async fn start_mock_client(client_id: u64) {
    let mut order_manager = ClientOrderManager::new();
    for seq_num in 1..=N_REQUESTS {
        let order = generate_random_order_request(client_id, seq_num);
        order_manager.add_order(order);
    }
    let order_manager = Arc::new(Mutex::new(order_manager));

    let (tx, _rx): (Sender<Order>, _) = broadcast::channel(N_REQUESTS as usize);

    info!("Starting rpc client");

    let mut threads = vec![];

    for leader_id in 0..N_LEADERS {
        let mut conn = connect_to_leader(leader_id).await;
        let rx_stream = tx.subscribe().into_stream().map(|order| order.unwrap());
        let resp = conn.limit_order_queue(rx_stream).await;
        assert!(resp.is_ok());

        let req = tonic::Request::new(Client { client_id });
        let mut order_ack_resp: tonic::Streaming<OrderAcknowledgement> = conn
            .register_order_acknowledgements(req)
            .await
            .unwrap()
            .into_inner();

        let req = tonic::Request::new(Client { client_id });
        let mut trades_resp: tonic::Streaming<Trade> =
            conn.register_trades(req).await.unwrap().into_inner();

        let order_manager = order_manager.clone();
        threads.push(tokio::spawn(async move {
            while let Ok(order_response) = order_ack_resp.message().await {
                match order_response {
                    Some(order_ack) => {
                        if order_ack.success {
                            let mut order_manager = order_manager.lock().await;
                            order_manager.complete_order(order_ack.seq_number);

                            if order_ack.seq_number == N_REQUESTS {
                                unsafe {
                                    let done = DONE.fetch_add(1, atomic::Ordering::SeqCst);
                                    if done + 1 == N_CLIENTS {
                                        let duration = START.unwrap().elapsed();
                                        println!("Duration: {:}ms", duration.as_millis())
                                    }
                                }
                            }
                            warn!(
                                "[{:}] Successful order: {:}",
                                order_ack.client_id, order_ack.seq_number
                            );
                        } else {
                            debug!(
                                "[{:}] Unsuccessful: {:}",
                                order_ack.client_id, order_ack.seq_number
                            );
                        }
                    }
                    None => error!("None returned in order response stream"),
                }
            }
        }));

        threads.push(tokio::spawn(async move {
            while let Ok(trade_response) = trades_resp.message().await {
                info!("[{:}] Trade response: {:?}", client_id, trade_response);
            }
        }));
    }

    threads.push(tokio::spawn(async move {
        delay_for(Duration::from_secs(1)).await;
        unsafe {
            START = Some(Instant::now());
        }

        loop {
            let mut order_manager = order_manager.lock().await;
            let order_option = order_manager.poll_next_order();
            if let Some(order) = order_option {
                let timeout = Instant::now() + RETRY_TIMEOUT;
                order_manager.add_order_with_timeout(order.clone(), timeout);
                warn!(
                    "[{:}] Sending order: {:}",
                    order.client_id, order.seq_number
                );
                tx.send(order);
                delay_for(Duration::from_micros(1_000_000 / TARGET_THROUGHPUT)).await;
            } else {
                delay_for(LOOPING_PERIOD).await;
            }
        }
    }));

    for th in threads {
        tokio::join!(th);
    }
}

async fn connect_to_leader(leader_id: u16) -> MatchingEngineRpcClient<tonic::transport::Channel> {
    loop {
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, DEFAULT_BASE_PORT + leader_id);
        let addr = format!("http://{:}", addr.to_string());
        if let Ok(new_conn) = MatchingEngineRpcClient::connect(addr).await {
            return new_conn;
        }
    }
}

fn generate_random_order_request(client_id: u64, seq_number: u64) -> Order {
    let mut rng = rand::thread_rng();
    let price: u64 = rng.gen_range(10..=1000);
    let side = OrderSide::from_i32(rng.gen_range(0..=1)).unwrap();
    Order {
        client_id,
        price,
        seq_number,
        side: side as i32,
        size: 1,
        security_id: 0,
    }
}
