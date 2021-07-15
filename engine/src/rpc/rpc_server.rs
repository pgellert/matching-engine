use std::pin::Pin;
use std::sync::Arc;

use futures::TryStreamExt;
use tokio::stream::Stream;
use tokio::sync::{broadcast, RwLock};
use tonic::{Code, Request, Response, Status};
use tracing::trace;

use raft::config::RaftConfig;
use raft::protobuf::{Command, CommandType};
use raft::raft::{Raft, RaftNode};

use crate::protobuf::matching_engine_rpc_server::MatchingEngineRpc;
use crate::protobuf::*;
use crate::rpc::me_state_machine::{MENode, ACK_CHANNEL_CAPACITY};

/// Replicated matching engine server implementation
pub struct MatchingEngine {
    me_node: Arc<MENode>,
    raft_node: RaftNode,
}

impl MatchingEngine {
    pub fn new(id: u64, config: RaftConfig) -> Self {
        let me_node = Arc::new(MENode::new());
        let raft = Arc::new(RwLock::new(Raft::new(id, config, me_node.clone())));
        let raft_node = RaftNode::new(raft);
        Self { me_node, raft_node }
    }

    /// Starts a matching engine server by starting the underlying Raft node
    pub async fn run(&self) {
        self.raft_node.run().await.unwrap();
    }

    /// Initiates placing a limit order through Raft
    async fn place_limit_order(&self, order: Order) -> Result<(), tonic::Status> {
        let is_leader = {
            let raft = self.raft_node.raft.read().await;
            raft.is_leader()
        };

        if !is_leader {
            return Ok(());
        }

        let is_duplicate = {
            let sm = self.me_node.state_machine.lock().await;
            sm.acknowledged_orders
                .contains(&(order.client_id, order.seq_number))
        };

        if is_duplicate {
            let tx = {
                let sm = self.me_node.state_machine.lock().await;
                let mut ack_channel_map = sm.order_acknowledgement_channel.write().await;
                if !ack_channel_map.contains_key(&order.client_id) {
                    ack_channel_map
                        .insert(order.client_id, broadcast::channel(ACK_CHANNEL_CAPACITY));
                    let (tx, _) = ack_channel_map.get(&order.client_id).unwrap();
                    tx.clone()
                } else {
                    let (tx, _) = ack_channel_map.get(&order.client_id).unwrap();
                    tx.clone()
                }
            };

            trace!(
                "Order has already been successful: {:?} {:?}",
                order.client_id,
                order.seq_number
            );

            tx.send(OrderAcknowledgement {
                client_id: order.client_id,
                success: true,
                seq_number: order.seq_number,
            });

            return Ok(());
        }

        let command = order_to_command(order);

        let _success = self.raft_node.start(command).await;

        tokio::task::yield_now().await;

        Ok(())
    }

    /// Initiates placing a cancel order through Raft
    async fn place_cancel_order(&self, order: CancelOrder) -> Result<(), tonic::Status> {
        let is_leader = {
            let raft = self.raft_node.raft.read().await;
            raft.is_leader()
        };

        if !is_leader {
            return Ok(());
        }

        let is_duplicate = {
            let sm = self.me_node.state_machine.lock().await;
            sm.acknowledged_orders
                .contains(&(order.client_id, order.seq_number))
        };

        if is_duplicate {
            let tx = {
                let sm = self.me_node.state_machine.lock().await;
                let mut ack_channel_map = sm.order_acknowledgement_channel.write().await;
                if !ack_channel_map.contains_key(&order.client_id) {
                    ack_channel_map
                        .insert(order.client_id, broadcast::channel(ACK_CHANNEL_CAPACITY));
                    let (tx, _) = ack_channel_map.get(&order.client_id).unwrap();
                    tx.clone()
                } else {
                    let (tx, _) = ack_channel_map.get(&order.client_id).unwrap();
                    tx.clone()
                }
            };

            trace!(
                "Order has already been successful: {:?} {:?}",
                order.client_id,
                order.seq_number
            );

            tx.send(OrderAcknowledgement {
                client_id: order.client_id,
                success: true,
                seq_number: order.seq_number,
            });

            return Ok(());
        }

        let command = cancel_order_to_command(order);

        let _success = self.raft_node.start(command).await;

        tokio::task::yield_now().await;

        Ok(())
    }
}

#[tonic::async_trait]
impl MatchingEngineRpc for Arc<MatchingEngine> {
    /// RPC stream for placing limit orders at the matching engine
    async fn limit_order_queue(
        &self,
        mut request: Request<tonic::Streaming<Order>>,
    ) -> Result<Response<()>, Status> {
        let me = self.clone();
        tokio::spawn(async move {
            let req_stream = request.get_mut();
            while let Ok(Some(order)) = req_stream.message().await {
                me.place_limit_order(order).await;
            }
        });

        Ok(Response::new(()))
    }

    /// RPC stream for placing cancel orders at the matching engine
    async fn cancel_order_queue(
        &self,
        mut request: Request<tonic::Streaming<CancelOrder>>,
    ) -> Result<Response<()>, Status> {
        let me = self.clone();
        tokio::spawn(async move {
            let req_stream = request.get_mut();
            while let Ok(Some(order)) = req_stream.message().await {
                me.place_cancel_order(order).await;
            }
        });

        Ok(Response::new(()))
    }

    type register_order_acknowledgementsStream =
        Pin<Box<dyn Stream<Item = Result<OrderAcknowledgement, Status>> + Send + Sync + 'static>>;

    /// RPC for registering for a stream of order acknowledgements
    async fn register_order_acknowledgements(
        &self,
        request: Request<Client>,
    ) -> Result<Response<Self::register_order_acknowledgementsStream>, Status> {
        let client = request.get_ref();

        let rx = {
            let sm = self.me_node.state_machine.lock().await;
            let ack_channel_map = sm.order_acknowledgement_channel.read().await;
            if !ack_channel_map.contains_key(&client.client_id) {
                drop(ack_channel_map);
                let mut ack_channel_map = sm.order_acknowledgement_channel.write().await;
                if !ack_channel_map.contains_key(&client.client_id) {
                    ack_channel_map
                        .insert(client.client_id, broadcast::channel(ACK_CHANNEL_CAPACITY));
                }
                let (tx, _) = ack_channel_map.get(&client.client_id).unwrap();
                tx.subscribe()
            } else {
                let (tx, _) = ack_channel_map.get(&client.client_id).unwrap();
                tx.subscribe()
            }
        };

        let resp_stream = Box::pin(
            rx.map_err(|err| tonic::Status::new(Code::FailedPrecondition, err.to_string())),
        );
        Ok(Response::new(resp_stream))
    }

    type register_tradesStream =
        Pin<Box<dyn Stream<Item = Result<Trade, Status>> + Send + Sync + 'static>>;

    /// RPC for registering for a stream of trades
    async fn register_trades(
        &self,
        request: Request<Client>,
    ) -> Result<Response<Self::register_tradesStream>, Status> {
        let client = request.get_ref();

        let rx = {
            let sm = self.me_node.state_machine.lock().await;
            sm.get_trade_channel_receiver(client.client_id).await
        };

        let resp_stream = Box::pin(
            rx.map_err(|err| tonic::Status::new(Code::FailedPrecondition, err.to_string())),
        );
        Ok(Response::new(resp_stream))
    }
}

/// Converts a matching engine limit order protobuf definition into a Raft protobuf definition
///
/// Note: this could be optimised to make [Order] and [Command] the same RPC definition, but that
/// would make the raft code the engine implementation interdependent.
#[inline]
fn order_to_command(order: Order) -> Command {
    let side = match OrderSide::from_i32(order.side).unwrap() {
        OrderSide::Buy => OrderSide::Buy,
        OrderSide::Sell => OrderSide::Sell,
    };
    Command {
        r#type: CommandType::Limit as i32,
        sequence_id: order.seq_number,
        price: order.price,
        client_id: order.client_id,
        size: order.size,
        security_id: order.security_id,
        side: side as i32,
        cancel_order_id: 0,
    }
}

/// Converts a matching engine cancel order protobuf definition into a Raft protobuf definition
///
/// Note: this could be optimised to make [CancelOrder] and [Command] the same RPC definition, but that
/// would make the raft code the engine implementation interdependent.
#[inline]
fn cancel_order_to_command(order: CancelOrder) -> Command {
    let side = match OrderSide::from_i32(order.side).unwrap() {
        OrderSide::Buy => OrderSide::Buy,
        OrderSide::Sell => OrderSide::Sell,
    };
    Command {
        r#type: CommandType::Cancel as i32,
        sequence_id: order.seq_number,
        price: 0,
        client_id: order.client_id,
        size: 0,
        security_id: order.security_id,
        side: side as i32,
        cancel_order_id: order.cancel_order_seq_number,
    }
}
