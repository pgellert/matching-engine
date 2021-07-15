use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::sync::{Mutex, RwLock};
use tracing::info;

use raft::protobuf::Command;
use raft::state_machine::StateMachine;

use crate::algos::book;
use crate::algos::optimised_fifo::FIFOBook;
use crate::protobuf::*;

type ClientId = u64;
type SequenceNumber = u64;
type OrderId = (ClientId, SequenceNumber);

pub(crate) const ACK_CHANNEL_CAPACITY: usize = 20000;

/// Replicated matching engine code
pub struct MEStateMachine {
    book: Box<dyn book::Book + Send + Sync>,
    pub acknowledged_orders: HashSet<OrderId>,
    pub order_acknowledgement_channel: RwLock<
        HashMap<
            ClientId,
            (
                broadcast::Sender<OrderAcknowledgement>,
                broadcast::Receiver<OrderAcknowledgement>,
            ),
        >,
    >,
    pub trade_channel:
        RwLock<HashMap<ClientId, (broadcast::Sender<Trade>, broadcast::Receiver<Trade>)>>,
}

impl MEStateMachine {
    pub fn new() -> Self {
        Self {
            book: Box::new(FIFOBook::new()),
            acknowledged_orders: Default::default(),
            order_acknowledgement_channel: Default::default(),
            trade_channel: Default::default(),
        }
    }

    /// Returns the channel over which to receive trades from the given client
    pub async fn get_trade_channel_receiver(
        &self,
        client_id: ClientId,
    ) -> broadcast::Receiver<Trade> {
        let mut trade_channel = self.trade_channel.write().await;
        if !trade_channel.contains_key(&client_id) {
            trade_channel.insert(client_id, broadcast::channel(ACK_CHANNEL_CAPACITY));
        }
        let (tx, _) = trade_channel.get(&client_id).unwrap();
        tx.subscribe()
    }

    /// Returns the channel over which to send trades to the given client
    pub async fn get_trade_channel_sender(&self, client_id: ClientId) -> broadcast::Sender<Trade> {
        let mut trade_channel = self.trade_channel.write().await;
        if !trade_channel.contains_key(&client_id) {
            trade_channel.insert(client_id, broadcast::channel(ACK_CHANNEL_CAPACITY));
        }
        let (tx, _) = trade_channel.get(&client_id).unwrap();
        tx.clone()
    }
}

#[async_trait]
impl StateMachine for MENode {
    /// Received the command (=client request) replicated using Raft, and applies it to the matching
    /// engine state machine
    async fn apply_command(&self, command: &Command, is_leader: bool) -> () {
        info!(
            "State machine received command with seq num {:}",
            &command.sequence_id
        );

        {
            let mut sm = self.state_machine.lock().await;
            let already_applied = sm
                .acknowledged_orders
                .contains(&(command.client_id, command.sequence_id));
            if already_applied {
                return;
            }

            sm.acknowledged_orders
                .insert((command.client_id, command.sequence_id));

            let book_order = book::Order::from_command(command);
            info!("Applying book order: {:?}", &book_order);
            sm.book.apply(book_order);

            let trades = sm.book.check_for_trades();
            info!("Possible trades: {:?}", trades);
            let tx_trades = sm.get_trade_channel_sender(command.client_id).await;
            for trade in trades {
                let execution_price = trade.bid.price;
                tx_trades.send(Trade {
                    order: Some(trade.ask.into_proto()),
                    execution_price,
                    execution_volume: trade.quantity,
                });
                tx_trades.send(Trade {
                    order: Some(trade.bid.into_proto()),
                    execution_price,
                    execution_volume: trade.quantity,
                });
            }
        }

        if !is_leader {
            return;
        }

        let tx = {
            let sm = self.state_machine.lock().await;
            let ack_channel_map = sm.order_acknowledgement_channel.write().await;
            if !ack_channel_map.contains_key(&command.client_id) {
                drop(ack_channel_map);
                let mut ack_channel_map = sm.order_acknowledgement_channel.write().await;
                if !ack_channel_map.contains_key(&command.client_id) {
                    ack_channel_map
                        .insert(command.client_id, broadcast::channel(ACK_CHANNEL_CAPACITY));
                }
                let (tx, _) = ack_channel_map.get(&command.client_id).unwrap();
                tx.clone()
            } else {
                let (tx, _) = ack_channel_map.get(&command.client_id).unwrap();
                tx.clone()
            }
        };

        let ack = OrderAcknowledgement {
            client_id: command.client_id,
            seq_number: command.sequence_id,
            success: true,
        };
        tx.send(ack);
    }
}

pub struct MENode {
    pub state_machine: Arc<Mutex<MEStateMachine>>,
}

impl MENode {
    pub fn new() -> Self {
        Self {
            state_machine: Arc::new(Mutex::new(MEStateMachine::new())),
        }
    }
}
