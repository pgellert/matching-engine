use std::cmp::{Ord, Ordering};

use crate::protobuf;

pub type ClientId = u64;
pub type SequenceNum = u64;
pub type OrderId = (ClientId, SequenceNum);
pub type Price = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// Container for data about an order
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub client_id: ClientId,
    pub seq_number: SequenceNum,
    pub price: Price,
    pub size: u64,
    pub side: Side,
}

impl Order {
    /// Initialises an order from an RPC order definition
    #[inline]
    fn from_proto(order: &protobuf::Order) -> Self {
        let side = match protobuf::OrderSide::from_i32(order.side).unwrap() {
            protobuf::OrderSide::Buy => Side::Buy,
            protobuf::OrderSide::Sell => Side::Sell,
        };
        Self {
            client_id: order.client_id,
            seq_number: order.seq_number,
            price: order.price,
            size: order.size,
            side,
        }
    }

    /// Creates an RPC order definition
    #[inline]
    pub(crate) fn into_proto(self) -> protobuf::Order {
        let side = match self.side {
            Side::Buy => protobuf::OrderSide::Buy,
            Side::Sell => protobuf::OrderSide::Sell,
        } as i32;
        protobuf::Order {
            client_id: self.client_id,
            seq_number: self.seq_number,
            security_id: 0,
            price: self.price,
            size: self.size,
            side,
        }
    }

    /// Creates a Raft command from this order
    #[inline]
    pub(crate) fn from_command(command: &raft::protobuf::Command) -> Self {
        let side = match raft::protobuf::OrderSide::from_i32(command.side).unwrap() {
            raft::protobuf::OrderSide::Buy => Side::Buy,
            raft::protobuf::OrderSide::Sell => Side::Sell,
            _ => panic!("Only Buy and Sell orders supported"),
        };
        Self {
            client_id: command.client_id,
            seq_number: command.sequence_id,
            price: command.price,
            size: command.size,
            side,
        }
    }

    fn partial_cmp_buy(&self, other: &Self) -> Option<Ordering> {
        Some(self.price.cmp(&other.price))
    }

    fn partial_cmp_sell(&self, other: &Self) -> Option<Ordering> {
        Some(other.price.cmp(&self.price))
    }

    /// Merges two orders and, if they are tradeable, returns the generated trade and an optional
    /// order that remains from filling the two inputs.
    pub fn merge(self, other: Self) -> Option<(Trade, Option<Self>)> {
        let (ask, bid) = match (self.side, other.side) {
            (Side::Buy, Side::Sell) => (other, self),
            (Side::Sell, Side::Buy) => (self, other),
            (_, _) => return None,
        };

        if ask.price > bid.price {
            return None;
        }

        match ask.size.cmp(&bid.size) {
            Ordering::Equal => {
                let quantity = ask.size;
                Some((Trade { quantity, ask, bid }, None))
            }
            Ordering::Greater => {
                let quantity = bid.size;
                let mut remainder = ask.clone();
                remainder.size -= quantity;
                Some((Trade { quantity, ask, bid }, Some(remainder)))
            }
            Ordering::Less => {
                let quantity = ask.size;
                let mut remainder = bid.clone();
                remainder.size -= quantity;
                Some((Trade { quantity, ask, bid }, Some(remainder)))
            }
        }
    }

    #[inline]
    pub(crate) fn has_id(&self, order_id: OrderId) -> bool {
        (self.client_id, self.seq_number) == order_id
    }

    #[inline]
    pub(crate) fn id(&self) -> OrderId {
        (self.client_id, self.seq_number)
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (&self.side, &other.side) {
            (&Side::Buy, &Side::Buy) => self.partial_cmp_buy(other),
            (&Side::Sell, &Side::Sell) => self.partial_cmp_sell(other),
            (_, _) => None,
        }
    }
}

impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal) // Sell and Buy are non-comparable
    }
}

/// Container for data about generated trades
#[derive(Debug)]
pub struct Trade {
    pub quantity: u64,
    pub ask: Order,
    pub bid: Order,
}

/// Standardised order book interface
pub trait Book {
    fn apply(&mut self, order: Order);
    fn check_for_trades(&mut self) -> Vec<Trade>;
    fn cancel(&mut self, order_id: OrderId, side: Side) -> bool;
}
