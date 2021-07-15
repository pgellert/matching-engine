use std::collections::BinaryHeap;

use crate::algos::book::{Book, Order, Side, Trade};

/// Price-time priority (or FIFO) matching engine implemented using a [BinaryHeap].
///
/// Implemented as a state-machine to be used for replication with Raft.
#[derive(Debug, Clone)]
pub struct FIFOBook {
    asks: BinaryHeap<Order>,
    bids: BinaryHeap<Order>,
}

impl FIFOBook {
    pub fn new() -> Self {
        Self {
            asks: Default::default(),
            bids: Default::default(),
        }
    }
}

impl Book for FIFOBook {
    fn apply(&mut self, order: Order) {
        match order.side {
            Side::Buy => &self.bids.push(order),
            Side::Sell => &self.asks.push(order),
        };
    }

    fn check_for_trades(&mut self) -> Vec<Trade> {
        let mut trades = vec![];
        while let (Some(ask), Some(bid)) = (self.asks.pop(), self.bids.pop()) {
            match Order::merge(ask, bid) {
                None => break,
                Some((trade, remainder)) => {
                    trades.push(trade);
                    if let Some(rem) = remainder {
                        match rem.side {
                            Side::Buy => self.bids.push(rem),
                            Side::Sell => self.asks.push(rem),
                        }
                    }
                }
            }
        }
        trades
    }

    fn cancel(&mut self, _order_id: (u64, u64), _side: Side) -> bool {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn make_default_order(side: Side, price: u64, size: u64) -> Order {
        Order {
            client_id: 0,
            seq_number: 0,
            price,
            size,
            side,
        }
    }

    fn make_default_queue(side: Side) -> BinaryHeap<Order> {
        BinaryHeap::from(vec![
            make_default_order(side, 0, 10),
            make_default_order(side, 3, 25),
            make_default_order(side, 2, 50),
        ])
    }

    fn make_tradable_ask_queue() -> BinaryHeap<Order> {
        BinaryHeap::from(vec![
            make_default_order(Side::Sell, 2, 10),
            make_default_order(Side::Sell, 3, 25),
            make_default_order(Side::Sell, 2, 50),
        ])
    }

    fn make_tradable_bid_queue() -> BinaryHeap<Order> {
        BinaryHeap::from(vec![
            make_default_order(Side::Buy, 1, 1),
            make_default_order(Side::Buy, 3, 25),
            make_default_order(Side::Buy, 2, 50),
        ])
    }

    #[test]
    fn test_construction() {
        let book = FIFOBook {
            asks: make_default_queue(Side::Sell),
            bids: make_default_queue(Side::Buy),
        };
        println!("Hello book!");
        println!("Bids: {:?}", &book.bids);
        println!("Asks: {:?}", &book.asks);
    }

    #[test]
    fn test_apply() {
        let mut book = FIFOBook {
            asks: make_default_queue(Side::Sell),
            bids: make_default_queue(Side::Buy),
        };
        book.apply(make_default_order(Side::Sell, 2, 1));
        println!("Bids: {:?}", &book.bids);
        println!("Asks: {:?}", &book.asks);
        assert_eq!(book.asks.len(), 4);
        assert_eq!(book.bids.len(), 3);
    }

    #[test]
    fn test_check_trades() {
        let mut book = FIFOBook {
            asks: make_tradable_ask_queue(),
            bids: make_tradable_bid_queue(),
        };
        let trades = book.check_for_trades();
        println!("Bids: {:?}", &book.bids);
        println!("Asks: {:?}", &book.asks);
        println!("Trades: {:?}", &trades);
        assert_eq!(trades.is_empty(), false);
    }

    #[test]
    fn test_check_trades_performance() {
        let orders = generate_tradable_orders(1_000_000);
        measure_book_performance(orders);
    }

    fn measure_book_performance(orders: Vec<Order>) {
        let mut book = FIFOBook::new();
        for order in orders {
            book.apply(order);
            book.check_for_trades();
        }
    }

    fn generate_tradable_orders(n: u64) -> Vec<Order> {
        let mut rand = rand::thread_rng();
        let mut last_ask = 0;
        let mut last_bid = 0;
        let mut orders = vec![];
        for _ in 0..n {
            let (price, size, side) = if rand.gen_bool(0.5) {
                // Generate buy order
                let price = std::cmp::max(1, last_ask as i64 + rand.gen_range(-10..10)) as u64;
                last_bid = price;
                (price, rand.gen_range(0..20), Side::Buy)
            } else {
                // Generate sell order
                let price = std::cmp::max(1, last_bid as i64 + rand.gen_range(-10..10)) as u64;
                last_ask = price;
                (price, rand.gen_range(0..20), Side::Sell)
            };

            orders.push(Order {
                client_id: 0,
                seq_number: 0,
                size,
                side,
                price,
            });
        }
        return orders;
    }
}
