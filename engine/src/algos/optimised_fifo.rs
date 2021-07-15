use std::cmp::{max, min};
use std::collections::{HashMap, VecDeque};
use std::iter::FromIterator;

use crate::algos::book::*;

/// Price-time priority (or FIFO) matching engine implemented using a [HashMap] to index by price.
///
/// Implemented as a state-machine to be used for replication with Raft.
#[derive(Debug, Clone)]
pub struct FIFOBook {
    ask_price_buckets: HashMap<u64, VecDeque<Order>>,
    bid_price_buckets: HashMap<u64, VecDeque<Order>>,
    min_ask_price: u64,
    max_ask_price: u64,
    min_bid_price: u64,
    max_bid_price: u64,
    orders: HashMap<OrderId, Price>,
}

/// Order book interface implementation
impl FIFOBook {
    pub fn new() -> Self {
        Self {
            ask_price_buckets: HashMap::new(),
            bid_price_buckets: HashMap::new(),
            min_ask_price: u64::MAX,
            max_ask_price: u64::MIN,
            min_bid_price: u64::MAX,
            max_bid_price: u64::MIN,
            orders: Default::default(),
        }
    }

    fn pop_bid(&mut self) -> Option<Order> {
        for bid_price in (self.min_bid_price..=self.max_bid_price).rev() {
            self.max_bid_price = bid_price;
            match self.bid_price_buckets.get_mut(&bid_price) {
                Some(orders) if !orders.is_empty() => {
                    let order = orders.pop_front().unwrap();
                    return Some(order);
                }
                _ => continue,
            }
        }
        None
    }

    fn pop_ask(&mut self) -> Option<Order> {
        for ask_price in self.min_ask_price..=self.max_ask_price {
            self.min_ask_price = ask_price;
            match self.ask_price_buckets.get_mut(&ask_price) {
                Some(orders) if !orders.is_empty() => {
                    let order = orders.pop_front().unwrap();
                    return Some(order);
                }
                _ => continue,
            }
        }
        None
    }

    fn merge(&mut self, ask: Order, bid: Order) -> Option<(Trade, Option<Order>)> {
        let ask_id = ask.id();
        let bid_id = bid.id();
        let result = Order::merge(ask, bid);
        if let Some((_, remainder)) = &result {
            if remainder.as_ref().map_or(true, |rem| !rem.has_id(ask_id)) {
                self.orders.remove(&ask_id);
            }
            if remainder.as_ref().map_or(true, |rem| !rem.has_id(bid_id)) {
                self.orders.remove(&bid_id);
            }
        }
        result
    }
}

impl Book for FIFOBook {
    /// Adds a buy or sell order to the book
    fn apply(&mut self, order: Order) {
        self.orders
            .insert((order.client_id, order.seq_number), order.price);

        match order.side {
            Side::Buy => {
                let bucket_opt = self.bid_price_buckets.get_mut(&order.price);
                match bucket_opt {
                    None => {
                        self.min_bid_price = min(self.min_bid_price, order.price);
                        self.max_bid_price = max(self.max_bid_price, order.price);
                        self.bid_price_buckets
                            .insert(order.price, VecDeque::from_iter(vec![order]));
                    }
                    Some(bucket) => bucket.push_back(order),
                }
            }
            Side::Sell => {
                let bucket_opt = self.ask_price_buckets.get_mut(&order.price);
                match bucket_opt {
                    None => {
                        self.min_ask_price = min(self.min_ask_price, order.price);
                        self.max_ask_price = max(self.max_ask_price, order.price);
                        self.ask_price_buckets
                            .insert(order.price, VecDeque::from_iter(vec![order]));
                    }
                    Some(bucket) => bucket.push_back(order),
                }
            }
        };
    }

    /// Fills tradeable orders in the book and returns the generated trades.
    fn check_for_trades(&mut self) -> Vec<Trade> {
        if self.max_bid_price < self.min_ask_price {
            return Vec::new();
        }

        let mut trades = vec![];

        let (mut bid, mut ask) = match (self.pop_bid(), self.pop_ask()) {
            (Some(bid_new), Some(ask_new)) => (bid_new, ask_new),
            _ => return trades,
        };

        while let Some((trade, remainder)) = self.merge(ask, bid) {
            trades.push(trade);

            if let Some(rem) = remainder {
                match rem.side {
                    Side::Buy => {
                        if let Some(ask_new) = self.pop_ask() {
                            ask = ask_new;
                            bid = rem;
                        } else {
                            self.bid_price_buckets
                                .get_mut(&rem.price)
                                .unwrap()
                                .push_front(rem);
                            return trades;
                        }
                    }
                    Side::Sell => {
                        if let Some(bid_new) = self.pop_bid() {
                            bid = bid_new;
                            ask = rem;
                        } else {
                            self.ask_price_buckets
                                .get_mut(&rem.price)
                                .unwrap()
                                .push_front(rem);
                            return trades;
                        }
                    }
                }
            } else {
                match (self.pop_bid(), self.pop_ask()) {
                    (Some(bid_new), Some(ask_new)) => {
                        bid = bid_new;
                        ask = ask_new;
                    }
                    _ => return trades,
                };
            }
        }
        trades
    }

    /// Cancels the given order from the book
    fn cancel(&mut self, order_id: OrderId, side: Side) -> bool {
        if let Some(price) = self.orders.remove(&order_id) {
            let side_buckets = match side {
                Side::Buy => &mut self.bid_price_buckets,
                Side::Sell => &mut self.ask_price_buckets,
            };

            if let Some(bucket) = side_buckets.get_mut(&price) {
                if let Some(index) = bucket
                    .iter()
                    .position(|order| (order.client_id, order.seq_number) == order_id)
                {
                    bucket.remove(index);
                    return true;
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn make_default_order(side: Side, price: u64, size: u64, seq_number: u64) -> Order {
        Order {
            client_id: 1,
            seq_number,
            price,
            size,
            side,
        }
    }

    fn make_tradeable_book() -> FIFOBook {
        let mut book = FIFOBook::new();

        for &buy_at in &[2, 3] {
            book.apply(make_default_order(Side::Buy, buy_at, 1, buy_at));
        }
        for &sell_at in &[2, 3, 4, 5] {
            book.apply(make_default_order(Side::Sell, sell_at, 1, 10 + sell_at));
        }

        book
    }

    fn count_orders_in_book(book: &FIFOBook, side: Side) -> usize {
        let price_to_bucket = match side {
            Side::Buy => &book.bid_price_buckets,
            Side::Sell => &book.ask_price_buckets,
        };
        price_to_bucket.values().map(|bucket| bucket.len()).sum()
    }

    #[test]
    fn test_apply() {
        let book = make_tradeable_book();

        let bid_order_count = count_orders_in_book(&book, Side::Buy);
        let ask_order_count = count_orders_in_book(&book, Side::Sell);

        println!("Book: {:?}", &book);
        assert_eq!(bid_order_count, 2);
        assert_eq!(ask_order_count, 4);
    }

    #[test]
    fn test_check_trades() {
        let mut book = make_tradeable_book();

        let trades = book.check_for_trades();

        println!("Book: {:?}", &book);
        println!("Trades: {:?}", &trades);
        assert_eq!(trades.is_empty(), false);
        assert_eq!(book.orders.len(), 4);
    }

    #[test]
    fn test_cancel() {
        let mut book = FIFOBook::new();
        book.apply(Order {
            client_id: 12,
            seq_number: 1234,
            price: 1,
            size: 1,
            side: Side::Buy,
        });

        let success = book.cancel((12, 1234), Side::Buy);

        println!("Book: {:?}", &book);

        assert!(success);
        assert_eq!(book.orders.len(), 0);
    }

    #[test]
    fn test_partial_fill_works() {
        let mut book = FIFOBook::new();
        book.apply(make_default_order(Side::Buy, 3, 10, 1));
        book.apply(make_default_order(Side::Sell, 2, 3, 2));
        book.apply(make_default_order(Side::Sell, 2, 6, 3));
        book.apply(make_default_order(Side::Sell, 3, 3, 4));

        let trades = book.check_for_trades();
        println!("Book: {:?}", &book);
        println!("Trades: {:?}", &trades);

        assert_eq!(trades.len(), 3);
        assert_eq!(book.bid_price_buckets.get(&3).unwrap().is_empty(), true);
        assert_eq!(book.ask_price_buckets.get(&3).unwrap().is_empty(), false);
    }

    #[test]
    fn test_check_trades_performance() {
        let orders = generate_tradable_orders(100_000);
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
                let price = std::cmp::max(
                    1,
                    std::cmp::min(1000, last_ask as i64 + rand.gen_range(-10..10)),
                ) as u64;
                last_bid = price;
                (price, rand.gen_range(0..20), Side::Buy)
            } else {
                // Generate sell order
                let price = std::cmp::max(
                    1,
                    std::cmp::min(1000, last_bid as i64 + rand.gen_range(-10..10)),
                ) as u64;
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

    #[ignore]
    #[test]
    fn test_large_book() {
        let mut book = FIFOBook::new();
        for i in 0..100000 {
            let order = Order {
                client_id: 0,
                seq_number: 0,
                price: rand::thread_rng().gen_range(120000..=130000),
                size: rand::thread_rng().gen_range(1..=20),
                side: Side::Buy,
            };
            book.apply(order);
            let order = Order {
                client_id: 0,
                seq_number: 0,
                price: rand::thread_rng().gen_range(120000..=130000),
                size: rand::thread_rng().gen_range(1..=20),
                side: Side::Sell,
            };
            book.apply(order);
        }
        // Clear trades out of the book
        book.check_for_trades();
        for price in 120000..=130000 {
            println!(
                "{:?}: {:?} -- {:?}",
                price,
                book.bid_price_buckets
                    .get(&price)
                    .map_or(0, |bucket| bucket.len()),
                book.ask_price_buckets
                    .get(&price)
                    .map_or(0, |bucket| bucket.len())
            )
        }
    }
}
