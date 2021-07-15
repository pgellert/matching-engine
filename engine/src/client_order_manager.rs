use std::cmp::{Ord, Ordering};
use std::collections::{BinaryHeap, HashSet};
use std::time::Instant;

use tokio::stream::StreamExt;
use tracing::debug;

use crate::protobuf::*;

/// Maintains outstanding client orders to be retried when the timeout is reached
pub(crate) struct ClientOrderManager {
    orders: BinaryHeap<OrderRequest>,
    done: HashSet<u64>,
}

impl ClientOrderManager {
    pub fn new() -> Self {
        Self {
            orders: BinaryHeap::new(),
            done: HashSet::new(),
        }
    }

    /// Adds a new order to the end of the order list that is ready to be sent
    pub fn add_order(&mut self, order: Order) {
        self.orders.push(OrderRequest::Ready(order));
    }

    /// Adds a new order that becomes ready after the [timeout] is reached
    pub fn add_order_with_timeout(&mut self, order: Order, timeout: Instant) {
        self.orders
            .push(OrderRequest::WaitingResponse(order, timeout));
    }

    /// Completes the order with the given sequence number. This is used to signal that the order no
    /// longer needs to be retried.
    pub fn complete_order(&mut self, seq_number: u64) {
        self.done.insert(seq_number);
    }

    /// If there are orders that are ready to be sent or retried, it returns an order, otherwise it
    /// returns None.
    pub fn poll_next_order(&mut self) -> Option<Order> {
        loop {
            let order = match self.orders.peek() {
                Some(OrderRequest::Ready(_)) => {
                    self.orders.pop().map(|order_req| order_req.into_order())
                }
                Some(OrderRequest::WaitingResponse(_, timeout)) => {
                    if Instant::now() > *timeout {
                        self.orders.pop().map(|o_req| o_req.into_order())
                    } else {
                        None
                    }
                }
                _ => None,
            };
            debug!("Possible order: {:?}", order);
            match order {
                None => return None,
                Some(order) => {
                    if self.done.contains(&order.seq_number) {
                        debug!("Already ACK'd: {:?}", order);
                        continue;
                    } else {
                        return Some(order);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum OrderRequest {
    Ready(Order),
    WaitingResponse(Order, Instant),
    Done(Order),
}

impl OrderRequest {
    fn into_order(self) -> Order {
        match self {
            OrderRequest::Ready(order) => order,
            OrderRequest::WaitingResponse(order, _) => order,
            OrderRequest::Done(order) => order,
        }
    }
}

impl Eq for Order {}

impl Eq for OrderRequest {}

impl PartialEq for OrderRequest {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq_number.cmp(&other.seq_number)
    }
}

impl Ord for OrderRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        use OrderRequest::{Done, Ready, WaitingResponse};

        match (self, other) {
            (Ready(ord), Ready(other_ord)) => ord.cmp(other_ord).reverse(),
            (Ready(_), _) => Ordering::Greater,
            (_, Ready(_)) => Ordering::Less,
            (WaitingResponse(order, timeout), WaitingResponse(other_order, other_timeout)) => {
                let primary_ord = timeout.cmp(other_timeout).reverse();
                if primary_ord == Ordering::Equal {
                    return order.cmp(other_order).reverse();
                }
                primary_ord
            }
            (WaitingResponse(_, _), _) => Ordering::Greater,
            (_, WaitingResponse(_, _)) => Ordering::Less,
            (Done(ord), Done(other_ord)) => ord.cmp(other_ord).reverse(),
        }
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd for OrderRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_ordering_works() {
        let mut hs = BinaryHeap::new();
        hs.push(OrderRequest::Ready(generate_random_order_request(0, 1)));
        hs.push(OrderRequest::Ready(generate_random_order_request(0, 2)));
        hs.push(OrderRequest::WaitingResponse(
            generate_random_order_request(0, 3),
            Instant::now() + Duration::from_secs(10),
        ));
        hs.push(OrderRequest::WaitingResponse(
            generate_random_order_request(0, 4),
            Instant::now() + Duration::from_secs(1),
        ));
        hs.push(OrderRequest::Done(generate_random_order_request(0, 5)));

        while let Some(order_req) = hs.pop() {
            println!("{:?}", order_req);
        }
    }

    #[test]
    fn test_streaming_order_manager() {
        let mut om = ClientOrderManager::new();
        let orders: Vec<Order> = (1..=5)
            .map(|_seq| generate_random_order_request(0, 2))
            .collect();
        orders.into_iter().for_each(|order| om.add_order(order));

        let mut hs = BinaryHeap::new();
        hs.push(OrderRequest::Ready(generate_random_order_request(0, 2)));
        hs.push(OrderRequest::Ready(generate_random_order_request(0, 2)));
        hs.push(OrderRequest::WaitingResponse(
            generate_random_order_request(0, 3),
            Instant::now() + Duration::from_secs(10),
        ));
        hs.push(OrderRequest::WaitingResponse(
            generate_random_order_request(0, 4),
            Instant::now() + Duration::from_secs(1),
        ));
        hs.push(OrderRequest::Done(generate_random_order_request(0, 5)));

        while let Some(order_req) = hs.pop() {
            println!("{:?}", order_req);
        }
    }

    fn generate_random_order_request(client_id: u64, seq_number: u64) -> Order {
        Order {
            client_id,
            price: 1754,
            seq_number,
            side: OrderSide::Buy as i32,
            size: 1,
            security_id: 0,
        }
    }
}
