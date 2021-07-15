extern crate engine;

use std::collections::linked_list::LinkedList;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rand::Rng;

use engine::algos::book::*;
use engine::algos::*;

fn fill_order_book(mut book: &mut dyn Book, n_orders: u64) {
    // Set up a book with significant volume
    for _ in 0..n_orders {
        let order = Order {
            client_id: 0,
            seq_number: 0,
            price: 1100, //rand::thread_rng().gen_range(1000..=1100) / 10 * 10,
            size: rand::thread_rng().gen_range(1..=20),
            side: Side::Buy,
        };
        book.apply(order);
        let order = Order {
            client_id: 0,
            seq_number: 0,
            price: 1000, //rand::thread_rng().gen_range(1000..=1100) / 10 * 10,
            size: rand::thread_rng().gen_range(1..=20),
            side: Side::Sell,
        };
        book.apply(order);
    }
    // Clear trades out of the book
    book.check_for_trades();

    // Add a new tradeable order
    let order = Order {
        client_id: 0,
        seq_number: 0,
        price: 1000,
        size: 1000,
        side: Side::Sell,
    };
    book.apply(order);
}

fn criterion_benchmark(c: &mut Criterion) {
    for n_orders in vec![100, 1_000, 10_000, 100_000, 1_000_000].into_iter() {
        let mut book = art_book::FIFOBook::new();

        fill_order_book(&mut book, n_orders);
        c.bench_function(&*format!("check_for_trades_{:}", n_orders), move |b| {
            // iter_batched_ref avoids timing the construction and destruction of the book
            b.iter_batched_ref(
                || book.clone(),
                |data| {
                    let mut data = black_box(data);
                    data.check_for_trades()
                },
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
