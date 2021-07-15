use std::sync::{Arc, Mutex};

use tokio::sync::watch;
use tokio::time::{delay_for, Duration};

const MAX_ID: u32 = 10;

struct ME {
    last_log_index_tx: watch::Sender<u32>,
    last_log_index_rx: watch::Receiver<u32>,
}

#[tokio::test]
async fn main() {
    let (tx, rx) = watch::channel(0);
    let me = ME {
        last_log_index_tx: tx,
        last_log_index_rx: rx,
    };
    let me = Arc::new(Mutex::new(me));

    let mut handles = vec![];

    let mm = me.clone();
    let tx_handle = tokio::spawn(async move {
        for id in 1..=MAX_ID {
            delay_for(Duration::from_millis(100)).await;
            let me = mm.lock().unwrap();
            me.last_log_index_tx.broadcast(id);
        }
    });

    for id in 1..=MAX_ID {
        let mm = me.clone();

        handles.push(tokio::spawn(async move {
            let mut rx = {
                let me = mm.lock().unwrap();
                me.last_log_index_rx.clone()
            };
            while let Some(index) = rx.recv().await {
                println!("received = {:?}", index);
                if index >= id {
                    return;
                }
            }
        }));
    }

    tokio::join!(tx_handle);
    for handle in handles {
        tokio::join!(handle);
    }
}
