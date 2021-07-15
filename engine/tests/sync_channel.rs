use std::sync::mpsc::sync_channel;

const N: u32 = 10;

#[tokio::test]
async fn explore_sync_channel() {
    let (tx, rx) = sync_channel(0);

    let th1 = tokio::spawn(async move {
        let start = std::time::Instant::now();
        for id in 0..N {
            println!("sent value = {:?} at {:?}", id, start.elapsed());
            tx.send(id);
        }
    });

    let th2 = tokio::task::spawn_blocking(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        while let Ok(value) = rx.recv() {
            println!("got value = {:?}", value);
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    });

    tokio::join!(th1, th2);
}
