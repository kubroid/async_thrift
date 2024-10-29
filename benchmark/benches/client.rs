use benchmark::{make_client, run_server};
use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::join_all;
use thrift::idl::TBenchAsyncClient as _;
use tokio::task::JoinHandle;

async fn call_fast(size: usize) {
    let mut client = make_client("localhost", 7070).await.unwrap();
    let _ = client.fast("0".repeat(size)).await;
}

async fn call_slow(size: usize) {
    let mut client = make_client("localhost", 7070).await.unwrap();
    let _ = client.slow("0".repeat(size)).await;
}

fn criterion_benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    run_server(&rt, "127.0.0.1:7070".to_owned());

    for size in [1, 100, 1000, 5000, 10000] {
        c.bench_function(format!("fast({})", size).as_str(), |b| {
            b.to_async(&rt).iter(|| call_fast(size));
        });

        c.bench_function(format!("slow({})", size).as_str(), |b| {
            b.to_async(&rt).iter(|| call_slow(size));
        });
    }

    let size = 100;
    for batch in [10, 25, 50, 75, 100, 250, 500, 750] {
        c.bench_function(
            format!("fast({}) batch of {:3}", size, batch).as_str(),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let mut handlers: Vec<JoinHandle<()>> = Vec::with_capacity(batch);
                    for _i in 0..batch {
                        handlers.push(rt.spawn(call_fast(size)));
                    }
                    let _ = join_all(handlers).await;
                })
            },
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(1))
        .sample_size(1000)
        .measurement_time(std::time::Duration::from_secs(1));
    targets = criterion_benches
}
criterion_main!(benches);
