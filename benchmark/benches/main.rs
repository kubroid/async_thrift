use async_thrift::{
    protocol::async_binary::{TAsyncBinaryInputProtocol, TAsyncBinaryOutputProtocol},
    transport::{
        async_framed::{TAsyncFramedReadTransport, TAsyncFramedWriteTransport},
        async_socket::TAsyncTcpChannel,
        AsyncReadHalf, AsyncWriteHalf, TAsyncIoChannel,
    },
};
use thrift::idl::BenchAsyncClient;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use criterion::{criterion_group, criterion_main, Criterion};

pub type ClientType = BenchAsyncClient<
    TAsyncBinaryInputProtocol<TAsyncFramedReadTransport<AsyncReadHalf<OwnedReadHalf>>>,
    TAsyncBinaryOutputProtocol<TAsyncFramedWriteTransport<AsyncWriteHalf<OwnedWriteHalf>>>,
>;

pub async fn make_client() -> async_thrift::Result<ClientType> {
    let stream = TcpStream::connect(("localhost", 7070)).await.unwrap();

    let mut c = TAsyncTcpChannel::with_stream(stream);

    match c.split() {
        Ok((i_chan, o_chan)) => {
            let i_prot =
                TAsyncBinaryInputProtocol::new(TAsyncFramedReadTransport::new(i_chan), true);
            let o_prot =
                TAsyncBinaryOutputProtocol::new(TAsyncFramedWriteTransport::new(o_chan), true);

            Ok(BenchAsyncClient::new(i_prot, o_prot))
        }
        Err(_) => Err(async_thrift::Error::Application(
            async_thrift::ApplicationError::new(
                async_thrift::ApplicationErrorKind::InternalError,
                "channel split failure",
            ),
        )),
    }
}

// #[tokio::main]
// async fn main() {
//     let mut client = make_client().await.unwrap();
//     client.fast("p".to_owned()).await.unwrap();
// }

// fn criterion_benchmark(c: &mut Criterion) {
//     c.bench_function("tokio async function", |b| {
//         // Use the `Tokio` executor
//         b.to_async(AsyncExecutor).iter(|| async {
//             let client = make_client().await;
//             // Your async operations...
//         })
//     });
// }

async fn do_something(size: usize) {
    let mut client = make_client().await.unwrap();
    //    client.fast("p".to_owned()).await.unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let size: usize = 1024;

    c.bench_with_input(
        criterion::BenchmarkId::new("input_example", size),
        &size,
        |b, &s| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(
                tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .unwrap(),
            )
            .iter(|| do_something(s));
        },
    );
}
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
