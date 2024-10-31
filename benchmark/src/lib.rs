#[cfg(all(not(feature = "rt-tokio"), not(feature = "rt-async-std")))]
compile_error!("Please select a runtime version: [rt-tokio, rt-async-std]");

#[cfg(all(feature = "rt-tokio", feature = "rt-async-std"))]
compile_error!("Please select only one runtime version");

use std::time::Duration;

use async_thrift::{
    protocol::async_binary::{
        TAsyncBinaryInputProtocol, TAsyncBinaryInputProtocolFactory, TAsyncBinaryOutputProtocol,
        TAsyncBinaryOutputProtocolFactory,
    },
    server::asynced::TAsyncServer,
    transport::{
        async_framed::{
            TAsyncFramedReadTransport, TAsyncFramedReadTransportFactory,
            TAsyncFramedWriteTransport, TAsyncFramedWriteTransportFactory,
        },
        async_socket::TAsyncTcpChannel,
        AsyncReadHalf, AsyncWriteHalf, TAsyncIoChannel,
    },
};
use async_trait::async_trait;
use thrift::idl::{BenchAsyncClient, BenchAsyncHandler, BenchAsyncProcessor};

#[cfg(feature = "rt-tokio")]
pub use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    task,
    time::sleep,
};

#[cfg(feature = "rt-async-std")]
pub use async_std::{net::TcpStream, task, task::sleep, task::JoinHandle};

#[cfg(feature = "rt-tokio")]
pub type ClientType = BenchAsyncClient<
    TAsyncBinaryInputProtocol<TAsyncFramedReadTransport<AsyncReadHalf<OwnedReadHalf>>>,
    TAsyncBinaryOutputProtocol<TAsyncFramedWriteTransport<AsyncWriteHalf<OwnedWriteHalf>>>,
>;

#[cfg(feature = "rt-async-std")]
pub type ClientType = BenchAsyncClient<
    TAsyncBinaryInputProtocol<TAsyncFramedReadTransport<AsyncReadHalf<TAsyncTcpChannel>>>,
    TAsyncBinaryOutputProtocol<TAsyncFramedWriteTransport<AsyncWriteHalf<TAsyncTcpChannel>>>,
>;

pub async fn make_client(host: &str, port: u16) -> async_thrift::Result<ClientType> {
    let stream = TcpStream::connect((host, port)).await.unwrap();

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

struct Handler;

#[async_trait]
impl BenchAsyncHandler for Handler {
    async fn handle_fast(&self, p: String) -> async_thrift::Result<String> {
        Ok(p)
    }
    async fn handle_slow(&self, p: String) -> async_thrift::Result<String> {
        sleep(Duration::from_millis(50)).await;
        Ok(p)
    }
}

pub async fn run_server(bind: String) {
    let processor = BenchAsyncProcessor::new(Handler {});
    let mut server = TAsyncServer::new(
        TAsyncFramedReadTransportFactory::new(),
        TAsyncBinaryInputProtocolFactory::new(),
        TAsyncFramedWriteTransportFactory::new(),
        TAsyncBinaryOutputProtocolFactory::new(),
        processor,
    );

    task::spawn(async move {
        let _ = server.listen(bind.as_str()).await;
    });
}
