use std::io::{self, ErrorKind};
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;

use byteorder::ByteOrder;

#[cfg(feature = "rt-tokio")]
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub mod async_buffered;
pub mod async_framed;
pub mod async_socket;

pub(crate) async fn default_read_exact<R: AsyncRead + ?Sized>(
    this: &mut R,
    mut buf: &mut [u8],
) -> io::Result<()> {
    while !buf.is_empty() {
        match this.read(buf).await {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    if !buf.is_empty() {
        Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        ))
    } else {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncRead {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        default_read_exact(self, buf).await
    }
}

#[async_trait]
pub trait AsyncWrite {
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize>;

    async fn flush(&mut self) -> io::Result<()>;
}

#[async_trait]
impl<R: AsyncRead + ?Sized + Send> AsyncRead for Box<R> {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read(buf).await
    }
}

#[async_trait]
impl<R: AsyncWrite + ?Sized + Send> AsyncWrite for Box<R> {
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (**self).write(buf).await
    }
    async fn flush(&mut self) -> io::Result<()> {
        (**self).flush().await
    }
}

/// Identifies a transport used by a `TAsyncInputProtocol` to receive bytes.
#[async_trait]
pub trait TAsyncReadTransport: AsyncRead {}

/// Identifies a transport used by `TAsyncOutputProtocol` to send bytes.
#[async_trait]
pub trait TAsyncWriteTransport: AsyncWrite {}

impl<T> TAsyncReadTransport for T where T: AsyncRead {}

impl<T> TAsyncWriteTransport for T where T: AsyncWrite {}

/// Helper type used by a server to create `TAsyncReadTransport` instances for
/// accepted client connections.
pub trait TAsyncReadTransportFactory {
    /// Create a `TTransport` that wraps a channel over which bytes are to be read.
    fn create(&self, channel: Box<dyn AsyncRead + Send>) -> Box<dyn TAsyncReadTransport + Send>;
}

/// Helper type used by a server to create `TWriteTransport` instances for
/// accepted client connections.
pub trait TAsyncWriteTransportFactory {
    /// Create a `TTransport` that wraps a channel over which bytes are to be sent.
    fn create(&self, channel: Box<dyn AsyncWrite + Send>) -> Box<dyn TAsyncWriteTransport + Send>;
}

impl<T> TAsyncReadTransportFactory for Box<T>
where
    T: TAsyncReadTransportFactory,
{
    fn create(&self, channel: Box<dyn AsyncRead + Send>) -> Box<dyn TAsyncReadTransport + Send> {
        (**self).create(channel)
    }
}

impl<T> TAsyncWriteTransportFactory for Box<T>
where
    T: TAsyncWriteTransportFactory,
{
    fn create(&self, channel: Box<dyn AsyncWrite + Send>) -> Box<dyn TAsyncWriteTransport + Send> {
        (**self).create(channel)
    }
}

pub trait TAsyncIoChannel: AsyncRead + AsyncWrite {
    /// Split the channel into a readable half and a writable half, where the
    /// readable half implements `io::AsyncRead` and the writable half implements
    /// `io::AsyncWrite`. Returns `None` if the channel was not initialized, or if it
    /// cannot be split safely.
    ///
    /// Returned halves may share the underlying OS channel or buffer resources.
    /// Implementations **should ensure** that these two halves can be safely
    /// used independently by concurrent threads.
    #[cfg(feature = "rt-async-std")]
    fn split(&self) -> crate::Result<(AsyncReadHalf<Self>, AsyncWriteHalf<Self>)>
    where
        Self: Sized;

    #[cfg(feature = "rt-tokio")]
    fn split(
        &mut self,
    ) -> crate::Result<(AsyncReadHalf<OwnedReadHalf>, AsyncWriteHalf<OwnedWriteHalf>)>
    where
        Self: Sized;
}

// The readable half of an object returned from `TIoChannel::split`.
#[derive(Debug)]
pub struct AsyncReadHalf<C>
where
    C: AsyncRead,
{
    handle: C,
}

/// The writable half of an object returned from `TIoChannel::split`.
#[derive(Debug)]
pub struct AsyncWriteHalf<C>
where
    C: AsyncWrite,
{
    handle: C,
}

impl<C> AsyncReadHalf<C>
where
    C: AsyncRead,
{
    /// Create a `AsyncReadHalf` associated with readable `handle`
    pub fn new(handle: C) -> AsyncReadHalf<C> {
        AsyncReadHalf { handle }
    }
}

impl<C> AsyncWriteHalf<C>
where
    C: AsyncWrite,
{
    /// Create a `AsyncWriteHalf` associated with writable `handle`
    pub fn new(handle: C) -> AsyncWriteHalf<C> {
        AsyncWriteHalf { handle }
    }
}

#[async_trait]
impl<C> AsyncRead for AsyncReadHalf<C>
where
    C: AsyncRead + std::marker::Send,
{
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.handle.read(buf).await
    }
}

#[async_trait]
impl<C> AsyncWrite for AsyncWriteHalf<C>
where
    C: AsyncWrite + std::marker::Send,
{
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.handle.write(buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.handle.flush().await
    }
}

impl<C> Deref for AsyncReadHalf<C>
where
    C: AsyncRead,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<C> DerefMut for AsyncReadHalf<C>
where
    C: AsyncRead,
{
    fn deref_mut(&mut self) -> &mut C {
        &mut self.handle
    }
}

impl<C> Deref for AsyncWriteHalf<C>
where
    C: AsyncWrite,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<C> DerefMut for AsyncWriteHalf<C>
where
    C: AsyncWrite,
{
    fn deref_mut(&mut self) -> &mut C {
        &mut self.handle
    }
}

#[async_trait]
pub trait AsyncReadBytesExt: AsyncRead {
    #[inline]
    async fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).await?;
        Ok(buf[0])
    }

    #[inline]
    async fn read_i8(&mut self) -> io::Result<i8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).await?;
        Ok(buf[0] as i8)
    }

    #[inline]
    async fn read_u16<T: ByteOrder>(&mut self) -> io::Result<u16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).await?;
        Ok(T::read_u16(&buf))
    }

    #[inline]
    async fn read_i16<T: ByteOrder>(&mut self) -> io::Result<i16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).await?;
        Ok(T::read_i16(&buf))
    }

    #[inline]
    async fn read_u32<T: ByteOrder>(&mut self) -> io::Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).await?;
        Ok(T::read_u32(&buf))
    }

    #[inline]
    async fn read_i32<T: ByteOrder>(&mut self) -> io::Result<i32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).await?;
        Ok(T::read_i32(&buf))
    }

    #[inline]
    async fn read_u64<T: ByteOrder>(&mut self) -> io::Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).await?;
        Ok(T::read_u64(&buf))
    }

    #[inline]
    async fn read_i64<T: ByteOrder>(&mut self) -> io::Result<i64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).await?;
        Ok(T::read_i64(&buf))
    }

    #[inline]
    async fn read_f32<T: ByteOrder>(&mut self) -> io::Result<f32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).await?;
        Ok(T::read_f32(&buf))
    }

    #[inline]
    async fn read_f64<T: ByteOrder>(&mut self) -> io::Result<f64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).await?;
        Ok(T::read_f64(&buf))
    }
}

impl<AR: AsyncRead> AsyncReadBytesExt for AR {}

#[async_trait]
pub trait AsyncWriteBytesExt: AsyncWrite {
    #[inline]
    async fn write_u8(&mut self, n: u8) -> io::Result<()> {
        self.write_all(&[n]).await
    }

    #[inline]
    async fn write_i8(&mut self, n: i8) -> io::Result<()> {
        self.write_all(&[n as u8]).await
    }

    #[inline]
    async fn write_u16<T: ByteOrder>(&mut self, n: u16) -> io::Result<()> {
        let mut buf = [0; 2];
        T::write_u16(&mut buf, n);
        self.write_all(&buf).await
    }

    #[inline]
    async fn write_i16<T: ByteOrder>(&mut self, n: i16) -> io::Result<()> {
        let mut buf = [0; 2];
        T::write_i16(&mut buf, n);
        self.write_all(&buf).await
    }

    #[inline]
    async fn write_u32<T: ByteOrder>(&mut self, n: u32) -> io::Result<()> {
        let mut buf = [0; 4];
        T::write_u32(&mut buf, n);
        self.write_all(&buf).await
    }

    #[inline]
    async fn write_i32<T: ByteOrder>(&mut self, n: i32) -> io::Result<()> {
        let mut buf = [0; 4];
        T::write_i32(&mut buf, n);
        self.write_all(&buf).await
    }

    #[inline]
    async fn write_u64<T: ByteOrder>(&mut self, n: u64) -> io::Result<()> {
        let mut buf = [0; 8];
        T::write_u64(&mut buf, n);
        self.write_all(&buf).await
    }

    #[inline]
    async fn write_i64<T: ByteOrder>(&mut self, n: i64) -> io::Result<()> {
        let mut buf = [0; 8];
        T::write_i64(&mut buf, n);
        self.write_all(&buf).await
    }

    #[inline]
    async fn write_f64<T: ByteOrder>(&mut self, n: f64) -> io::Result<()> {
        let mut buf = [0; 8];
        T::write_f64(&mut buf, n);
        self.write_all(&buf).await
    }

    async fn write_all(&mut self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write(buf).await {
                Ok(0) => {
                    return Err(io::Error::new(
                        ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl<AW: AsyncWrite> AsyncWriteBytesExt for AW {}
