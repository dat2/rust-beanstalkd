// https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
// https://tokio.rs/docs/getting-started/simple-server/use std::io;
use std::net::SocketAddr;
use std::io;
use std::sync::Mutex;
use std::thread;
use std::time::*;
use std::error::Error;

use tokio_core::reactor::Handle;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Encoder, Decoder, Framed};
use tokio_timer::*;
use bytes::BytesMut;
use tokio_proto::pipeline::ServerProto;
use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};
use futures::{future, IntoFuture, Future, BoxFuture, Poll, StartSend, Stream, Sink, Async};
use futures::sync::mpsc;
use futures::sync::oneshot;

use combine::primitives::{State, Parser};

use command::*;
use reply::*;
use log_service::Log;

// Codec
#[derive(Default)]
struct BeanstalkCodec;

impl Encoder for BeanstalkCodec {
  type Item = BeanstalkReply;
  type Error = io::Error;

  fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
    let bytes: Vec<u8> = item.into();
    dst.extend(bytes.iter());
    Ok(())
  }
}

impl Decoder for BeanstalkCodec {
  type Item = BeanstalkCommand;
  type Error = io::Error;

  fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    // check if there's at least one \r\n

    if let Some(index) = buf.iter().position(|&b| b == b'\r') {
      // calculate how many lines to drain
      let lines = if buf[0] == b'p' && buf[1] == b'u' && buf[2] == b't' {
        1
      } else {
        0
      };

      let mut drain_index = index + 2;
      let mut has_enough_lines = true;
      for _ in 0..lines {
        if let Some(j) = buf.iter().skip(drain_index).position(|&b| b == b'\r') {
          drain_index += j + 2;
        } else {
          has_enough_lines = false;
          break;
        }
      }

      if has_enough_lines {
        let drained_buffer = buf.split_to(drain_index).freeze();
        let state = State::new(drained_buffer.as_ref());
        match BeanstalkCommandParser::command().parse(state) {
          Ok((value, _state)) => Ok(Some(value)),
          Err(_error) => Ok(Some(BeanstalkCommand::Unknown)),
        }
      } else {
        Ok(None)
      }
    } else {
      Ok(None)
    }
  }
}

struct BeanstalkRequest {
  client_id: usize,
  command: BeanstalkCommand
}

impl BeanstalkRequest {
  fn new(client_id: usize, command: BeanstalkCommand) -> BeanstalkRequest {
    BeanstalkRequest { client_id: client_id, command: command }
  }
}

// TODO notify the application that we need to end this connection
struct WrappedFrameTransport<T> {
  client_id: usize,
  framed: Framed<T, BeanstalkCodec>,
}

impl<T: AsyncRead + AsyncWrite> WrappedFrameTransport<T> {
  fn new(client_id: usize, framed: Framed<T, BeanstalkCodec>) -> WrappedFrameTransport<T> {
    WrappedFrameTransport {
      client_id: client_id,
      framed: framed,
    }
  }
}

impl<T: AsyncRead + AsyncWrite + 'static> Stream for WrappedFrameTransport<T> {
  type Item = BeanstalkRequest;
  type Error = io::Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    let poll_result = self.framed.poll();

    poll_result.map(|async_option| {
      async_option.map(|async| {
        async.map(|command| BeanstalkRequest::new(self.client_id, command))
      })
    })
  }
}

impl<T: AsyncRead + AsyncWrite + 'static> Sink for WrappedFrameTransport<T> {
  type SinkItem = BeanstalkReply;
  type SinkError = io::Error;

  fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
    self.framed.start_send(item)
  }

  fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
    self.framed.poll_complete()
  }
}

// Protocol
struct BeanstalkProtocol {
  mutex: Mutex<usize>,
}

impl BeanstalkProtocol {
  fn new() -> BeanstalkProtocol {
    let mutex = Mutex::new(0);
    BeanstalkProtocol { mutex: mutex }
  }
}

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for BeanstalkProtocol {
  // request matches codec in type
  type Request = BeanstalkRequest;

  // response matches codec out type
  type Response = BeanstalkReply;

  // hook in the codec
  type Transport = WrappedFrameTransport<T>;
  type BindTransport = Result<Self::Transport, io::Error>;

  fn bind_transport(&self, io: T) -> Self::BindTransport {
    let mut mutex = self.mutex.lock().unwrap();
    *mutex += 1;
    info!(target: "beanstalkd", "[PROTO] Client {:?}", *mutex);
    Ok(WrappedFrameTransport::new(*mutex, io.framed(BeanstalkCodec)))
  }
}

enum ServiceMessage {
  Put(oneshot::Sender<usize>, Vec<u8>)
}

// Service
#[derive(Clone)]
struct BeanstalkService {
  tx: mpsc::UnboundedSender<ServiceMessage>,
}

impl Service for BeanstalkService {
  type Request = BeanstalkRequest;
  type Response = BeanstalkReply;
  type Error = io::Error;
  type Future = BoxFuture<Self::Response, Self::Error>;

  fn call(&self, req: Self::Request) -> Self::Future {
    match req.command {
      BeanstalkCommand::Put(_priority, _delay, _ttr, _bytes, data) => {
        let (tx, rx) = oneshot::channel::<usize>();

        (&self.tx).send(ServiceMessage::Put(tx, data))
          .into_future()
          .then(|_| future::ok(BeanstalkReply::Ok(Vec::new())))
          .boxed()
      }
      BeanstalkCommand::Unknown => {
        future::ok(BeanstalkReply::Error(BeanstalkError::UnknownCommand)).boxed()
      }
      _c => future::ok(BeanstalkReply::Ok(Vec::new())).boxed(),
    }
  }
}

struct BeanstalkServiceFactory {
  tx: mpsc::UnboundedSender<ServiceMessage>
}

impl BeanstalkServiceFactory {
  fn new(handle: &Handle) -> BeanstalkServiceFactory {
    let (tx, rx) = mpsc::unbounded();

    let mut id = 0;
    handle.spawn(rx.for_each(move |v: ServiceMessage| {
      match v {
        ServiceMessage::Put(tx, data) => {
          tx.send(id);
          id += 1;
          print!("recvd data: {:?}", data.len());
        }
      }
      Ok(())
    }));

    BeanstalkServiceFactory { tx: tx }
  }
}

impl NewService for BeanstalkServiceFactory {
  type Request = BeanstalkRequest;
  type Response = BeanstalkReply;
  type Error = io::Error;
  type Instance = BeanstalkService;

  fn new_service(&self) -> io::Result<Self::Instance> {
    Ok(BeanstalkService { tx: self.tx.clone() })
  }
}

pub fn serve(addr: SocketAddr) {
  let mut server = TcpServer::new(BeanstalkProtocol::new(), addr);
  server.threads(10);
  server.with_handle(BeanstalkServiceFactory::new);
}
