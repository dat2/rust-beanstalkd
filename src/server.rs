// https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
// https://tokio.rs/docs/getting-started/simple-server/use std::io;
use std::net::SocketAddr;
use std::io;

use tokio_core::io::{Codec, EasyBuf, Io, Framed};
use tokio_proto::pipeline::ServerProto;
use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};
use futures::{future, Future, BoxFuture, Poll, Async, StartSend};
use futures::stream::Stream;
use futures::sink::Sink;

use combine::primitives::{State, Parser};

use command::*;
use reply::*;

// Codec
#[derive(Default)]
struct BeanstalkCodec;

impl Codec for BeanstalkCodec {
  type In = BeanstalkCommand;
  type Out = BeanstalkReply;

  fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Self::In>> {
    // check if there's at least one \r\n
    let clone = buf.clone();
    let slice = clone.as_slice();

    if let Some(index) = slice.iter().position(|&b| b == b'\r') {
      // calculate how many lines to drain
      let lines = if slice[0] == b'p' && slice[1] == b'u' && slice[2] == b't' {
        1
      } else {
        0
      };

      let mut drain_index = index + 2;
      let mut has_enough_lines = true;
      for _ in 0..lines {
        if let Some(j) = slice.iter().skip(drain_index).position(|&b| b == b'\r') {
          drain_index += j + 2;
        } else {
          has_enough_lines = false;
          break;
        }
      }

      if has_enough_lines {
        let drained_buffer = buf.drain_to(drain_index);
        match BeanstalkCommandParser::command().parse(State::new(drained_buffer.as_slice())) {
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

  fn encode(&mut self, msg: BeanstalkReply, buf: &mut Vec<u8>) -> io::Result<()> {
    let bytes: Vec<u8> = msg.into();
    buf.extend_from_slice(&bytes);
    Ok(())
  }
}

struct WrappedFrameTransport<T> {
  framed: Framed<T, BeanstalkCodec>
}

impl<T: Io + 'static> Stream for WrappedFrameTransport<T> {
  type Item = BeanstalkCommand;
  type Error = io::Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    let poll_result = self.framed.poll();
    if let Ok(Async::Ready(Some(BeanstalkCommand::Quit))) = poll_result {
      Ok(Async::Ready(None))
    } else {
      poll_result
    }
  }
}

impl<T: Io + 'static> Sink for WrappedFrameTransport<T> {
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
#[derive(Default)]
struct BeanstalkProtocol;

impl<T: Io + 'static> ServerProto<T> for BeanstalkProtocol {
  // request matches codec in type
  type Request = BeanstalkCommand;

  // response matches codec out type
  type Response = BeanstalkReply;

  // hook in the codec
  type Transport = WrappedFrameTransport<T>;
  type BindTransport = Result<Self::Transport, io::Error>;

  fn bind_transport(&self, io: T) -> Self::BindTransport {
    Ok(WrappedFrameTransport { framed: io.framed(BeanstalkCodec) })
  }
}

// Service
struct BeanstalkService {
  application: Box<BeanstalkApplication>,
}

impl Service for BeanstalkService {
  // must match protocol
  type Request = BeanstalkCommand;
  type Response = BeanstalkReply;

  // non streaming protocols, service errors are always io::Error
  type Error = io::Error;

  // future for computing the response
  type Future = BoxFuture<Self::Response, Self::Error>;

  fn call(&self, req: Self::Request) -> Self::Future {
    println!("{:?}", req);

    // TODO use futures::sync::mpsc::unbounded to send to an application

    match req {
      BeanstalkCommand::Unknown => {
        future::ok(BeanstalkReply::Error(BeanstalkError::UnknownCommand)).boxed()
      }
      c => future::ok(BeanstalkReply::Ok(c.to_string().into_bytes())).boxed(),
    }
  }
}

// Application
#[derive(Clone)]
struct BeanstalkApplication;

impl BeanstalkApplication {
  fn new() -> BeanstalkApplication {
    BeanstalkApplication
  }
}

impl NewService for BeanstalkApplication {
  type Request = BeanstalkCommand;
  type Response = BeanstalkReply;
  type Error = io::Error;
  type Instance = BeanstalkService;

  fn new_service(&self) -> io::Result<Self::Instance> {
    Ok(BeanstalkService { application: Box::new(self.clone()) })
  }
}

pub fn serve(addr: SocketAddr) {
  let application = BeanstalkApplication::new();
  let server = TcpServer::new(BeanstalkProtocol, addr);
  server.serve(application);
}
