// https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
// https://tokio.rs/docs/getting-started/simple-server/use std::io;
use std::net::SocketAddr;
use std::io;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};

use tokio_core::io::{Codec, EasyBuf, Io, Framed};
use tokio_proto::pipeline::ServerProto;
use tokio_proto::TcpServer;
use tokio_service::Service;
use futures::{future, Future, BoxFuture, Poll, StartSend};
use futures::stream::Stream;
use futures::sink::Sink;

use combine::primitives::{State, Parser};

use command::*;
use reply::*;
use log_service::Log;

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

// TODO notify the application that we need to end this connection
struct WrappedFrameTransport<T> {
  client_id: usize,
  framed: Framed<T, BeanstalkCodec>,
}

impl<T> WrappedFrameTransport<T> {
  fn new(client_id: usize, framed: Framed<T, BeanstalkCodec>) -> WrappedFrameTransport<T> {
    WrappedFrameTransport {
      client_id: client_id,
      framed: framed,
    }
  }
}

impl<T: Io + 'static> Stream for WrappedFrameTransport<T> {
  type Item = (usize, BeanstalkCommand);
  type Error = io::Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    let poll_result = self.framed.poll();

    poll_result.map(|async_option| {
      async_option.map(|async| {
        async.map(|command| (self.client_id, command))
      })
    })
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
struct BeanstalkProtocol {
  mutex: Mutex<usize>,
}

unsafe impl Sync for BeanstalkProtocol {}

impl BeanstalkProtocol {
  fn new() -> BeanstalkProtocol {
    let mutex = Mutex::new(0);
    BeanstalkProtocol { mutex: mutex }
  }
}

impl<T: Io + 'static> ServerProto<T> for BeanstalkProtocol {
  // request matches codec in type
  type Request = (usize, BeanstalkCommand);

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

// Service
struct BeanstalkService {
  default_tube: Tube<(u32, Vec<u8>)>,
  id: Mutex<u32>,
}

impl BeanstalkService {
  fn new() -> BeanstalkService {
    BeanstalkService {
      default_tube: Tube::new(),
      id: Mutex::new(0),
    }
  }
}

impl Service for BeanstalkService {
  // must match protocol
  type Request = (usize, BeanstalkCommand);
  type Response = BeanstalkReply;

  // non streaming protocols, service errors are always io::Error
  type Error = io::Error;

  // future for computing the response
  type Future = BoxFuture<Self::Response, Self::Error>;

  fn call(&self, req: Self::Request) -> Self::Future {
    // TODO use futures::sync::mpsc::unbounded to send to an application

    match req.1 {
      BeanstalkCommand::Put(_priority, _delay, _ttr, _bytes, data) => {
        let mut mutex = self.id.lock().unwrap();
        *mutex += 1;
        self.default_tube.insert((*mutex, data));
        future::ok(BeanstalkReply::Inserted(*mutex)).boxed()
      }
      BeanstalkCommand::Reserve(None) => {
        let (id, data) = self.default_tube.reserve();
        future::ok(BeanstalkReply::Reserved(id, data)).boxed()
      }
      BeanstalkCommand::Unknown => {
        future::ok(BeanstalkReply::Error(BeanstalkError::UnknownCommand)).boxed()
      }
      _c => future::ok(BeanstalkReply::Ok(Vec::new())).boxed(),
    }
  }
}

struct Tube<T> {
  queue: Mutex<VecDeque<T>>,
  cvar: Condvar,
}

impl<T> Tube<T> {
  fn new() -> Tube<T> {
    let queue = Mutex::new(VecDeque::new());
    let cvar = Condvar::new();
    Tube {
      queue: queue,
      cvar: cvar,
    }
  }

  fn insert(&self, t: T) {
    let mut queue = self.queue.lock().unwrap();
    queue.push_front(t);
    self.cvar.notify_one();
  }

  fn reserve(&self) -> T {
    let mut queue = self.queue.lock().unwrap();
    while queue.is_empty() {
      queue = self.cvar.wait(queue).unwrap();
    }
    queue.pop_back().unwrap()
  }
}

pub fn serve(addr: SocketAddr) {
  let server = TcpServer::new(BeanstalkProtocol::new(), addr);
  server.serve(|| Ok(Log::new(BeanstalkService::new())));
}
