use std::io;
use tokio_core::io::{Codec, EasyBuf, Io, Framed};
use tokio_proto::pipeline::ServerProto;
use tokio_service::{Service, NewService};
use futures::{future, Future, BoxFuture};

use combine::primitives::{State, Parser};

use command::*;
use reply::*;

// Codec
pub struct BeanstalkCodec;

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

// Protocol
pub struct BeanstalkProtocol;

impl<T: Io + 'static> ServerProto<T> for BeanstalkProtocol {
  // request matches codec in type
  type Request = BeanstalkCommand;

  // response matches codec out type
  type Response = BeanstalkReply;

  // hook in the codec
  type Transport = Framed<T, BeanstalkCodec>;
  type BindTransport = Result<Self::Transport, io::Error>;

  fn bind_transport(&self, io: T) -> Self::BindTransport {
    Ok(io.framed(BeanstalkCodec))
  }
}

// Service
pub struct BeanstalkService {
  application: Box<BeanstalkApplication>
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
      BeanstalkCommand::Unknown => future::ok(BeanstalkReply::Error(BeanstalkError::UnknownCommand)).boxed(),
      c => future::ok(BeanstalkReply::Ok(c.to_string().into_bytes())).boxed(),
    }
  }
}

// Application
#[derive(Clone)]
pub struct BeanstalkApplication;

impl BeanstalkApplication {
  pub fn new() -> BeanstalkApplication {
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
