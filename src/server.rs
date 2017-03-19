// https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
// https://tokio.rs/docs/getting-started/simple-server/use std::io;
use std::net::SocketAddr;
use std::io;
use std::sync::Mutex;
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread;
use std::time::*;

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Encoder, Decoder, Framed};
use tokio_timer::*;
use bytes::BytesMut;
use tokio_proto::pipeline::ServerProto;
use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};
use futures::{future, Future, BoxFuture, Poll, StartSend, Stream, Sink, Async};
use futures::task;

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

struct SendToken {
  id: usize,
}

impl Future for SendToken {
  type Item = usize;
  type Error = io::Error;

  fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
    Ok(Async::Ready(self.id))
  }
}

struct ReserveToken {
  rx: Receiver<(usize, Vec<u8>)>,
  item: Result<(usize, Vec<u8>), TryRecvError>,
}

impl ReserveToken {
  fn new(rx: Receiver<(usize, Vec<u8>)>) -> ReserveToken {
    let item = rx.try_recv();
    ReserveToken { rx: rx, item: item }
  }

  fn schedule_unpark(&mut self) {
    let task = task::park();
    thread::spawn(move || {
      thread::sleep(Duration::from_millis(500));
      task.unpark();
    }).join().unwrap();
    self.item = self.rx.try_recv();
  }
}

impl Future for ReserveToken {
  type Item = (usize, Vec<u8>);
  type Error = io::Error;

  fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
    match self.item {
      Err(TryRecvError::Disconnected) => Err(io::Error::new(io::ErrorKind::Other, "disconnected")),
      Err(TryRecvError::Empty) => {
        self.schedule_unpark();
        Ok(Async::NotReady)
      },
      Ok(ref item) => Ok(Async::Ready(item.clone()))
    }
  }
}

// Service
struct BeanstalkService {
  item_tx: Sender<(usize, Vec<u8>)>,
  reserve_tx: Sender<Sender<(usize, Vec<u8>)>>,
  id: Mutex<usize>,
}

impl BeanstalkService {
  fn new(item_tx: Sender<(usize, Vec<u8>)>, reserve_tx: Sender<Sender<(usize, Vec<u8>)>>) -> BeanstalkService {
    BeanstalkService {
      item_tx: item_tx,
      reserve_tx: reserve_tx,
      id: Mutex::new(0),
    }
  }

  fn get_id(&self) -> usize {
    let mut mutex = self.id.lock().unwrap();
    *mutex += 1;
    *mutex
  }

  fn get_reserve_token(&self) -> ReserveToken {
    // TODO report error in Future for ReserveToken
    let (tx, rx) = channel();
    self.reserve_tx.send(tx).unwrap();
    ReserveToken::new(rx)
  }

  fn get_send_token(&self, item: (usize, Vec<u8>)) -> SendToken {
    // TODO report error in Future for SendToken
    let send_token = SendToken { id: item.0 };
    self.item_tx.send(item).unwrap();
    send_token
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
    match req.1 {
      BeanstalkCommand::Put(_priority, _delay, _ttr, _bytes, data) => {
        let id = self.get_id();
        self.get_send_token((id, data))
          .map(|id| BeanstalkReply::Inserted(Box::new(id)))
          .boxed()
      }
      BeanstalkCommand::Reserve(timeout) => {
        match timeout {
          Some(seconds) => {
            let timer = Timer::default();
            // TODO timeout error kills the connection
            timer.timeout(self.get_reserve_token().map(|(id, bytes)| BeanstalkReply::Reserved(id, bytes)),
                          Duration::new(seconds as u64, 0))
                  .boxed()
          }
          None => {
            self.get_reserve_token()
              .map(|(id, bytes)| BeanstalkReply::Reserved(id, bytes))
              .boxed()
          }
        }
      }
      BeanstalkCommand::Unknown => {
        future::ok(BeanstalkReply::Error(BeanstalkError::UnknownCommand)).boxed()
      }
      _c => future::ok(BeanstalkReply::Ok(Vec::new())).boxed(),
    }
  }
}

struct BeanstalkApplication {
  item_tx: Sender<(usize, Vec<u8>)>,
  reserve_tx: Sender<Sender<(usize, Vec<u8>)>>,
}

unsafe impl Send for BeanstalkApplication { }
unsafe impl Sync for BeanstalkApplication { }

impl BeanstalkApplication {
  fn new() -> BeanstalkApplication {
    let (item_tx, item_rx) = channel();
    let (reserve_tx, reserve_rx) = channel();

    thread::spawn(move || {
      loop {
        let tx: Sender<(usize, Vec<u8>)> = reserve_rx.recv().unwrap();
        let item = item_rx.recv().unwrap();
        tx.send(item).unwrap();
      }
    });

    BeanstalkApplication { item_tx: item_tx, reserve_tx: reserve_tx }
  }
}

impl NewService for BeanstalkApplication {
  type Request = (usize, BeanstalkCommand);
  type Response = BeanstalkReply;
  type Error = io::Error;
  type Instance = Log<BeanstalkService>;

  fn new_service(&self) -> io::Result<Self::Instance> {
    Ok(Log::new(BeanstalkService::new(self.item_tx.clone(), self.reserve_tx.clone())))
  }
}

pub fn serve(addr: SocketAddr) {
  let mut server = TcpServer::new(BeanstalkProtocol::new(), addr);
  server.threads(10);
  server.serve(BeanstalkApplication::new());
}
