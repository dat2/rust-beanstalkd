#![recursion_limit = "1024"]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate combine;

mod command;
mod reply;
mod service;

// https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
// https://tokio.rs/docs/getting-started/simple-server/

use tokio_proto::TcpServer;
use service::{BeanstalkApplication, BeanstalkProtocol};
use std::net::SocketAddr;

pub fn serve(addr: SocketAddr) {
  let application = BeanstalkApplication::new();
  let server = TcpServer::new(BeanstalkProtocol, addr);
  server.serve(application);
}
