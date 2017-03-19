use tokio_service::Service;
use std::fmt;
use std::thread;
use std::error::Error;
use futures::{Future, Poll, Async};

pub struct LogOnComplete<F> {
  inner: F,
}

impl<F: Future> Future for LogOnComplete<F>
  where F::Item: fmt::Debug,
        F::Error: Error
{
  type Item = F::Item;
  type Error = F::Error;

  fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
    let result = self.inner.poll();

    let handle = thread::current();
    if let Ok(Async::Ready(ref reply)) = result {
      info!(target: "beanstalkd", "[REPLY] {:?} {:?}", handle.name(), reply);
    } else if let Err(ref e) = result {
      error!(target: "beanstalkd", "[REPLY] {:?} {:?}", handle.name(), e.description());
    }

    result
  }
}

#[derive(Clone)]
pub struct Log<S> {
  upstream: S,
}

impl<S> Log<S> {
  pub fn new(upstream: S) -> Log<S> {
    Log { upstream: upstream }
  }
}

impl<S> Service for Log<S>
  where S: Service,
        S::Request: fmt::Debug,
        S::Response: fmt::Debug,
        S::Error: Error
{
  type Request = S::Request;
  type Response = S::Response;
  type Error = S::Error;
  type Future = LogOnComplete<S::Future>;

  fn call(&self, request: Self::Request) -> Self::Future {
    let handle = thread::current();
    info!(target: "beanstalkd", "[REQUEST] {:?} {:?}", handle.name(), request);

    LogOnComplete { inner: self.upstream.call(request) }
  }
}
