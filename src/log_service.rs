use tokio_service::Service;
use std::fmt;

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
        S::Response: fmt::Debug
{
  type Request = S::Request;
  type Response = S::Response;
  type Error = S::Error;
  type Future = S::Future;

  fn call(&self, request: Self::Request) -> Self::Future {
    info!(target: "beanstalkd", "[REQUEST] {:?}", request);
    self.upstream
      .call(request)
      // .then(|result| {
      //   match result {
      //     Ok(reply) => {
      //       info!(target: "beanstalkd", "[REPLY] {:?}", reply);
      //       Ok(reply)
      //     }
      //     e => e,
      //   }
      // })
  }
}
