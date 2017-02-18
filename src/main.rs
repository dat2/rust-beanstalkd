extern crate rust_beanstalkd;
extern crate futures;
extern crate clap;

use clap::{Arg, App};

use rust_beanstalkd::server;

fn port_validator(v: String) -> Result<(), String> {
  v.parse()
    .map(|_: u16| ())
    .map_err(|_| format!("{} is an invalid port.", v))
}

fn main() {
  let matches = App::new("rust-beanstalkd")
    .version("0.2.0")
    .author("Nicholas Dujay <nickdujay@gmail.com>")
    .about("A pure rust implementation of beanstalkd. See http://kr.github.io/beanstalkd/.")
    .arg(Arg::with_name("port")
      .short("p")
      .long("port")
      .value_name("PORT")
      .takes_value(true)
      .validator(port_validator))
    .get_matches();

  let port = matches.value_of("port").unwrap_or("11300");
  let addr = format!("127.0.0.1:{}", port).parse().unwrap();

  server::serve(addr);
}
