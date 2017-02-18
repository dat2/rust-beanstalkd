use std::fmt;
use std::convert::From;

// beanstalkd error messages
pub enum BeanstalkError {
  OutOfMemory,
  InternalError,
  BadFormat,
  UnknownCommand,
  ExpectedCrlf,
  JobTooBig,
  Draining,
}

impl fmt::Display for BeanstalkError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    use self::BeanstalkError::*;

    match *self {
      OutOfMemory => write!(f, "OUT_OF_MEMORY\r\n"),
      InternalError => write!(f, "INTERNAL_ERROR\r\n"),
      BadFormat => write!(f, "BAD_FORMAT\r\n"),
      UnknownCommand => write!(f, "UNKNOWN_COMMAND\r\n"),
      ExpectedCrlf => write!(f, "EXPECTED_CRLF\r\n"),
      JobTooBig => write!(f, "JOB_TOO_BIG\r\n"),
      Draining => write!(f, "DRAINING\r\n"),
    }
  }
}

impl From<BeanstalkError> for Vec<u8> {
  fn from(error: BeanstalkError) -> Vec<u8> {
    error.to_string().into_bytes()
  }
}

pub enum BeanstalkReply {
  Inserted(u32),
  Buried(Option<u32>),
  Using(String),
  Reserved(u32, Vec<u8>),
  DeadlineSoon,
  TimedOut,
  Deleted,
  NotFound,
  Released,
  Touched,
  Watching(u32),
  NotIgnored,
  Found(u32, Vec<u8>),
  Kicked(Option<u32>),
  Ok(Vec<u8>),
  Paused,
  Error(BeanstalkError),
}

impl From<BeanstalkReply> for Vec<u8> {
  fn from(reply: BeanstalkReply) -> Vec<u8> {
    use self::BeanstalkReply::*;

    match reply {
      Inserted(id) => format!("INSERTED {}\r\n", id).into_bytes(),
      Buried(id) => {
        format!("BURIED{}\r\n",
                if let Some(i) = id {
                  format!(" {}", i)
                } else {
                  String::new()
                })
          .into_bytes()
      }
      Using(tube) => format!("USING {}\r\n", tube).into_bytes(),
      Reserved(id, job) => {
        let mut bytes = format!("RESERVED {} {}\r\n", id, job.len()).into_bytes();
        bytes.extend_from_slice(&job);
        bytes.extend_from_slice(b"\r\n");
        bytes
      }
      Found(id, job) => {
        let mut bytes = format!("FOUND {} {}\r\n", id, job.len()).into_bytes();
        bytes.extend_from_slice(&job);
        bytes.extend_from_slice(b"\r\n");
        bytes
      }
      Kicked(bound) => {
        format!("KICKED{}\r\n",
                if let Some(b) = bound {
                  format!(" {}", b)
                } else {
                  String::new()
                })
          .into_bytes()
      }
      Ok(job) => {
        let mut bytes = format!("OK {}\r\n", job.len()).into_bytes();
        bytes.extend_from_slice(&job);
        bytes.extend_from_slice(b"\r\n");
        bytes
      }
      Error(e) => e.into(),
      reply => reply.to_string().into_bytes(),
    }
  }
}

impl fmt::Display for BeanstalkReply {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    use self::BeanstalkReply::*;

    match *self {
      Inserted(id) => write!(f, "INSERTED {}\r\n", id),
      Buried(id) => {
        write!(f,
               "BURIED{}\r\n",
               if let Some(i) = id {
                 format!(" {}", i)
               } else {
                 String::new()
               })
      }
      Using(ref tube) => write!(f, "USING {}\r\n", tube),
      Reserved(id, ref bytes) => write!(f, "RESERVED {} {}\r\n{}\r\n", id, bytes.len(), ""),
      DeadlineSoon => write!(f, "DEADLINE_SOON\r\n"),
      TimedOut => write!(f, "TIMED_OUT\r\n"),
      Deleted => write!(f, "DELETED\r\n"),
      NotFound => write!(f, "NOT_FOUND\r\n"),
      Released => write!(f, "RELEASED\r\n"),
      Touched => write!(f, "TOUCHED\r\n"),
      Watching(count) => write!(f, "WATCHING {}\r\n", count),
      NotIgnored => write!(f, "NOT_IGNORED\r\n"),
      Found(id, ref bytes) => write!(f, "FOUND {} {}\r\n{}\r\n", id, bytes.len(), ""),
      Kicked(bound) => {
        write!(f,
               "KICKED{}\r\n",
               if let Some(b) = bound {
                 format!(" {}", b)
               } else {
                 String::new()
               })
      }
      Ok(ref data) => write!(f, "OK {}\r\n{}\r\n", data.len(), ""),
      Paused => write!(f, "PAUSED\r\n"),
      Error(ref e) => write!(f, "{}", e),
    }
  }
}
