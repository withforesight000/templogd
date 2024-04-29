use std::{io, os::fd::AsRawFd};

use chrono::Local;
use nix::unistd::isatty;
use syslog::{Formatter3164, LoggerBackend};

pub trait Logger {
    fn info(&mut self, message: &str);
    fn error(&mut self, message: &str);
}

pub enum LoggerType {
    AUTO,
    STDOUT,
    SYSLOG
}

fn has_controlling_terminal() -> bool {
    isatty(io::stdout().as_raw_fd()).unwrap_or(false)
}

pub fn new(logger_type: LoggerType) -> Box<dyn Logger> {
    match logger_type {
        LoggerType::AUTO => {
            if has_controlling_terminal() {
                Box::new(StdOutLogger::new()) as Box<dyn Logger>
            } else {
                Box::new(SyslogLogger::new()) as Box<dyn Logger>
            }
        },
        LoggerType::STDOUT => Box::new(StdOutLogger::new()) as Box<dyn Logger>,
        LoggerType::SYSLOG => Box::new(SyslogLogger::new()) as Box<dyn Logger>,
    }
}

pub struct SyslogLogger {
    writer: syslog::Logger<LoggerBackend, Formatter3164>,
}

impl SyslogLogger {
    fn new() -> SyslogLogger {
        let formatter = Formatter3164 {
            hostname: None, // workaround fix for Logger format
            ..Default::default()
        };

        let writer = syslog::unix(formatter).expect("could not connect to syslog");
        SyslogLogger { writer }
    }
}

impl Logger for SyslogLogger {
    fn info(&mut self, message: &str) {
        self.writer
            .info(message)
            .expect("could not write to syslog");
    }

    fn error(&mut self, message: &str) {
        self.writer.err(message).expect("could not write to syslog");
    }
}

pub struct StdOutLogger {}

impl StdOutLogger {
    fn new() -> StdOutLogger {
        StdOutLogger {}
    }

    fn localtime() -> String {
        let now = Local::now();
        return now.format("%Y-%m-%d %H:%M:%S%.3f %:z").to_string();
    }
}

impl Logger for StdOutLogger {
    fn info(&mut self, message: &str) {
        println!("{}: info: {}", StdOutLogger::localtime(), message);
    }

    fn error(&mut self, message: &str) {
        eprintln!("{}: error: {}", StdOutLogger::localtime(), message);
    }
}
