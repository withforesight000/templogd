use clap::{Parser, ValueEnum};
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

mod config;
mod controller;
mod infra;
mod usecase;
mod validator;

/// tempgrpcd
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct TempgrpcdArgs {
    /// Server bind address
    #[arg(long, required = true, env = "TEMPGRPCD_SERVER_BIND_ADDRESS")]
    server_bind_address: String,

    /// Server port
    #[arg(long, required = true, env = "TEMPGRPCD_SERVER_PORT")]
    server_port: String,

    /// API bearer token
    #[arg(long, required = true, env = "TEMPGRPCD_BEARER_TOKEN")]
    bearer_token: String,

    /// Redis host
    #[arg(long, required = true, env = "TEMPGRPCD_REDIS_HOST")]
    redis_host: String,

    /// Redis port
    #[arg(long, default_value_t = 6379, env = "TEMPGRPCD_REDIS_PORT")]
    redis_port: i32,

    /// Log output format: json or text.
    #[arg(long, value_enum, default_value_t = LogFormat::Json, env = "TEMPGRPCD_LOG_FORMAT")]
    log_format: LogFormat,

    /// Maximum log level to emit.
    #[arg(long, value_enum, default_value_t = LogLevel::Info, env = "TEMPGRPCD_LOG_LEVEL")]
    log_level: LogLevel,
}

/// Selects the serialization format used for log records.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum LogFormat {
    Json,
    Text,
}

/// Selects the maximum severity emitted by the tracing subscriber.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl LogLevel {
    /// Converts the command-line value to tracing's level filter.
    fn as_filter(self) -> tracing::level_filters::LevelFilter {
        match self {
            Self::Trace => tracing::level_filters::LevelFilter::TRACE,
            Self::Debug => tracing::level_filters::LevelFilter::DEBUG,
            Self::Info => tracing::level_filters::LevelFilter::INFO,
            Self::Warn => tracing::level_filters::LevelFilter::WARN,
            Self::Error => tracing::level_filters::LevelFilter::ERROR,
            Self::Off => tracing::level_filters::LevelFilter::OFF,
        }
    }
}

/// Initializes the subscriber with the requested format and maximum level.
fn init_logging(format: LogFormat, level: LogLevel) {
    let level = level.as_filter();
    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            // Record span creation and close events as operation start/end logs.
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            // Keep the configured maximum level as the incident response control.
            .with_max_level(level)
            .init(),
        LogFormat::Text => tracing_subscriber::fmt()
            // Record span creation and close events as operation start/end logs.
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            // Keep the configured maximum level as the incident response control.
            .with_max_level(level)
            .init(),
    }
}

#[tokio::main]
async fn main() {
    let args = TempgrpcdArgs::parse();
    init_logging(args.log_format, args.log_level);
    info!("starting tempgrpcd...");

    let config = config::new(args);
    info!("config loaded");

    infra::server::run(config).await;
    tracing::info!("exiting...");
}

#[cfg(test)]
mod tests {
    use super::{LogFormat, LogLevel, TempgrpcdArgs};
    use clap::Parser;

    #[test]
    fn parses_default_logging_arguments() {
        let args = TempgrpcdArgs::try_parse_from([
            "tempgrpcd",
            "--server-bind-address",
            "127.0.0.1",
            "--server-port",
            "50051",
            "--bearer-token",
            "token",
            "--redis-host",
            "127.0.0.1",
        ])
        .unwrap();

        assert_eq!(args.log_format, LogFormat::Json);
        assert_eq!(args.log_level, LogLevel::Info);
    }

    #[test]
    fn parses_custom_logging_arguments() {
        let args = TempgrpcdArgs::try_parse_from([
            "tempgrpcd",
            "--server-bind-address",
            "127.0.0.1",
            "--server-port",
            "50051",
            "--bearer-token",
            "token",
            "--redis-host",
            "127.0.0.1",
            "--log-format",
            "text",
            "--log-level",
            "debug",
        ])
        .unwrap();

        assert_eq!(args.log_format, LogFormat::Text);
        assert_eq!(args.log_level, LogLevel::Debug);
    }
}
