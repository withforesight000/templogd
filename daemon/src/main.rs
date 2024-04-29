use common::logger;

// static PROCESS_NAME: &str = "templogd";

fn main() {
    // TODO: Add command line argument parsing
    // TODO: consoder how to initialize logger
    let mut logger = logger::new(logger::LoggerType::STDOUT);

    logger.info("Hello, world!");
}
