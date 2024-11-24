// The main program shall:
// - process command line arguments
// - create the tag manager
// - create the bus client
// - create the collector
// - spawn the collector and bus client tasks
// - keep the main task running
// Tasks shall be exception driven, i.e. not periodic
// Events shall be generated within the tasks.
// Command line arguments shall be:
// - --bus_ip <ip address>, default 127.0.0.1
// - --bus_port <port>, default 1324
// - --deadband <float>, default 0.05
// - --ct-ratio <float>, default 60.0
// - --sps <integer>, default 400
// - --i2c-bus <integer>, default 1
// - --i2c-address <integer>, default 0x47
// - --tag_root <string>, default "i2c_amps_"
// - --continuous, default false
// - --verbose, default false
// - --log_file <string>, default "i2c_amps.log"
// - --help, print help and exit

mod bus_client;
mod collector;
mod tag;

use bus_client::{BusClient, PyMScadaBusClient};
use collector::{Collector, DataCollector};
use tag::TagManager;
use clap::Parser;

#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bus_ip: String,

    #[arg(long, default_value_t = 1324)]
    bus_port: u16,

    #[arg(long, default_value_t = 0.05)]
    deadband: f64,

    #[arg(long, default_value_t = 60.0)]
    ct_ratio: f64,

    #[arg(long, default_value_t = 400)]
    sps: u32,

    #[arg(long, default_value_t = 1)]
    i2c_bus: u8,

    #[arg(long, default_value = "0x47")]
    i2c_address: String,

    #[arg(long, default_value = "i2c_amps_")]
    tag_root: String,

    #[arg(long)]
    continuous: bool,

    #[arg(long)]
    verbose: bool,
    
    #[arg(long, default_value = "i2c_amps.log")]
    log_file: String,
}

#[derive(Clone)]
struct App {
    tag_manager: TagManager,
    args: Args,
}

impl App {
    fn new(tag_manager: TagManager, args: Args) -> Self {
        App {
            tag_manager,
            args,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // Create tag manager with channel
    let (tag_manager, receiver) = TagManager::new(args.deadband);
    
    // Create app with tag manager
    let app = App::new(tag_manager.clone(), args.clone());
    
    // Initialize components
    let collector = DataCollector::new(app.tag_manager.clone());
    let mut bus_client = PyMScadaBusClient::new(
        app.tag_manager.clone(),
        &app.args.bus_ip,
        app.args.bus_port,
        receiver,
        app.args.verbose,
    );
    
    // Spawn collector task
    tokio::spawn(async move {
        collector.run().await
    });

    // Run bus client in the main task
    bus_client.run().await?;

    Ok(())
} 