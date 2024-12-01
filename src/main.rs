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
// mod collector;
mod tag;
mod collector_ads1115;
mod processor;
mod rotary_buffer;

use bus_client::{BusClient, PyMScadaBusClient};
// use collector::{Collector, DataCollector};
use collector_ads1115::{ADS1115Collector};
use tag::TagManager;
use clap::Parser;
use std::sync::Arc;
use processor::Processor;
use rotary_buffer::RotaryBuffer;

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

    #[arg(long, default_value_t = 860)]
    sps: u32,

    #[arg(long, default_value_t = 1)]
    i2c_bus: u8,

    #[arg(long, default_value = "0x48")]
    i2c_address: String,

    #[arg(long, default_value = "i2c_amps")]
    tag_prefix: String,

    #[arg(long)]
    continuous: bool,

    #[arg(long)]
    verbose: bool,
    
    #[arg(long, default_value = "i2c_amps.log")]
    log_file: String,

    #[arg(long)]
    fft: bool,

    #[arg(long, default_value_t = 120)]
    batch_size: usize,
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
    let (tag_manager, receiver) = TagManager::new(args.deadband);
    let app = App::new(tag_manager.clone(), args.clone());
    
    let buffer = Arc::new(RotaryBuffer::new(10000));
    
    let collector = ADS1115Collector::new(
        buffer.clone(),
        app.args.i2c_bus,
        u8::from_str_radix(&app.args.i2c_address.trim_start_matches("0x"), 16)?,
        app.args.sps,
        app.args.verbose,
    );

    let processor = Processor::new(
        app.args.tag_prefix.clone(),
        app.tag_manager.clone(),
        buffer.clone(),
        app.args.batch_size,
        app.args.ct_ratio,
        app.args.verbose,
        app.args.fft,
    );
    
    let mut bus_client = PyMScadaBusClient::new(
        app.tag_manager.clone(),
        &app.args.bus_ip,
        app.args.bus_port,
        receiver,
        app.args.verbose,
    );
    
    tokio::spawn(async move {
        collector.run().await
    });

    tokio::spawn(async move {
        processor.run().await
    });

    bus_client.run().await?;
    Ok(())
}

// -------------------------------------------------
// Tests
// -------------------------------------------------
