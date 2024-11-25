/*
The bus client shall:
- be event driven, responding to tag updates and id responses
- have self-contained timers for reconnections
- connect to the bus
- if the connection is lost call TagManager::reset_ids()
- attempt to reconnect with RFC compliant reconnection logic
BusClient::publish() shall:
- receive tag
- if the tag id is 0
  - send id request message to the bus with the tag name
- if the tag id is not 0
  - send update message to the bus with id, value and time_us
BusClient::on_id_response() shall:
- receive id response message
- call TagManager::set_id() with the tag name and id

From the pymscada project documentation:
Protocol description and protocol constants.

Bus holds a tag forever, assigns a tag id forever, holds a tag value with len
< 1000 forever, otherwise wipes periodically. So assign tagnames once, at the
start of your program; use RTA as an update messenger, share whole structures
rarely.

- version 8-bit unsigned int == 0x01
- command 8-bit unsigned int
- tag_id 16-bit unsigned int     0 is not a valid tag_id
- size 16-bit unsigned int
  - if size == 0xff  continuation mandatory
  - size 0x00 completes an empty continuation
- time_us 64-bit unsigned int, UTC microseconds
- data size of 8-bit char

command
- CMD_ID data is tagname
  - reply: CMD_ID with tag_id and data as tagname
- CMD_SET id, data is typed or json packed
  - no reply
- CMD_UNSUB id
  - no reply
- CMD_GET id
- CMD_RTA id, data is request to author
- CMD_SUB id
  - reply: SET id and value, value may be None
- CMD_LIST
  - size == 0x00
    - tags with values newer than time_us
  - size > 0x00
    - ^text matches start of tagname
    - text$ matches start of tagname
    - text matches anywhere in tagname
  - reply: LIST data as space separated tagnames
- CMD_LOG data to logging.warning

# Tuning constants
MAX_LEN = 65535 - 14  # TODO fix server(?) when 3

# Network protocol commands
CMD_ID = 1  # query / inform tag ID - data is tagname bytes string
CMD_SET = 2  # set a tag
CMD_GET = 3  # get a tag
CMD_RTA = 4  # request to author
CMD_SUB = 5  # subscribe to a tag
CMD_UNSUB = 6  # unsubscribe from a tag
CMD_LIST = 7  # bus list tags
CMD_ERR = 8  # action failed
CMD_LOG = 9  # bus print a logging message

CMD_TEXT = {
    1: 'CMD_ID',
    2: 'CMD_SET',
    3: 'CMD_GET',
    4: 'CMD_RTA',
    5: 'CMD_SUB',
    6: 'CMD_UNSUB',
    7: 'CMD_LIST',
    8: 'CMD_ERR',
    9: 'CMD_LOG'
}

COMMANDS = [CMD_ID, CMD_SET, CMD_GET, CMD_RTA, CMD_SUB, CMD_UNSUB, CMD_LIST,
            CMD_ERR, CMD_LOG]

# data types
TYPE_INT = 1  # 64 bit signed integer
TYPE_FLOAT = 2  # 64 bit IEEE float
TYPE_STR = 3  # string
TYPE_BYTES = 4
TYPE_JSON = 5

TYPES = [TYPE_INT, TYPE_FLOAT, TYPE_STR, TYPE_BYTES, TYPE_JSON]
*/

use crate::tag::{TagManager, TagMessage};
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;
use async_trait::async_trait;

#[async_trait]
pub trait BusClient: Send + Sync {
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct PyMScadaBusClient {
    tag_manager: TagManager,
    host: String,
    port: u16,
    connected: bool,
    reconnect_attempts: u32,
    stream: Option<TcpStream>,
    receiver: mpsc::Receiver<TagMessage>,
    verbose: bool,
}

impl PyMScadaBusClient {
    pub fn new(tag_manager: TagManager, host: &str, port: u16, receiver: mpsc::Receiver<TagMessage>, verbose: bool) -> Self {
        PyMScadaBusClient {
            tag_manager,
            host: host.to_string(),
            port,
            connected: false,
            reconnect_attempts: 0,
            stream: None,
            receiver,
            verbose,
        }
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = format!("{}:{}", self.host, self.port);
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                println!("Connected to PyMScada bus at {}", addr);
                self.stream = Some(stream);
                self.connected = true;
                self.reconnect_attempts = 0;
                Ok(())
            }
            Err(e) => {
                self.handle_connection_loss().await;
                Err(Box::new(e))
            }
        }
    }

    async fn handle_connection_loss(&mut self) {
        self.connected = false;
        self.stream = None;
        
        // RFC compliant exponential backoff
        let delay = std::cmp::min(
            1 << self.reconnect_attempts, // exponential backoff
            60 // max delay of 60 seconds
        );
        self.reconnect_attempts += 1;
        
        println!("Connection lost. Waiting {} seconds before reconnecting...", delay);
        tokio::time::sleep(Duration::from_secs(delay as u64)).await;
    }

    async fn process_message(&mut self, msg: TagMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match msg {
            TagMessage::Update { name, value, time_us } => {
                if let Some(tag) = self.tag_manager.get_tag(&name).await {
                    if let Some(stream) = &mut self.stream {
                        if tag.id == 0 {
                            if self.verbose {
                                println!("Requesting ID for tag {}", name);
                            }
                            let packet = encode_id_request(&name);
                            print_protocol(self.verbose, "Sending", &packet);
                            stream.write_all(&packet).await?;
                        } else {
                            if self.verbose {
                                println!("Publishing tag {} with id {}, value {} at time {}", 
                                    name, tag.id, value, time_us);
                            }
                            let packet = encode_set_float(tag.id, value, time_us);
                            print_protocol(self.verbose, "Sending", &packet);
                            stream.write_all(&packet).await?;
                        }
                    }
                }
            },
            TagMessage::SetId { name, id } => {
                self.tag_manager.set_id(&name, id).await;
            },
            TagMessage::ResetIds => {
                self.tag_manager.reset_ids().await;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BusClient for PyMScadaBusClient {
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = vec![0u8; 1024];
        let mut read_buffer = Vec::new();

        loop {
            if !self.connected {
                if let Err(e) = self.connect().await {
                    println!("Connection attempt failed: {}", e);
                    continue;
                }
            }

            tokio::select! {
                result = async { 
                    if let Some(stream) = &mut self.stream {
                        stream.read(&mut buffer).await
                    } else {
                        Ok(0)
                    }
                } => {
                    match result {
                        Ok(0) => {
                            println!("Server closed the connection");
                            self.handle_connection_loss().await;
                        }
                        Ok(n) => {
                            // Append new data to read_buffer
                            read_buffer.extend_from_slice(&buffer[..n]);

                            // Process complete packets
                            while read_buffer.len() >= 14 {  // Update minimum packet size
                                let size = if read_buffer.len() >= 6 {
                                    u16::from_be_bytes([read_buffer[4], read_buffer[5]]) as usize
                                } else {
                                    continue;
                                };

                                let total_size = 14 + size;  // Update header size
                                if read_buffer.len() >= total_size {
                                    let packet = read_buffer.drain(..total_size).collect::<Vec<_>>();
                                    print_protocol(self.verbose, "Received", &packet);
                                    if let Ok((command, _tag_id, _, _data)) = protocol_decode(&packet) {
                                        if command == CMD_ID as u8 {  // Update command type
                                            if let Ok((id, tag_name)) = decode_id_response(&packet) {
                                                self.process_message(TagMessage::SetId {
                                                    name: tag_name,
                                                    id: id as u32,  // Convert u16 to u32 for internal use
                                                }).await?;
                                            }
                                        }
                                    }
                                } else {
                                    break;  // Wait for more data
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error reading from socket: {}", e);
                            self.handle_connection_loss().await;
                        }
                    }
                }

                // Handle incoming messages from TagManager
                Some(msg) = self.receiver.recv() => {
                    if let Err(e) = self.process_message(msg).await {
                        println!("Error processing message: {}", e);
                    }
                }
            }
        }
    }
} 

// Protocol constants
const PROTOCOL_VERSION: u8 = 0x01;

// Commands
const CMD_ID: u8 = 1;
const CMD_SET: u8 = 2;

// Data types
const TYPE_FLOAT: u8 = 2;

// Protocol structure
// - version: u8
// - command: u8  
// - tag_id: u16
// - size: u16
// - time_us: u64 (UTC microseconds)
// - data: [u8] (variable length)

pub fn protocol_encode(
    command: u8,
    tag_id: u16,
    time_us: u64,
    data: &[u8]
) -> Vec<u8> {
    let mut buffer = Vec::new();
    
    // Version (1 byte)
    buffer.push(PROTOCOL_VERSION);
    
    // Command (1 byte)
    buffer.push(command);
    
    // Tag ID (2 bytes)
    buffer.extend_from_slice(&tag_id.to_be_bytes());
    
    // Size (2 bytes)
    buffer.extend_from_slice(&(data.len() as u16).to_be_bytes());
    
    // Time (8 bytes)
    buffer.extend_from_slice(&time_us.to_be_bytes());
    
    // Data (variable length)
    buffer.extend_from_slice(data);
    
    buffer
}

pub fn protocol_decode(buffer: &[u8]) -> Result<(u8, u16, u64, Vec<u8>), &'static str> {
    if buffer.len() < 14 { // Minimum packet size: 1 + 1 + 2 + 2 + 8 = 14 bytes
        return Err("Buffer too small for protocol header");
    }
    
    let version = buffer[0];
    if version != PROTOCOL_VERSION {
        return Err("Invalid protocol version");
    }
    
    let command = buffer[1];
    let tag_id = u16::from_be_bytes([buffer[2], buffer[3]]);
    let size = u16::from_be_bytes([buffer[4], buffer[5]]);
    let time_us = u64::from_be_bytes([
        buffer[6], buffer[7], buffer[8], buffer[9],
        buffer[10], buffer[11], buffer[12], buffer[13],
    ]);
    
    if buffer.len() < 14 + size as usize {
        return Err("Buffer too small for specified data size");
    }
    
    let data = buffer[14..14 + size as usize].to_vec();
    
    Ok((command, tag_id, time_us, data))
}

// Helper functions for specific packet types
pub fn encode_id_request(tag_name: &str) -> Vec<u8> {
    protocol_encode(
        CMD_ID,
        0, // tag_id is 0 for requests
        0, // time_us not used for ID requests
        tag_name.as_bytes()
    )
}

pub fn encode_set_float(tag_id: u32, value: f64, time_us: i64) -> Vec<u8> {
    let mut data = Vec::new();
    data.push(TYPE_FLOAT);
    data.extend_from_slice(&value.to_be_bytes());
    
    protocol_encode(
        CMD_SET,
        tag_id.try_into().unwrap(),
        time_us.try_into().unwrap(),
        &data
    )
}

pub fn decode_id_response(buffer: &[u8]) -> Result<(u32, String), &'static str> {
    let (command, tag_id, _, data) = protocol_decode(buffer)?;
    
    if command != CMD_ID {
        return Err("Not an ID response packet");
    }
    
    if tag_id == 0 {
        return Err("Invalid tag ID in response");
    }
    
    match String::from_utf8(data) {
        Ok(tag_name) => Ok((tag_id.into(), tag_name)),
        Err(_) => Err("Invalid UTF-8 in tag name")
    }
} 

// Change print_protocol to be a standalone function instead of a method
fn print_protocol(verbose: bool, direction: &str, buffer: &[u8]) {
    if !verbose {
        return;
    }
    
    println!("{} Protocol Packet:", direction);
    
    if buffer.len() < 14 {
        println!("  Warning: Packet too short ({} bytes)", buffer.len());
        if buffer.len() >= 1 {
            println!("  Version: 0x{:02x}", buffer[0]);
        }
        if buffer.len() >= 2 {
            let command = buffer[1];
            let cmd_text = match command {
                1 => "CMD_ID",
                2 => "CMD_SET",
                3 => "CMD_GET",
                4 => "CMD_RTA",
                5 => "CMD_SUB",
                6 => "CMD_UNSUB",
                7 => "CMD_LIST",
                8 => "CMD_ERR",
                9 => "CMD_LOG",
                _ => "UNKNOWN",
            };
            println!("  Command: {} ({})", cmd_text, command);
        }
        println!("  Raw: {:?}", buffer);
        return;
    }

    let version = buffer[0];
    let command = buffer[1];
    let tag_id = u16::from_be_bytes([buffer[2], buffer[3]]);
    let size = u16::from_be_bytes([buffer[4], buffer[5]]) as usize;
    let time_us = u64::from_be_bytes([
        buffer[6], buffer[7], buffer[8], buffer[9],
        buffer[10], buffer[11], buffer[12], buffer[13],
    ]);

    let cmd_text = match command {
        1 => "CMD_ID",
        2 => "CMD_SET",
        3 => "CMD_GET",
        4 => "CMD_RTA",
        5 => "CMD_SUB",
        6 => "CMD_UNSUB",
        7 => "CMD_LIST",
        8 => "CMD_ERR",
        9 => "CMD_LOG",
        _ => "UNKNOWN",
    };

    println!("  Version: 0x{:02x}", version);
    println!("  Command: {} ({})", cmd_text, command);
    println!("  Tag ID: {}", tag_id);
    println!("  Size: {}", size);
    println!("  Time: {}", time_us);
    
    if buffer.len() >= 14 + size {
        let data = &buffer[14..14 + size];
        match std::str::from_utf8(data) {
            Ok(text) => println!("  Data: \"{}\" {:?}", text, data),
            Err(_) => println!("  Data: {:?}", data),
        }
    } else {
        println!("  Data: <truncated>");
    }
    println!("  Raw: {:?}", buffer);
} 