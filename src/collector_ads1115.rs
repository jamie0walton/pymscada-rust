/*
Collector for ADS1115.

This shall do all configuration of the ADS1115 and collection of data.
*/

use crate::rotary_buffer::{RotaryBuffer, Reading};
use async_trait::async_trait;
use linux_embedded_hal::I2cdev;
use embedded_hal::blocking::i2c::{Write, WriteRead};
use rppal::gpio::{Gpio, Trigger};
use std::sync::Arc;
use tokio::time::{self, Duration};
use std::sync::atomic::{AtomicBool, Ordering};

#[async_trait]
pub trait Collector {
    async fn configure(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn collect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct ADS1115Collector {
    buffer: Arc<RotaryBuffer>,
    i2c_bus: u8,
    i2c_address: u8,
    sps: u32,
    verbose: bool,
    running: Arc<AtomicBool>,
}

fn configure_ads1115(
    i2c: &mut I2cdev,
    address: u8,
    os: Option<bool>,
    mux: Option<u8>,
    pga: Option<u8>,
    mode: Option<bool>,
    data_rate: Option<u8>,
    comp_mode: Option<bool>,
    comp_pol: Option<bool>,
    comp_lat: Option<bool>,
    comp_que: Option<u8>,
    verbose: bool,
) -> bool {
    let config = 
        ((os.unwrap_or(false) as u16) << 15) |
        ((mux.unwrap_or(0) & 0x7) as u16) << 12 |
        ((pga.unwrap_or(2) & 0x7) as u16) << 9 |
        ((mode.unwrap_or(true) as u16) << 8) |
        ((data_rate.unwrap_or(4) & 0x7) as u16) << 5 |
        ((comp_mode.unwrap_or(false) as u16) << 4) |
        ((comp_pol.unwrap_or(false) as u16) << 3) |
        ((comp_lat.unwrap_or(false) as u16) << 2) |
        ((comp_que.unwrap_or(3) & 0x3) as u16);

    if verbose {
        // Print human readable configuration
        println!("Setting ADS1115 Configuration:");
        
        // Operational status
        let os_bit = (config >> 15) & 0x1;
        println!("Operational status: {}", if os_bit == 1 {"Start conversion"} else {"No effect"});
        
        // Input multiplexer
        let mux_val = (config >> 12) & 0x7;
        let mux_setting = match mux_val {
            0 => "AIN0 - AIN1 (default)",
            1 => "AIN0 - AIN3",
            2 => "AIN1 - AIN3",
            3 => "AIN2 - AIN3",
            4 => "AIN0 - GND",
            5 => "AIN1 - GND",
            6 => "AIN2 - GND",
            7 => "AIN3 - GND",
            _ => "Unknown"
        };
        println!("Input multiplexer: {}", mux_setting);
        
        // Programmable gain
        let pga_val = (config >> 9) & 0x7;
        let pga_setting = match pga_val {
            0 => "±6.144V",
            1 => "±4.096V",
            2 => "±2.048V",
            3 => "±1.024V",
            4 => "±0.512V",
            5 => "±0.256V",
            _ => "Unknown"
        };
        println!("Programmable gain: {}", pga_setting);
        
        // Mode
        let mode_bit = (config >> 8) & 0x1;
        println!("Mode: {}", if mode_bit == 0 {"Continuous"} else {"Single-shot"});
        
        // Data rate
        let dr_val = (config >> 5) & 0x7;
        let dr_setting = match dr_val {
            0 => "8 SPS",
            1 => "16 SPS",
            2 => "32 SPS",
            3 => "64 SPS",
            4 => "128 SPS",
            5 => "250 SPS",
            6 => "475 SPS",
            7 => "860 SPS",
            _ => "Unknown"
        };
        println!("Data rate: {}", dr_setting);

        // Comparator mode
        let comp_mode_bit = (config >> 4) & 0x1;
        println!("Comparator mode: {}", if comp_mode_bit == 0 {"Traditional"} else {"Window"});

        // Comparator polarity
        let comp_pol_bit = (config >> 3) & 0x1;
        println!("Comparator polarity: {}", if comp_pol_bit == 0 {"Active-low"} else {"Active-high"});

        // Comparator latch
        let comp_lat_bit = (config >> 2) & 0x1;
        println!("Comparator latch: {}", if comp_lat_bit == 0 {"Non-latching"} else {"Latching"});
        
        // Comparator queue
        let comp_que_val = (config & 0x3) as u8;
        println!("Comparator queue: {}", match comp_que_val {
            3 => String::from("Disabled"),
            n => format!("Assert after {} conversions", n + 1)
        });
    }

    // Write configuration
    let config_bytes = config.to_be_bytes();
    if i2c.write(address, &[0x01, config_bytes[0], config_bytes[1]]).is_err() {
        return false;
    }

    // Set thresholds only if comparator is enabled (comp_que != 3)
    if let Some(que) = comp_que {
        if que != 3 {
            // Set high threshold to +32767 (max positive)
            if i2c.write(address, &[0x02, 0x7F, 0xFF]).is_err() {
                return false;
            }
            // Set low threshold to -32768 (max negative)
            if i2c.write(address, &[0x03, 0x80, 0x00]).is_err() {
                return false;
            }
        }
    }

    true
}

fn turn_off_ads1115(i2c: &mut I2cdev, address: u8, verbose: bool) {
    let config_word = 
        ((false as u16) << 15) |          // OS: No effect
        ((0u8 & 0x7) as u16) << 12 |      // MUX: default (AIN0/AIN1)
        ((2u8 & 0x7) as u16) << 9 |       // PGA: default (±2.048V)
        ((true as u16) << 8) |            // MODE: default (Single-shot)
        ((4u8 & 0x7) as u16) << 5 |       // DR: default (128 SPS)
        ((false as u16) << 4) |           // COMP_MODE: default (Traditional)
        ((false as u16) << 3) |           // COMP_POL: default (Active-low)
        ((false as u16) << 2) |           // COMP_LAT: default (Non-latching)
        ((3u8 & 0x3) as u16);             // COMP_QUE: default (Disabled)
    
    // Write configuration
    let msb = ((config_word >> 8) & 0xFF) as u8;
    let lsb = (config_word & 0xFF) as u8;
    if let Err(_) = i2c.write(address, &[0x01, msb, lsb]) {
        println!("Failed to write default configuration");
        return;
    }
    
    // Read back and verify
    let mut read_buf = [0u8; 2];
    if let Err(_) = i2c.write_read(address, &[0x01], &mut read_buf) {
        println!("Failed to read back configuration");
        return;
    }
    
    let read_value = ((read_buf[0] as u16) << 8) | (read_buf[1] as u16);
    
    // Mask out the OS bit (bit 15) for comparison
    let config_masked = config_word & 0x7FFF;
    let read_masked = read_value & 0x7FFF;
    
    if read_masked == config_masked {
        if verbose {
            println!("Config returned to defaults");
        }
    } else {
        println!("Failed to reset configuration");
    }
}

impl ADS1115Collector {
    pub fn new(
        buffer: Arc<RotaryBuffer>,
        i2c_bus: u8,
        i2c_address: u8,
        sps: u32,
        verbose: bool,
    ) -> Self {
        ADS1115Collector {
            buffer,
            i2c_bus,
            i2c_address,
            sps,
            verbose,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.configure().await?;
        let result = self.collect().await;
        
        // Cleanup when stopping
        let i2c_path = format!("/dev/i2c-{}", self.i2c_bus);
        if let Ok(mut i2c) = I2cdev::new(&i2c_path) {
            turn_off_ads1115(&mut i2c, self.i2c_address, self.verbose);
        }
        
        result
    }
}

#[async_trait]
impl Collector for ADS1115Collector {
    async fn configure(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let i2c_path = format!("/dev/i2c-{}", self.i2c_bus);
        let mut i2c = I2cdev::new(&i2c_path)?;
        
        if !configure_ads1115(
            &mut i2c,
            self.i2c_address,
            Some(true),     // OS: Start conversion
            Some(0),        // MUX: AIN0 vs AIN1 (differential)
            Some(1),        // PGA: ±4.096V
            Some(false),    // MODE: Continuous
            Some(sps_to_dr(self.sps)),  // DR: based on sps
            None,           // COMP_MODE: default
            None,           // COMP_POL: default
            Some(true),     // COMP_LAT: Latching
            Some(0),        // COMP_QUE: Assert after 1 conversion
            self.verbose,
        ) {
            return Err("Failed to configure ADS1115".into());
        }

        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn collect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let i2c_path = format!("/dev/i2c-{}", self.i2c_bus);
        let mut i2c = I2cdev::new(&i2c_path)?;
        
        // Setup GPIO and channel for interrupts
        let gpio = Gpio::new()?;
        let mut gclk_pin = gpio.get(4)?.into_input_pulldown();
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let tx_interrupt = tx.clone();
        
        gclk_pin.set_async_interrupt(Trigger::FallingEdge, move |_level| {
            let _ = tx_interrupt.blocking_send(());
        })?;

        let start_time = std::time::Instant::now();
        
        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                Some(_) = rx.recv() => {
                    let mut read_buf = [0u8; 2];
                    if i2c.write_read(self.i2c_address, &[0x00], &mut read_buf).is_ok() {
                        let raw_value = i16::from_be_bytes([read_buf[0], read_buf[1]]);
                        let timestamp_nanos = start_time.elapsed().as_nanos();
                        
                        let reading = Reading {
                            value: raw_value,
                            timestamp_nanos,
                        };

                        while !self.buffer.write(reading.clone()) {
                            time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                }
                else => {
                    // Channel closed or other error
                    break;
                }
            }
        }
        
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

fn sps_to_dr(sps: u32) -> u8 {
    match sps {
        8 => 0,   // 000
        16 => 1,  // 001 
        32 => 2,  // 010
        64 => 3,  // 011
        128 => 4, // 100
        250 => 5, // 101
        475 => 6, // 110
        860 => 7, // 111
        _ => 7    // Default to fastest rate if invalid
    }
}

// -------------------------------------------------
// Tests
// -------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const DEFAULT_BUS: u8 = 1;
    const DEFAULT_ADDRESS: u8 = 0x48;
    const DEFAULT_SPS: u32 = 860;
    const TEST_SAMPLE_COUNT: usize = 2000;

    #[tokio::test]
    async fn test_collect_samples() {
        let buffer = Arc::new(RotaryBuffer::new(TEST_SAMPLE_COUNT));
        let collector = ADS1115Collector::new(
            buffer.clone(),
            DEFAULT_BUS,
            DEFAULT_ADDRESS,
            DEFAULT_SPS,
            true, // verbose for debugging
        );

        // Clone the running flag before moving collector
        let running = collector.running.clone();

        // Spawn collector task
        let collect_handle = tokio::spawn(async move {
            collector.run().await
        });

        // Wait for samples to be collected
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30); // 30 second timeout
        let mut total_samples = 0;
        const BATCH_SIZE: usize = 100;

        while total_samples < TEST_SAMPLE_COUNT {
            if start.elapsed() > timeout {
                panic!("Timeout waiting for samples. Only collected {} samples", total_samples);
            }

            let readings = buffer.read_batch(BATCH_SIZE).await;
            let batch_avg = readings.iter()
                .map(|r| r.value as f64)
                .sum::<f64>() / readings.len() as f64;
            
            println!("Batch {}: Average = {:.2}", 
                total_samples / BATCH_SIZE, 
                batch_avg
            );
            
            total_samples += readings.len();
        }

        // Calculate and print achieved SPS
        let elapsed_secs = start.elapsed().as_secs_f64();
        let achieved_sps = total_samples as f64 / elapsed_secs;
        println!("\nCollection Statistics:");
        println!("  Total Samples: {}", total_samples);
        println!("  Elapsed Time: {:.2} seconds", elapsed_secs);
        println!("  Achieved SPS: {:.1} samples/second", achieved_sps);
        println!("  Target SPS: {}", DEFAULT_SPS);

        // Verify we got enough samples
        assert!(total_samples >= TEST_SAMPLE_COUNT, 
            "Expected {} samples, but got {}", TEST_SAMPLE_COUNT, total_samples);

        // Use the cloned running flag to stop the collector
        running.store(false, Ordering::SeqCst);
        let _ = collect_handle.await;
    }
} 
