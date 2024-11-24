use crate::tag::TagManager;
use async_trait::async_trait;
use linux_embedded_hal::I2cdev;
use embedded_hal::blocking::i2c::{Write, WriteRead};
use rppal::gpio::{Gpio, Trigger};
use std::sync::mpsc;
use rustfft::{FftPlanner, num_complex::Complex};
use std::time::Instant;
use tokio::time::{self, Duration};

#[derive(Debug)]
struct Reading {
    value: i16,
    timestamp_nanos: u128,
}

#[async_trait]
pub trait Collector {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct ADS1115Collector {
    tag_prefix: String,
    tag_manager: TagManager,
    i2c_bus: u8,
    i2c_address: u8,
    ct_ratio: f64,
    sps: u32,
    verbose: bool,
    fft: bool,
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
        ((false as u16) << 15) |           // OS: No effect
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
        tag_prefix: String,
        tag_manager: TagManager,
        i2c_bus: u8,
        i2c_address: u8,
        ct_ratio: f64,
        sps: u32,
        verbose: bool,
        fft: bool,
    ) -> Self {
        ADS1115Collector {
            tag_prefix,
            tag_manager,
            i2c_bus,
            i2c_address,
            ct_ratio,
            sps,
            verbose,
            fft,
        }
    }

    async fn setup_tags(&self) {
        let rms_tag = format!("{}_rms", self.tag_prefix);
        if self.verbose {
            println!("Creating tag: {}", rms_tag);
        }
        self.tag_manager.update(&rms_tag, 0.0, 0).await;
        
        if self.fft {
            for i in 2..=7 {
                let harmonic_tag = format!("{}_harmonic_{}", self.tag_prefix, i);
                if self.verbose {
                    println!("Creating tag: {}", harmonic_tag);
                }
                self.tag_manager.update(&harmonic_tag, 0.0, 0).await;
            }
        }
    }

    fn process_readings(&self, readings: &[Reading]) -> Vec<f64> {
        // Convert raw ADC values to current values using CT ratio
        readings.iter()
            .map(|r| (r.value as f64) * 4.096 / 32768.0 * self.ct_ratio)
            .collect()
    }

    fn calculate_rms(&self, processed: &[f64]) -> f64 {
        // Calculate RMS value
        let sum_squares: f64 = processed.iter()
            .map(|x| x * x)
            .sum();
        let rms = (sum_squares / processed.len() as f64).sqrt();
        rms
    }

    fn calculate_harmonics(&self, processed: &[f64]) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        let mut planner = FftPlanner::new();
        let fft = planner.plan_fft_forward(processed.len());

        // Prepare complex input
        let mut complex_input: Vec<Complex<f64>> = processed.iter()
            .map(|&x| Complex::new(x, 0.0))
            .collect();

        // Perform FFT
        fft.process(&mut complex_input);

        // Calculate magnitude spectrum
        let magnitudes: Vec<f64> = complex_input.iter()
            .map(|c| c.norm())
            .collect();

        // Calculate phase spectrum
        let phases: Vec<f64> = complex_input.iter()
            .map(|c| c.arg())
            .collect();

        // Calculate harmonic percentages relative to fundamental
        let fundamental = magnitudes[1];  // Index 1 is 50/60Hz component
        let harmonics: Vec<f64> = magnitudes.iter()
            .skip(2)
            .take(6)  // Get harmonics 2-7
            .map(|&m| (m / fundamental) * 100.0)
            .collect();

        (harmonics, magnitudes, phases)
    }
}

#[async_trait]
impl Collector for ADS1115Collector {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_tags().await;
        
        // Setup I2C
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

        let mut interval = time::interval(Duration::from_millis(1000));
        
        loop {
            interval.tick().await;
            let mut readings = Vec::with_capacity(120);
            let start_time = Instant::now();

            // Configure ADS1115
            if !configure_ads1115(
                &mut i2c,
                self.i2c_address,
                Some(true),     // OS: Start conversion
                Some(0),        // MUX: AIN0 vs AIN1 (differential)
                Some(1),        // PGA: ±4.096V
                Some(false),    // MODE: Continuous
                Some(7),        // DR: 860 SPS
                None,           // COMP_MODE: default
                None,           // COMP_POL: default
                Some(true),     // COMP_LAT: Latching
                Some(0),        // COMP_QUE: Assert after 1 conversion
                self.verbose,
            ) {
                eprintln!("Failed to configure ADS1115");
                continue;
            }

            // Collect readings
            while readings.len() < 120 {
                tokio::select! {
                    Some(_) = rx.recv() => {
                        let mut read_buf = [0u8; 2];
                        if i2c.write_read(self.i2c_address, &[0x00], &mut read_buf).is_ok() {
                            let raw_value = i16::from_be_bytes([read_buf[0], read_buf[1]]);
                            let timestamp_nanos = start_time.elapsed().as_nanos();
                            readings.push(Reading { value: raw_value, timestamp_nanos });
                        }
                    }
                    else => break,
                }
            }

            // Turn off ADS1115 after collection
            turn_off_ads1115(&mut i2c, self.i2c_address, self.verbose);

            // Process readings and update tags
            if !readings.is_empty() {
                let processed = self.process_readings(&readings);
                let time_us = chrono::Utc::now().timestamp_micros();
                
                // Calculate and update RMS
                let rms = self.calculate_rms(&processed);
                self.tag_manager.update(&format!("{}_rms", self.tag_prefix), rms, time_us).await;

                // Calculate and update FFT if enabled
                if self.fft {
                    let (harmonics, _, _) = self.calculate_harmonics(&processed);
                    for (i, percentage) in harmonics.iter().enumerate().skip(1) {
                        self.tag_manager.update(
                            &format!("{}_harmonic_{}", self.tag_prefix, i + 1),
                            *percentage,
                            time_us
                        ).await;
                    }
                }
            }
        }
    }
} 