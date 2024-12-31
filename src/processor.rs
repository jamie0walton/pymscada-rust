use crate::tag::TagManager;
use crate::rotary_buffer::{RotaryBuffer, Reading};
use std::sync::Arc;
use chrono::Timelike;

pub struct Processor {
    tag_prefix: String,
    tag_manager: TagManager,
    buffer: Arc<RotaryBuffer>,
    batch_size: usize,
    ct_ratio: f64,
    verbose: bool,
    amp_hours: f64,
}

impl Processor {
    pub fn new(
        tag_prefix: String,
        tag_manager: TagManager,
        buffer: Arc<RotaryBuffer>,
        batch_size: usize,
        ct_ratio: f64,
        verbose: bool,
    ) -> Self {
        Processor {
            tag_prefix,
            tag_manager,
            buffer,
            batch_size,
            ct_ratio,
            verbose,
            amp_hours: 0.0,
        }
    }

    fn scale_readings(&self, readings: &[Reading]) -> Vec<f64> {
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
        (sum_squares / processed.len() as f64).sqrt()
    }

    async fn setup_tags(&self) {
        // Create RMS tag
        let rms_tag_name = format!("{}_rms", self.tag_prefix);
        if self.verbose {
            println!("Creating tag: {}", rms_tag_name);
        }
        self.tag_manager.get_tag(&rms_tag_name).await;
        
        // Create SPS tag
        let sps_tag_name = format!("{}_sps", self.tag_prefix);
        if self.verbose {
            println!("Creating tag: {}", sps_tag_name);
        }
        self.tag_manager.get_tag(&sps_tag_name).await;
        self.tag_manager.set_deadband(&sps_tag_name, 2.0).await;

        // Create amp-hour tag
        let ah_tag_name = format!("{}_Ah", self.tag_prefix);
        if self.verbose {
            println!("Creating tag: {}", ah_tag_name);
        }
        self.tag_manager.get_tag(&ah_tag_name).await;
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_tags().await;
        
        let mut amp_hours = self.amp_hours;
        let mut compensation = 0.0;
        let mut last_hour = 0;
        let mut last_time_us = chrono::Utc::now().timestamp_micros();
        let mut seen_new_day = false;

        loop {
            // buffer.read_batch() will block, do not add a wait.
            let readings = self.buffer.read_batch(self.batch_size).await;

            // Work out the time first, then calculate samples per second
            let time_us = chrono::Utc::now().timestamp_micros();
            let sps = readings.len() as f64 * 1_000_000.0 /
                (time_us - last_time_us) as f64;
            self.tag_manager.update(
                &format!("{}_sps", self.tag_prefix),
                sps,
                time_us
            ).await;
            if self.verbose {
                println!("Readings: {}", readings.len());
                println!("Samples per second: {}", sps);
            }
            
            // Scale the readings
            let scaled = self.scale_readings(&readings);

            // Work out the RMS from the readings
            let rms = self.calculate_rms(&scaled);
            self.tag_manager.update(
                &format!("{}_rms", self.tag_prefix),
                rms,
                time_us
            ).await;
            if self.verbose {
                println!("rms: {}", rms);
            }

            // Sum amp-hours from midnight
            let current_hour = chrono::Local::now().hour();
            if current_hour < last_hour {
                println!("Midnight crossed! Resetting amp-hours from {} to 0.0", amp_hours);
                seen_new_day = true;
                amp_hours = 0.0;
                self.tag_manager.update(
                    &format!("{}_Ah", self.tag_prefix),
                    0.0,
                    time_us
                ).await;
            }
            last_hour = current_hour;
            if seen_new_day {
                let increment = rms * (time_us - last_time_us) as f64 / 3600.0 / 1_000_000.0;
                let y = increment - compensation;
                let t = amp_hours + y;
                compensation = (t - amp_hours) - y;
                amp_hours = t;
                self.tag_manager.update(
                    &format!("{}_Ah", self.tag_prefix),
                    amp_hours,
                    time_us
                ).await;
            }
            last_time_us = time_us;
        }
    }
}

