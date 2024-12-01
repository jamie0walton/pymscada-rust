use crate::tag::TagManager;
use crate::rotary_buffer::{RotaryBuffer, Reading};
use std::sync::Arc;
use tokio::time::{self, Duration};
use num_complex::Complex;
use rustfft::FftPlanner;

pub struct Processor {
    tag_prefix: String,
    tag_manager: TagManager,
    buffer: Arc<RotaryBuffer>,
    batch_size: usize,
    ct_ratio: f64,
    verbose: bool,
    fft: bool,
}

impl Processor {
    pub fn new(
        tag_prefix: String,
        tag_manager: TagManager,
        buffer: Arc<RotaryBuffer>,
        batch_size: usize,
        ct_ratio: f64,
        verbose: bool,
        fft: bool,
    ) -> Self {
        Processor {
            tag_prefix,
            tag_manager,
            buffer,
            batch_size,
            ct_ratio,
            verbose,
            fft,
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
        (sum_squares / processed.len() as f64).sqrt()
    }

    fn calculate_harmonics(&self, processed: &[f64]) -> Vec<f64> {
        // Find zero crossings and trim data
        let mut start_idx = 0;
        for i in 1..processed.len() {
            if processed[i-1] < 0.0 && processed[i] >= 0.0 {
                let dist_prev = processed[i-1].abs();
                let dist_curr = processed[i].abs();
                start_idx = if dist_prev < dist_curr { i-1 } else { i };
                break;
            }
        }
        
        let mut end_idx = processed.len() - 1;
        for i in (1..processed.len()).rev() {
            if processed[i-1] < 0.0 && processed[i] >= 0.0 {
                let dist_prev = processed[i-1].abs();
                let dist_curr = processed[i].abs();
                end_idx = if dist_prev < dist_curr { i-1 } else { i };
                break;
            }
        }
        
        // Use trimmed slice for analysis
        let trimmed_values = &processed[start_idx..=end_idx];
        
        // Apply Hamming window
        let windowed: Vec<Complex<f64>> = trimmed_values.iter()
            .enumerate()
            .map(|(i, &x)| {
                let window = 0.54 - 0.46 * (2.0 * std::f64::consts::PI * i as f64 / (trimmed_values.len() - 1) as f64).cos();
                Complex::new(x * window, 0.0)
            })
            .collect();

        let mut planner = FftPlanner::new();
        let fft = planner.plan_fft_forward(windowed.len());
        let mut spectrum = windowed;
        fft.process(&mut spectrum);

        // Calculate magnitude spectrum
        let magnitudes: Vec<f64> = spectrum.iter()
            .take(spectrum.len() / 2)  // Only take first half (Nyquist)
            .map(|c| (c.norm() / (processed.len() as f64).sqrt()))
            .collect();

        // Find fundamental frequency (largest magnitude after DC)
        let fundamental_idx = (1..magnitudes.len())
            .max_by(|&i, &j| magnitudes[i].partial_cmp(&magnitudes[j]).unwrap())
            .unwrap();
        let fundamental_magnitude = magnitudes[fundamental_idx];

        // Calculate harmonic percentages (2nd through 7th harmonics)
        (2..=7).map(|i| {
            let harmonic_idx = fundamental_idx * i;
            if harmonic_idx < magnitudes.len() {
                (magnitudes[harmonic_idx] / fundamental_magnitude) * 100.0
            } else {
                0.0
            }
        }).collect()
    }

    fn calculate_effective_sps(&self, readings: &[Reading]) -> f64 {
        if readings.len() < 2 {
            return 0.0;
        }
        
        // Calculate time difference between first and last reading in seconds
        let time_span_nanos = readings.last().unwrap().timestamp_nanos - readings.first().unwrap().timestamp_nanos;
        let time_span_seconds = time_span_nanos as f64 / 1_000_000_000.0;
        
        // Calculate samples per second
        (readings.len() as f64 - 1.0) / time_span_seconds
    }

    async fn setup_tags(&self) {
        // Create RMS tag
        let rms_tag = format!("{}_rms", self.tag_prefix);
        if self.verbose {
            println!("Creating tag: {}", rms_tag);
        }
        self.tag_manager.update(&rms_tag, 0.0, 0).await;
        
        // Create SPS tag
        let sps_tag = format!("{}_sps", self.tag_prefix);
        if self.verbose {
            println!("Creating tag: {}", sps_tag);
        }
        self.tag_manager.update(&sps_tag, 0.0, 0).await;

        // Create harmonic tags if FFT is enabled
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

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.setup_tags().await;
        
        let mut interval = time::interval(Duration::from_millis(500));
        
        loop {
            interval.tick().await;
            let readings = self.buffer.read_batch(self.batch_size).await;
            if !readings.is_empty() {
                let processed = self.process_readings(&readings);
                let time_us = chrono::Utc::now().timestamp_micros();
                let effective_sps = self.calculate_effective_sps(&readings);
                self.tag_manager.update(
                    &format!("{}_sps", self.tag_prefix),
                    effective_sps,
                    time_us
                ).await;
                let rms = self.calculate_rms(&processed);
                self.tag_manager.update(
                    &format!("{}_rms", self.tag_prefix),
                    rms,
                    time_us
                ).await;
                if self.fft {
                    let harmonics = self.calculate_harmonics(&processed);
                    for (i, percentage) in harmonics.iter().enumerate() {
                        self.tag_manager.update(
                            &format!("{}_harmonic_{}", self.tag_prefix, i + 2),
                            *percentage,
                            time_us
                        ).await;
                    }
                }
            }
        }
    }
}

// -------------------------------------------------
// Tests
// -------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn test_harmonics_analysis() {
        let sample_rate = 10000.0; // 10kHz
        let duration = 0.1; // 100ms
        let num_samples = (sample_rate * duration) as usize;
        let fundamental_freq = 50.0;
        
        // Define test cases with expected values
        let test_cases = vec![
            (
                "Pure Signal",
                vec![(1.0, 1)],  // Just fundamental
                0.707,  // Expected RMS
                vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]  // Expected harmonics (2-7)
            ),
            (
                "Complex Signal",
                vec![
                    (1.0, 1),   // Fundamental
                    (0.02, 2),  // 2% of 2nd harmonic
                    (0.03, 3),  // 3% of 3rd harmonic
                    (0.04, 4),  // 4% of 4th harmonic
                    (0.05, 5),  // 5% of 5th harmonic
                    (0.06, 6),  // 6% of 6th harmonic
                    (0.07, 7),  // 7% of 7th harmonic
                ],
                0.712,  // Expected RMS
                vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0]  // Expected harmonics (2-7)
            ),
        ];

        // Create processor instance
        let (tag_manager, _rx) = TagManager::new(0.0);
        let buffer = Arc::new(RotaryBuffer::new(num_samples));
        let processor = Processor::new(
            "test".to_string(),
            tag_manager,
            buffer,
            num_samples,
            1.0,
            false,
            true
        );

        // Test each case
        for (case_name, harmonics, expected_rms, expected_harmonics) in test_cases {
            println!("\n{} Analysis:", case_name);
            
            // Generate signal
            let signal: Vec<Reading> = (0..num_samples)
                .map(|i| {
                    let t = i as f64 / sample_rate;
                    let value: f64 = harmonics.iter()
                        .map(|(amplitude, n)| {
                            amplitude * (2.0 * PI * fundamental_freq * (*n as f64) * t).sin()
                        })
                        .sum();
                    Reading {
                        value: (value * 32768.0 / 4.096) as i16,
                        timestamp_nanos: (i as u128) * 100_000
                    }
                })
                .collect();

            // Process signal
            let processed = processor.process_readings(&signal);
            let rms = processor.calculate_rms(&processed);
            let found_harmonics = processor.calculate_harmonics(&processed);

            // Print results
            println!("RMS Analysis:");
            println!("  Expected: {:.3}", expected_rms);
            println!("  Found:    {:.3}", rms);
            println!("  Diff:     {:.3}", (rms - expected_rms).abs());
            
            println!("\nHarmonics Analysis:");
            for (i, (found, expected)) in found_harmonics.iter().zip(expected_harmonics.iter()).enumerate() {
                println!("  H{}: Expected: {:.3}%, Found: {:.3}%, Diff: {:.3}%", 
                        i + 2, expected, found, (found - expected).abs());
            }

            // Assertions
            assert!((rms - expected_rms).abs() < 0.01, 
                "{}: RMS error too large. Expected {}, found {}", 
                case_name, expected_rms, rms);

            for (i, (found, expected)) in found_harmonics.iter().zip(expected_harmonics.iter()).enumerate() {
                assert!((found - expected).abs() < 0.5,
                    "{}: Harmonic {} error too large. Expected {:.1}%, found {:.1}%", 
                    case_name, i + 2, expected, found);
            }
        }
    }
}

