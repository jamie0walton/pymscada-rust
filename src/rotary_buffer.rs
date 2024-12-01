use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub struct Reading {
    pub value: i16,
    pub timestamp_nanos: u128,
}

pub struct RotaryBuffer {
    buffer: RwLock<Vec<Reading>>,
    write_ptr: AtomicUsize,
    read_ptr: AtomicUsize,
    capacity: usize,
    notify: Notify,
}

impl RotaryBuffer {
    pub fn new(capacity: usize) -> Self {
        RotaryBuffer {
            buffer: RwLock::new(vec![Reading { value: 0, timestamp_nanos: 0 }; capacity]),
            write_ptr: AtomicUsize::new(0),
            read_ptr: AtomicUsize::new(0),
            capacity,
            notify: Notify::new(),
        }
    }

    pub fn write(&self, reading: Reading) -> bool {
        let current_write = self.write_ptr.load(Ordering::Relaxed);
        let next_write = (current_write + 1) % self.capacity;
        let current_read = self.read_ptr.load(Ordering::Relaxed);

        if next_write == current_read {
            return false; // Buffer full
        }

        if let Ok(mut buffer) = self.buffer.write() {
            buffer[current_write] = reading;
            self.write_ptr.store(next_write, Ordering::Release);
            self.notify.notify_one();
            true
        } else {
            false
        }
    }

    pub async fn read_batch(&self, batch_size: usize) -> Vec<Reading> {
        loop {
            let current_read = self.read_ptr.load(Ordering::Relaxed);
            let current_write = self.write_ptr.load(Ordering::Acquire);
            
            let available = if current_write >= current_read {
                current_write - current_read
            } else {
                self.capacity - current_read + current_write
            };

            if available >= batch_size {
                if let Ok(buffer) = self.buffer.read() {
                    let mut readings = Vec::with_capacity(batch_size);
                    let mut read_pos = current_read;

                    for _ in 0..batch_size {
                        readings.push(buffer[read_pos].clone());
                        read_pos = (read_pos + 1) % self.capacity;
                    }

                    self.read_ptr.store(read_pos, Ordering::Release);
                    return readings;
                }
            }

            // Wait for more data
            self.notify.notified().await;
        }
    }
}


/*
-------------------------------------------------
Tests
-------------------------------------------------
*/
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_buffer_with_mock_collector_processor() {
        let buffer = Arc::new(RotaryBuffer::new(10000));
        let buffer_collector = buffer.clone();
        let buffer_processor = buffer.clone();
        
        // Spawn mock processor first (no changes to its logic)
        let processor_handle = tokio::spawn(async move {
            let batch_size = 100;
            let mut last_value = -1i16;
            let mut total_processed = 0;

            while total_processed < 50_000 {
                let readings = buffer_processor.read_batch(batch_size).await;
                
                // Verify sequence
                assert_eq!(readings.len(), batch_size, "Unexpected batch size");
                assert_eq!(readings[0].value as i16, last_value + 1, 
                    "Sequence break: expected {}, got {}", 
                    last_value + 1, readings[0].value);
                
                last_value = readings[batch_size - 1].value;
                total_processed += batch_size;

                if total_processed % 10000 == 0 {
                    println!("Processed {} readings", total_processed);
                }
            }
            
            assert_eq!(total_processed, 50_000, "Did not process all readings");
            println!("Successfully processed all readings");
        });

        // Add shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        // Spawn mock collector with shutdown capability
        let collector_handle = tokio::spawn(async move {
            let mut i = 0;
            loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }

                let reading = Reading {
                    value: i as i16,
                    timestamp_nanos: i as u128,
                };
                
                while !buffer_collector.write(reading.clone()) {
                    sleep(Duration::from_micros(100)).await;
                }

                if i % 40 == 0 {
                    sleep(Duration::from_micros(10)).await;
                }
                i += 1;
            }
        });

        // Wait for processor to complete first
        processor_handle.await.unwrap();
        
        // Then shutdown the collector
        shutdown_tx.send(()).unwrap();
        
        // Finally wait for collector to shutdown
        collector_handle.await.unwrap();
    }
} 