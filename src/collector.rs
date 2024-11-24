// The collector shall:
// - set up the tags that it will write data to
// - carry out a periodic task
// - in the periodic task:
//   - collect and timestamp data
//   - write the data to the tag
//
// For this example, read seconds from the system clock
// and write it to a tag named "seconds"

use crate::tag::TagManager;
use async_trait::async_trait;
use tokio::time::{self, Duration};

#[async_trait]
pub trait Collector {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct DataCollector {
    tag_manager: TagManager,
}

unsafe impl Send for DataCollector {}
unsafe impl Sync for DataCollector {}

impl DataCollector {
    pub fn new(tag_manager: TagManager) -> Self {
        DataCollector { tag_manager }
    }

    // Set up the tags that this collector will write to
    async fn setup_tags(&self) {
        // Initialize the "seconds" tag with default values
        self.tag_manager.update("seconds", 0.0, 0).await;
    }

    // Internal method to collect and timestamp data
    async fn collect_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        let seconds = chrono::Utc::now().timestamp() as f64 % 60.0;
        let time_us = chrono::Utc::now().timestamp_micros();
        self.tag_manager.update("seconds", seconds, time_us).await;
        Ok(())
    }
}

#[async_trait]
impl Collector for DataCollector {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Set up tags first
        self.setup_tags().await;
        
        // Create event source (for this example, using a timer)
        let mut interval = time::interval(Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            if let Err(e) = self.collect_data().await {
                eprintln!("Error collecting data: {}", e);
            }
        }
    }
} 