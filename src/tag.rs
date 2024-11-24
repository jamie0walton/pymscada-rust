// The tag manager shall:
// - store tags
// - update tags
// Tags shall have:
// - id: 0 when unassigned, otherwise a unique identifier
// - name: a string
// - value: may be empty, otherwise a float
// - time_us: an integer timestamp in microseconds
// - deadband: a float
// TagManager::update() shall:
// - receive a tag name, value, and timestamp
// - if the tag value is empty, update
// - otherwise only update if the difference in the value is > deadband
// - update the value and timestamp and call BusClient::publish()
// TagManager::set_id() shall:
// - receive a tag name and integer id
// - set the id for the tag
// - trigger a BusClient::publish() for the tag
// TagManager::reset_ids() shall:
// - reset all ids to 0

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct TagValue {
    pub value: f64,
    #[allow(dead_code)]
    pub time_us: i64,
}

#[derive(Clone)]
pub struct Tag {
    pub id: u32,
    #[allow(dead_code)]
    pub name: String,
    pub value: TagValue,
    pub deadband: f64,
}

pub enum TagMessage {
    Update { name: String, value: f64, time_us: i64 },
    SetId { name: String, id: u32 },
    ResetIds,
}

#[derive(Clone)]
pub struct TagManager {
    tags: Arc<RwLock<HashMap<String, Tag>>>,
    default_deadband: f64,
    sender: mpsc::Sender<TagMessage>,
}

impl TagManager {
    pub fn new(default_deadband: f64) -> (Self, mpsc::Receiver<TagMessage>) {
        let (sender, receiver) = mpsc::channel(100);  // Buffer size of 100
        (TagManager {
            tags: Arc::new(RwLock::new(HashMap::new())),
            default_deadband,
            sender,
        }, receiver)
    }

    pub async fn update(&self, name: &str, value: f64, time_us: i64) {
        let mut tags = self.tags.write().await;
        
        if let Some(existing_tag) = tags.get(name) {
            let diff = (existing_tag.value.value - value).abs();
            if diff <= existing_tag.deadband {
                return;
            }
        }

        tags.insert(
            name.to_string(),
            Tag {
                id: 0,
                name: name.to_string(),
                value: TagValue { value, time_us },
                deadband: self.default_deadband,
            },
        );

        // Send update message through channel
        let _ = self.sender.send(TagMessage::Update {
            name: name.to_string(),
            value,
            time_us,
        }).await;
    }

    pub async fn set_id(&self, name: &str, id: u32) {
        let mut tags = self.tags.write().await;
        if let Some(tag) = tags.get_mut(name) {
            tag.id = id;
        }

        self.sender.send(TagMessage::SetId { name: name.to_string(), id }).await.unwrap();
    }

    pub async fn reset_ids(&self) {
        let mut tags = self.tags.write().await;
        for tag in tags.values_mut() {
            tag.id = 0;
        }

        self.sender.send(TagMessage::ResetIds).await.unwrap();
    }

    pub async fn get_tag(&self, name: &str) -> Option<Tag> {
        let tags = self.tags.read().await;
        tags.get(name).cloned()
    }
} 