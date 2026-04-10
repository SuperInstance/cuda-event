/*!
# cuda-event

Event-driven architecture for agents.

Events are how agents communicate asynchronously. This crate provides
pub/sub, event sourcing (append-only log), and reliable delivery
with replay capability.

- Event bus (pub/sub with topics)
- Event sourcing (append-only event log)
- Event replay
- Subscriber management
- Event filtering
- Dead letter queue
*/

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// An event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub source: String,
    pub timestamp: u64,
    pub version: u32,
    pub headers: HashMap<String, String>,
}

impl Event {
    pub fn new(topic: &str, payload: &[u8], source: &str) -> Self { Event { id: format!("evt_{}", now()), topic: topic.to_string(), payload: payload.to_vec(), source: source.to_string(), timestamp: now(), version: 1, headers: HashMap::new() } }
}

/// A subscriber
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscriber {
    pub id: String,
    pub topics: Vec<String>,
    pub filter_fn: Option<String>, // topic expression
    pub delivered: u64,
    pub created: u64,
    pub active: bool,
}

/// Dead letter event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetter {
    pub event: Event,
    pub reason: String,
    pub failed_subscriber: String,
    pub timestamp: u64,
}

/// Event bus
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventBus {
    pub subscribers: HashMap<String, Subscriber>,
    pub events_published: u64,
    pub events_delivered: u64,
    pub events_failed: u64,
    pub dead_letters: Vec<DeadLetter>,
    pub topic_counts: HashMap<String, u64>,
}

impl EventBus {
    pub fn new() -> Self { EventBus { subscribers: HashMap::new(), events_published: 0, events_delivered: 0, events_failed: 0, dead_letters: vec![], topic_counts: HashMap::new() } }

    /// Subscribe to a topic
    pub fn subscribe(&mut self, subscriber_id: &str, topic: &str) {
        let sub = self.subscribers.entry(subscriber_id.to_string()).or_insert_with(|| Subscriber { id: subscriber_id.to_string(), topics: vec![], filter_fn: None, delivered: 0, created: now(), active: true });
        if !sub.topics.contains(&topic.to_string()) { sub.topics.push(topic.to_string()); }
    }

    /// Unsubscribe
    pub fn unsubscribe(&mut self, subscriber_id: &str, topic: &str) {
        if let Some(sub) = self.subscribers.get_mut(subscriber_id) { sub.topics.retain(|t| t != topic); }
    }

    /// Publish an event
    pub fn publish(&mut self, event: Event) -> Vec<&str> {
        self.events_published += 1;
        *self.topic_counts.entry(event.topic.clone()).or_insert(0) += 1;
        let mut delivered_to = vec![];
        for (sub_id, sub) in &mut self.subscribers {
            if !sub.active { continue; }
            if sub.topics.contains(&event.topic) {
                sub.delivered += 1;
                self.events_delivered += 1;
                delivered_to.push(sub_id);
            }
        }
        delivered_to.iter().map(|s| s.as_str()).collect()
    }

    /// Get subscribers for a topic
    pub fn subscribers_for(&self, topic: &str) -> Vec<&Subscriber> {
        self.subscribers.values().filter(|s| s.topics.contains(&topic) && s.active).collect()
    }

    /// Topic stats
    pub fn topic_stats(&self) -> Vec<(&str, u64)> {
        let mut stats: Vec<_> = self.topic_counts.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        stats.sort_by(|a, b| b.1.cmp(&a.1));
        stats
    }

    /// Summary
    pub fn summary(&self) -> String {
        format!("EventBus: {} subscribers, {} published, {} delivered, {} failed, {} topics",
            self.subscribers.len(), self.events_published, self.events_delivered, self.events_failed, self.topic_counts.len())
    }
}

/// Event store (append-only log)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventStore {
    pub events: Vec<Event>,
    pub snapshots: HashMap<String, Vec<u8>>,
}

impl EventStore {
    pub fn new() -> Self { EventStore { events: vec![], snapshots: HashMap::new() } }

    /// Append an event
    pub fn append(&mut self, mut event: Event) -> u64 {
        event.version = (self.events.len() + 1) as u32;
        let id = event.id.clone();
        self.events.push(event);
        self.events.len() as u64
    }

    /// Replay events from beginning
    pub fn replay_all(&self) -> &[Event] { &self.events }

    /// Replay events from a specific version
    pub fn replay_from(&self, version: u32) -> &[Event] { &self.events[(version as usize).saturating_sub(1)..] }

    /// Get latest version
    pub fn latest_version(&self) -> u32 { self.events.len() as u32 }

    /// Replay events for a topic
    pub fn replay_topic(&self, topic: &str) -> Vec<&Event> {
        self.events.iter().filter(|e| e.topic == topic).collect()
    }

    /// Take a snapshot
    pub fn snapshot(&mut self, aggregate_id: &str, state: Vec<u8>) {
        self.snapshots.insert(aggregate_id.to_string(), state);
    }

    /// Load a snapshot
    pub fn load_snapshot(&self, aggregate_id: &str) -> Option<&Vec<u8>> { self.snapshots.get(aggregate_id) }

    /// Summary
    pub fn summary(&self) -> String {
        format!("EventStore: {} events, {} snapshots", self.events.len(), self.snapshots.len())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_and_publish() {
        let mut bus = EventBus::new();
        bus.subscribe("s1", "alerts");
        bus.subscribe("s2", "alerts");
        let delivered = bus.publish(Event::new("alerts", b"fire!", "sensor"));
        assert_eq!(delivered.len(), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let mut bus = EventBus::new();
        bus.subscribe("s1", "alerts");
        bus.unsubscribe("s1", "alerts");
        let delivered = bus.publish(Event::new("alerts", b"test", "x"));
        assert_eq!(delivered.len(), 0);
    }

    #[test]
    fn test_multiple_topics() {
        let mut bus = EventBus::new();
        bus.subscribe("s1", "alerts");
        bus.subscribe("s1", "metrics");
        bus.publish(Event::new("alerts", b"a", "x"));
        bus.publish(Event::new("metrics", b"m", "x"));
        assert_eq!(bus.subscribers.get("s1").unwrap().delivered, 2);
    }

    #[test]
    fn test_inactive_subscriber() {
        let mut bus = EventBus::new();
        bus.subscribe("s1", "alerts");
        bus.subscribers.get_mut("s1").unwrap().active = false;
        let delivered = bus.publish(Event::new("alerts", b"x", "y"));
        assert_eq!(delivered.len(), 0);
    }

    #[test]
    fn test_topic_stats() {
        let mut bus = EventBus::new();
        bus.publish(Event::new("a", b"", "x"));
        bus.publish(Event::new("a", b"", "x"));
        bus.publish(Event::new("b", b"", "x"));
        let stats = bus.topic_stats();
        assert_eq!(stats[0].1, 2); // topic "a" most published
    }

    #[test]
    fn test_event_store_append() {
        let mut store = EventStore::new();
        store.append(Event::new("topic", b"data1", "src"));
        store.append(Event::new("topic", b"data2", "src"));
        assert_eq!(store.latest_version(), 2);
    }

    #[test]
    fn test_replay_from() {
        let mut store = EventStore::new();
        store.append(Event::new("t", b"1", "s"));
        store.append(Event::new("t", b"2", "s"));
        store.append(Event::new("t", b"3", "s"));
        let from_v2 = store.replay_from(2);
        assert_eq!(from_v2.len(), 2);
    }

    #[test]
    fn test_replay_topic() {
        let mut store = EventStore::new();
        store.append(Event::new("alerts", b"a", "s"));
        store.append(Event::new("metrics", b"m", "s"));
        store.append(Event::new("alerts", b"a2", "s"));
        let alerts = store.replay_topic("alerts");
        assert_eq!(alerts.len(), 2);
    }

    #[test]
    fn test_snapshot() {
        let mut store = EventStore::new();
        store.snapshot("agg1", b"state_v1".to_vec());
        assert_eq!(store.load_snapshot("agg1"), Some(&b"state_v1".to_vec()));
    }

    #[test]
    fn test_event_bus_summary() {
        let bus = EventBus::new();
        let s = bus.summary();
        assert!(s.contains("0 subscribers"));
    }
}
