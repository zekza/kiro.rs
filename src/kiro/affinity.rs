//! 用户到凭据的短期亲和绑定。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

struct AffinityEntry {
    credential_id: u64,
    last_used: Instant,
}

pub struct UserAffinityManager {
    entries: Mutex<HashMap<u64, AffinityEntry>>,
    ttl: Duration,
}

impl Default for UserAffinityManager {
    fn default() -> Self {
        Self::new()
    }
}

impl UserAffinityManager {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(30 * 60),
        }
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        let mut entries = self.entries.lock();
        match entries.get_mut(&key) {
            Some(entry) if entry.last_used.elapsed() < self.ttl => {
                entry.last_used = Instant::now();
                Some(entry.credential_id)
            }
            Some(_) => {
                entries.remove(&key);
                None
            }
            None => None,
        }
    }

    pub fn set(&self, key: u64, credential_id: u64) {
        self.entries.lock().insert(
            key,
            AffinityEntry {
                credential_id,
                last_used: Instant::now(),
            },
        );
    }

    pub fn remove_by_credential(&self, credential_id: u64) {
        self.entries
            .lock()
            .retain(|_, entry| entry.credential_id != credential_id);
    }
}
