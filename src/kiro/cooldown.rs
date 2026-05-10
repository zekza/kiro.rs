//! 凭据临时冷却管理。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CooldownReason {
    RateLimitExceeded,
    ServerError,
    TokenRefreshFailed,
}

impl CooldownReason {
    fn base_duration(self) -> Duration {
        match self {
            Self::RateLimitExceeded => Duration::from_secs(60),
            Self::ServerError => Duration::from_secs(120),
            Self::TokenRefreshFailed => Duration::from_secs(60),
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::RateLimitExceeded => "rate_limit",
            Self::ServerError => "server_error",
            Self::TokenRefreshFailed => "token_refresh_failed",
        }
    }
}

#[derive(Debug, Clone)]
struct CooldownEntry {
    reason: CooldownReason,
    expires_at: Instant,
    trigger_count: u32,
}

pub struct CooldownManager {
    entries: Mutex<HashMap<u64, CooldownEntry>>,
    max_short_cooldown: Duration,
}

impl Default for CooldownManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CooldownManager {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_short_cooldown: Duration::from_secs(300),
        }
    }

    pub fn is_available(&self, credential_id: u64) -> bool {
        self.check(credential_id).is_none()
    }

    pub fn check(&self, credential_id: u64) -> Option<(CooldownReason, Duration)> {
        let mut entries = self.entries.lock();
        let now = Instant::now();
        match entries.get(&credential_id) {
            Some(entry) if entry.expires_at > now => Some((
                entry.reason,
                entry.expires_at.saturating_duration_since(now),
            )),
            Some(_) => {
                entries.remove(&credential_id);
                None
            }
            None => None,
        }
    }

    pub fn set(&self, credential_id: u64, reason: CooldownReason) -> Duration {
        let mut entries = self.entries.lock();
        let now = Instant::now();
        let entry = entries.entry(credential_id).or_insert(CooldownEntry {
            reason,
            expires_at: now,
            trigger_count: 0,
        });

        if entry.reason == reason {
            entry.trigger_count = entry.trigger_count.saturating_add(1);
        } else {
            entry.reason = reason;
            entry.trigger_count = 1;
        }

        let multiplier = 1.5_f64.powi(entry.trigger_count.saturating_sub(1) as i32);
        let duration = Duration::from_secs(
            ((reason.base_duration().as_secs() as f64) * multiplier) as u64,
        )
        .min(self.max_short_cooldown);
        entry.expires_at = now + duration;

        tracing::warn!(
            credential_id,
            reason = reason.description(),
            duration_secs = duration.as_secs(),
            "凭据进入临时冷却"
        );
        duration
    }

    pub fn clear(&self, credential_id: u64) {
        self.entries.lock().remove(&credential_id);
    }
}
