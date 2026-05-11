//! Prompt cache usage 本地模拟器
//!
//! Kiro 上游不直接返回 Anthropic prompt cache usage。本模块只在代理内按请求
//! prefix fingerprint 估算 `cache_read_input_tokens` / `cache_creation_input_tokens`，
//! 用于让 Claude Code/sub2api 看到接近 Anthropic 的 usage 字段。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::token::{
    count_message_content_tokens, count_system_message_tokens, count_tool_definition_tokens,
};

use super::types::{CacheControl, Message, MessagesRequest};

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);
const ONE_HOUR_CACHE_TTL: Duration = Duration::from_secs(3600);
const MAX_BREAKPOINTS: usize = 4;
const MAX_ENTRIES: usize = 100_000;
const PREFIX_LOOKBACK_LIMIT: usize = 20;
const GLOBAL_BUCKET_KEY: u64 = 0;

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheResult {
    pub cache_read_input_tokens: i32,
    pub cache_creation_input_tokens: i32,
    pub cache_creation_5m_input_tokens: i32,
    pub cache_creation_1h_input_tokens: i32,
    pub uncached_input_tokens: i32,
}

#[derive(Debug, Clone)]
pub struct CacheProfile {
    total_input_tokens: i32,
    min_cacheable_tokens: i32,
    blocks: Vec<CacheBlock>,
    breakpoints: Vec<CacheBreakpoint>,
    identity_key: Option<u64>,
    #[allow(dead_code)]
    binding_key: Option<u64>,
}

#[derive(Debug, Clone)]
struct CacheBlock {
    prefix_fingerprint: [u8; 32],
    cumulative_tokens: i32,
}

#[derive(Debug, Clone)]
struct CacheBreakpoint {
    block_index: usize,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    token_count: i32,
    ttl: Duration,
    expires_at: Instant,
    last_used_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheScope {
    Global,
    PerCredential,
}

impl CacheScope {
    fn as_u8(self) -> u8 {
        match self {
            Self::Global => 0,
            Self::PerCredential => 1,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::PerCredential,
            _ => Self::Global,
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "per_credential" | "percredential" => Self::PerCredential,
            _ => Self::Global,
        }
    }
}

pub struct CacheTracker {
    entries: Mutex<HashMap<u64, HashMap<[u8; 32], CacheEntry>>>,
    max_supported_ttl: Duration,
    scope: AtomicU8,
    cache_skip_rate: Mutex<Option<f32>>,
}

impl CacheTracker {
    pub fn new(
        max_supported_ttl: Duration,
        scope: CacheScope,
        cache_skip_rate: Option<f32>,
    ) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_supported_ttl,
            scope: AtomicU8::new(scope.as_u8()),
            cache_skip_rate: Mutex::new(cache_skip_rate.map(clamp_skip_rate)),
        }
    }

    fn cache_scope(&self) -> CacheScope {
        CacheScope::from_u8(self.scope.load(Ordering::Relaxed))
    }

    fn should_skip_lookup(&self) -> bool {
        let Some(rate) = *self.cache_skip_rate.lock() else {
            return false;
        };
        if rate <= 0.0 {
            false
        } else if rate >= 1.0 {
            true
        } else {
            fastrand::f32() < rate
        }
    }

    fn effective_bucket_key(&self, credential_id: u64, profile: &CacheProfile) -> u64 {
        let identity_key = profile.identity_key.unwrap_or(GLOBAL_BUCKET_KEY);
        match self.cache_scope() {
            CacheScope::Global => identity_key,
            CacheScope::PerCredential => {
                let mut hasher = Sha256::new();
                hasher.update(identity_key.to_be_bytes());
                hasher.update(credential_id.to_be_bytes());
                let hash: [u8; 32] = hasher.finalize().into();
                u64::from_be_bytes(hash[..8].try_into().unwrap_or([0; 8]))
            }
        }
    }

    pub fn build_profile(
        &self,
        payload: &MessagesRequest,
        total_input_tokens: i32,
    ) -> CacheProfile {
        let flattened = flatten_cacheable_blocks(payload);
        let request_prelude = canonicalize_json(serde_json::json!({
            "model": payload.model,
        }));
        let prelude_bytes = serde_json::to_vec(&request_prelude).unwrap_or_default();
        let mut prefix_hasher = Sha256::new();
        prefix_hasher.update((prelude_bytes.len() as u64).to_be_bytes());
        prefix_hasher.update(&prelude_bytes);

        let tools_extras = compute_segment_extras_hash(payload, BlockSegment::Tools);
        let system_extras = compute_segment_extras_hash(payload, BlockSegment::System);
        let messages_extras = compute_segment_extras_hash(payload, BlockSegment::Messages);

        let mut blocks = Vec::with_capacity(flattened.len());
        let mut breakpoints = Vec::new();
        let mut cumulative_tokens = 0i32;

        for (index, block) in flattened.into_iter().enumerate() {
            cumulative_tokens = cumulative_tokens.saturating_add(block.tokens);

            let block_bytes = serde_json::to_vec(&block.value).unwrap_or_default();
            let block_hash: [u8; 32] = Sha256::digest(&block_bytes).into();
            let mut next_prefix_hasher = prefix_hasher.clone();
            next_prefix_hasher.update(block_hash);
            let content_fingerprint: [u8; 32] = next_prefix_hasher.finalize().into();
            prefix_hasher = Sha256::new();
            prefix_hasher.update(content_fingerprint);

            let segment_extras = match block.segment {
                BlockSegment::Tools => &tools_extras,
                BlockSegment::System => &system_extras,
                BlockSegment::Messages => &messages_extras,
            };

            blocks.push(CacheBlock {
                prefix_fingerprint: mix_fingerprint(&content_fingerprint, segment_extras),
                cumulative_tokens,
            });

            if let Some(ttl) = block.breakpoint_ttl {
                breakpoints.push(CacheBreakpoint {
                    block_index: index,
                    ttl: ttl.min(self.max_supported_ttl),
                });
            }
        }

        if breakpoints.len() > MAX_BREAKPOINTS {
            tracing::warn!(
                breakpoint_count = breakpoints.len(),
                "cache_control breakpoint 超过 4 个，本地按无缓存处理"
            );
            breakpoints.clear();
        }

        if has_ttl_order_violation(&breakpoints) {
            tracing::warn!("cache_control TTL 顺序非法，本地按无缓存处理");
            breakpoints.clear();
        }

        CacheProfile {
            total_input_tokens: total_input_tokens.max(0),
            min_cacheable_tokens: minimum_cacheable_tokens_for_model(&payload.model),
            blocks,
            breakpoints,
            identity_key: extract_identity_key(payload),
            binding_key: extract_binding_key(payload),
        }
    }

    pub fn compute_and_update(&self, credential_id: u64, profile: &CacheProfile) -> CacheResult {
        let effective_id = self.effective_bucket_key(credential_id, profile);
        let Some(last_breakpoint) = profile.last_cacheable_breakpoint() else {
            return CacheResult {
                uncached_input_tokens: profile.total_input_tokens,
                ..Default::default()
            };
        };

        let last_breakpoint_tokens = last_breakpoint
            .cumulative_tokens
            .min(profile.total_input_tokens);
        let now = Instant::now();
        let mut all_entries = self.entries.lock();
        prune_expired(&mut all_entries, now);

        let mut matched_tokens = 0;
        let skipped_lookup = self.should_skip_lookup();

        if !skipped_lookup
            && let Some(bucket) = all_entries.get_mut(&effective_id)
        {
            let mut best: Option<(usize, [u8; 32], i32)> = None;
            for bp in profile.cacheable_breakpoints() {
                let mut scanned = 0usize;
                for idx in (0..=bp.block_index).rev() {
                    if scanned >= PREFIX_LOOKBACK_LIMIT {
                        break;
                    }
                    scanned += 1;
                    let block = &profile.blocks[idx];
                    let Some(entry) = bucket.get(&block.prefix_fingerprint) else {
                        continue;
                    };
                    if entry.expires_at <= now {
                        continue;
                    }
                    let candidate_tokens = block.cumulative_tokens.min(profile.total_input_tokens);
                    match best {
                        Some((_, _, existing)) if existing >= candidate_tokens => {}
                        _ => best = Some((idx, block.prefix_fingerprint, candidate_tokens)),
                    }
                    break;
                }
            }

            if let Some((_, fingerprint, cum_tokens)) = best {
                if let Some(entry) = bucket.get_mut(&fingerprint) {
                    entry.expires_at = now + entry.ttl;
                    entry.last_used_at = now;
                }
                matched_tokens = cum_tokens;
            }
        }

        let bucket = all_entries.entry(effective_id).or_default();
        for breakpoint in profile.cacheable_breakpoints() {
            let block = &profile.blocks[breakpoint.block_index];
            let next_expiry = now + breakpoint.ttl;
            match bucket.get_mut(&block.prefix_fingerprint) {
                Some(existing) => {
                    existing.token_count = existing.token_count.max(block.cumulative_tokens);
                    existing.ttl = breakpoint.ttl;
                    existing.expires_at = next_expiry;
                    existing.last_used_at = now;
                }
                None => {
                    bucket.insert(
                        block.prefix_fingerprint,
                        CacheEntry {
                            token_count: block.cumulative_tokens,
                            ttl: breakpoint.ttl,
                            expires_at: next_expiry,
                            last_used_at: now,
                        },
                    );
                }
            }
        }

        if bucket.len() > MAX_ENTRIES {
            let mut sorted: Vec<_> = bucket
                .iter()
                .map(|(k, v)| (*k, v.last_used_at))
                .collect();
            sorted.sort_by_key(|(_, last_used)| *last_used);
            for (key, _) in sorted.into_iter().take(bucket.len() - MAX_ENTRIES) {
                bucket.remove(&key);
            }
        }

        let cache_read = matched_tokens.max(0);
        let cache_creation = last_breakpoint_tokens.saturating_sub(matched_tokens).max(0);
        let (cache_5m, cache_1h) = compute_ttl_breakdown(profile, matched_tokens);
        let uncached = profile
            .total_input_tokens
            .saturating_sub(cache_read)
            .max(0);

        CacheResult {
            cache_read_input_tokens: cache_read,
            cache_creation_input_tokens: cache_creation,
            cache_creation_5m_input_tokens: cache_5m,
            cache_creation_1h_input_tokens: cache_1h,
            uncached_input_tokens: uncached,
        }
    }
}

fn clamp_skip_rate(rate: f32) -> f32 {
    if rate.is_nan() {
        0.0
    } else {
        rate.clamp(0.0, 1.0)
    }
}

fn has_ttl_order_violation(breakpoints: &[CacheBreakpoint]) -> bool {
    let mut seen_5m = false;
    for bp in breakpoints {
        if bp.ttl == ONE_HOUR_CACHE_TTL && seen_5m {
            return true;
        }
        if bp.ttl == DEFAULT_CACHE_TTL {
            seen_5m = true;
        }
    }
    false
}

fn compute_ttl_breakdown(profile: &CacheProfile, matched_tokens: i32) -> (i32, i32) {
    let mut five_min = 0i32;
    let mut one_hour = 0i32;
    let mut prev_cum = 0i32;

    for bp in profile.cacheable_breakpoints() {
        let cum = bp.cumulative_tokens.min(profile.total_input_tokens);
        if cum <= prev_cum {
            continue;
        }
        let segment_start = prev_cum.max(matched_tokens);
        let new_tokens = cum.saturating_sub(segment_start).max(0);
        if new_tokens > 0 {
            if bp.ttl == ONE_HOUR_CACHE_TTL {
                one_hour = one_hour.saturating_add(new_tokens);
            } else {
                five_min = five_min.saturating_add(new_tokens);
            }
        }
        prev_cum = cum;
    }

    (five_min, one_hour)
}

impl CacheProfile {
    fn cacheable_breakpoints(&self) -> Vec<ResolvedBreakpoint> {
        self.breakpoints
            .iter()
            .filter_map(|breakpoint| {
                let block = self.blocks.get(breakpoint.block_index)?;
                if block.cumulative_tokens < self.min_cacheable_tokens {
                    return None;
                }
                Some(ResolvedBreakpoint {
                    block_index: breakpoint.block_index,
                    cumulative_tokens: block.cumulative_tokens,
                    ttl: breakpoint.ttl,
                })
            })
            .collect()
    }

    fn last_cacheable_breakpoint(&self) -> Option<ResolvedBreakpoint> {
        self.cacheable_breakpoints().into_iter().last()
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedBreakpoint {
    block_index: usize,
    cumulative_tokens: i32,
    ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockSegment {
    Tools,
    System,
    Messages,
}

#[derive(Debug)]
struct PendingBlock {
    value: serde_json::Value,
    tokens: i32,
    breakpoint_ttl: Option<Duration>,
    segment: BlockSegment,
}

fn flatten_cacheable_blocks(payload: &MessagesRequest) -> Vec<PendingBlock> {
    let mut blocks = Vec::new();

    if let Some(tools) = &payload.tools {
        for (tool_index, tool) in tools.iter().enumerate() {
            let mut value = serde_json::to_value(tool).unwrap_or(serde_json::Value::Null);
            let breakpoint_ttl = extract_cache_ttl(&value);
            strip_cache_control(&mut value);
            blocks.push(PendingBlock {
                value: canonicalize_json(serde_json::json!({
                    "kind": "tool",
                    "tool_index": tool_index,
                    "tool": value,
                })),
                tokens: count_tool_definition_tokens(tool) as i32,
                breakpoint_ttl,
                segment: BlockSegment::Tools,
            });
        }
    }

    if let Some(system) = &payload.system {
        for (system_index, block) in system.iter().enumerate() {
            let mut value = serde_json::to_value(block).unwrap_or(serde_json::Value::Null);
            let breakpoint_ttl = extract_cache_ttl(&value);
            strip_cache_control(&mut value);
            strip_billing_header_line(&mut value);
            let tokens = value
                .get("text")
                .and_then(|v| v.as_str())
                .map(|t| crate::token::count_tokens(t) as i32)
                .unwrap_or_else(|| count_system_message_tokens(block) as i32);

            blocks.push(PendingBlock {
                value: canonicalize_json(serde_json::json!({
                    "kind": "system",
                    "system_index": system_index,
                    "block": value,
                })),
                tokens,
                breakpoint_ttl,
                segment: BlockSegment::System,
            });
        }
    }

    for (message_index, message) in payload.messages.iter().enumerate() {
        blocks.extend(flatten_message_blocks(message_index, message));
    }

    blocks
}

fn flatten_message_blocks(message_index: usize, message: &Message) -> Vec<PendingBlock> {
    match &message.content {
        serde_json::Value::String(text) => vec![build_message_block(
            message_index,
            &message.role,
            0,
            serde_json::json!({ "type": "text", "text": text }),
            None,
        )],
        serde_json::Value::Array(blocks) => blocks
            .iter()
            .enumerate()
            .map(|(block_index, block)| {
                let breakpoint_ttl = extract_cache_ttl(block);
                let mut normalized = block.clone();
                strip_cache_control(&mut normalized);
                build_message_block(
                    message_index,
                    &message.role,
                    block_index,
                    normalized,
                    breakpoint_ttl,
                )
            })
            .collect(),
        other => vec![build_message_block(
            message_index,
            &message.role,
            0,
            other.clone(),
            None,
        )],
    }
}

fn build_message_block(
    message_index: usize,
    role: &str,
    block_index: usize,
    block: serde_json::Value,
    breakpoint_ttl: Option<Duration>,
) -> PendingBlock {
    PendingBlock {
        tokens: count_message_content_tokens(&block) as i32,
        value: canonicalize_json(serde_json::json!({
            "kind": "message",
            "message_index": message_index,
            "role": role,
            "block_index": block_index,
            "block": block,
        })),
        breakpoint_ttl,
        segment: BlockSegment::Messages,
    }
}

fn extract_cache_ttl(value: &serde_json::Value) -> Option<Duration> {
    let cache_control = value.get("cache_control")?;
    let cache_control: CacheControl = serde_json::from_value(cache_control.clone()).ok()?;
    if cache_control.cache_type != "ephemeral" {
        return None;
    }

    if let Some(block_type) = value.get("type").and_then(|v| v.as_str()) {
        if block_type == "thinking" || block_type == "redacted_thinking" {
            return None;
        }
        if block_type == "text" {
            let text = value.get("text").and_then(|v| v.as_str()).unwrap_or("");
            if text.is_empty() {
                return None;
            }
        }
    }

    Some(match cache_control.ttl.as_deref() {
        Some("1h") => ONE_HOUR_CACHE_TTL,
        _ => DEFAULT_CACHE_TTL,
    })
}

fn strip_cache_control(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(arr) => {
            for item in arr {
                strip_cache_control(item);
            }
        }
        serde_json::Value::Object(map) => {
            map.remove("cache_control");
            for item in map.values_mut() {
                strip_cache_control(item);
            }
        }
        _ => {}
    }
}

fn strip_billing_header_line(value: &mut serde_json::Value) {
    if let Some(text) = value.get("text").and_then(|v| v.as_str()) {
        let filtered = text
            .lines()
            .filter(|line| !line.trim_start().starts_with("x-anthropic-billing-header:"))
            .collect::<Vec<_>>()
            .join("\n");
        if filtered.len() != text.len() {
            value["text"] = serde_json::Value::String(filtered);
        }
    }
}

fn compute_segment_extras_hash(payload: &MessagesRequest, segment: BlockSegment) -> [u8; 32] {
    let extras = match segment {
        BlockSegment::Tools => serde_json::Value::Null,
        BlockSegment::System => serde_json::json!({
            "output_config": payload.output_config,
        }),
        BlockSegment::Messages => serde_json::json!({
            "tool_choice": payload.tool_choice,
            "thinking": payload.thinking,
            "output_config": payload.output_config,
        }),
    };
    let bytes = serde_json::to_vec(&canonicalize_json(extras)).unwrap_or_default();
    Sha256::digest(&bytes).into()
}

pub fn extract_identity_key(payload: &MessagesRequest) -> Option<u64> {
    build_identity_str(payload, true).map(hash_to_u64)
}

pub fn extract_binding_key(payload: &MessagesRequest) -> Option<u64> {
    build_identity_str(payload, false).map(hash_to_u64)
}

fn build_identity_str(payload: &MessagesRequest, include_session: bool) -> Option<String> {
    let user_id = payload.metadata.as_ref()?.user_id.as_ref()?.trim();
    if user_id.is_empty() {
        return None;
    }

    let s = if let Ok(json) = serde_json::from_str::<serde_json::Value>(user_id) {
        let device_id = json.get("device_id").and_then(|v| v.as_str()).unwrap_or("");
        let account_uuid = json.get("account_uuid").and_then(|v| v.as_str()).unwrap_or("");
        if include_session {
            let session_id = json.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
            format!("{device_id}\x00{account_uuid}\x00{session_id}")
        } else {
            format!("{device_id}\x00{account_uuid}")
        }
    } else if include_session {
        user_id.to_string()
    } else {
        user_id
            .split_once("__session_")
            .map(|(prefix, _)| prefix.to_string())
            .unwrap_or_else(|| user_id.to_string())
    };
    Some(s)
}

fn hash_to_u64(s: String) -> u64 {
    let hash: [u8; 32] = Sha256::digest(s.as_bytes()).into();
    u64::from_be_bytes(hash[..8].try_into().unwrap_or([0; 8]))
}

fn mix_fingerprint(content: &[u8; 32], extras: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(content);
    hasher.update(extras);
    hasher.finalize().into()
}

fn minimum_cacheable_tokens_for_model(model: &str) -> i32 {
    let m = model.to_lowercase();
    if m.contains("opus-4-5")
        || m.contains("opus-4.5")
        || m.contains("opus-4-6")
        || m.contains("opus-4.6")
        || m.contains("opus-4-7")
        || m.contains("opus-4.7")
        || m.contains("haiku-4-5")
        || m.contains("haiku-4.5")
    {
        return 4096;
    }
    if m.contains("sonnet-4-6")
        || m.contains("sonnet-4.6")
        || m.contains("haiku-3-5")
        || m.contains("haiku-3.5")
    {
        return 2048;
    }
    if m.contains("haiku") {
        return 2048;
    }
    1024
}

fn prune_expired(entries: &mut HashMap<u64, HashMap<[u8; 32], CacheEntry>>, now: Instant) {
    entries.retain(|_, bucket| {
        bucket.retain(|_, entry| entry.expires_at > now);
        !bucket.is_empty()
    });
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(map) => {
            let ordered: BTreeMap<_, _> = map
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect();
            let mut out = serde_json::Map::new();
            for (key, value) in ordered {
                out.insert(key, value);
            }
            serde_json::Value::Object(out)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::types::{Message, SystemMessage};

    fn request_with_cache_marker() -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([{
                    "type": "text",
                    "text": "short user tail",
                    "cache_control": { "type": "ephemeral" }
                }]),
            }],
            stream: false,
            system: Some(vec![SystemMessage {
                block_type: Some("text".to_string()),
                text: "stable system prompt ".repeat(600),
                cache_control: None,
            }]),
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    #[test]
    fn identical_request_hits_cache_on_second_call() {
        let tracker = CacheTracker::new(Duration::from_secs(3600), CacheScope::Global, None);
        let req = request_with_cache_marker();
        let profile1 = tracker.build_profile(&req, 5000);
        let first = tracker.compute_and_update(1, &profile1);
        assert_eq!(first.cache_read_input_tokens, 0);
        assert!(first.cache_creation_input_tokens > 0);
        assert_eq!(first.uncached_input_tokens, 5000);

        let profile2 = tracker.build_profile(&req, 5000);
        let second = tracker.compute_and_update(1, &profile2);
        assert_eq!(
            second.cache_read_input_tokens,
            first.cache_creation_input_tokens
        );
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert_eq!(
            second.uncached_input_tokens,
            5000 - second.cache_read_input_tokens
        );
    }
}
