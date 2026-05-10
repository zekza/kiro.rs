//! 轻量调用记录。
//!
//! 记录目标是排查 Claude Code/sub2api 侧中断、usage 和缓存观测问题。为降低泄露风险，
//! 请求和响应只记录截断体，且会递归脱敏常见凭据字段。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

use crate::anthropic::cache_tracker::CacheResult;
use crate::common::utf8::truncate_with_ellipsis;

#[derive(Debug, Clone)]
pub struct CallLogger {
    path: PathBuf,
    max_body_bytes: usize,
    write_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct CallLogRecord {
    pub route: &'static str,
    pub model: String,
    pub stream: bool,
    pub credential_id: Option<u64>,
    pub status: &'static str,
    pub http_status: Option<u16>,
    pub duration_ms: u128,
    pub input_tokens: i32,
    pub output_tokens: Option<i32>,
    pub cache: CacheResult,
    pub request_body: Option<String>,
    pub response_body: Option<String>,
    pub error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonRecord<'a> {
    timestamp_ms: u128,
    route: &'a str,
    model: &'a str,
    stream: bool,
    credential_id: Option<u64>,
    status: &'a str,
    http_status: Option<u16>,
    duration_ms: u128,
    input_tokens: i32,
    output_tokens: Option<i32>,
    cache_read_input_tokens: i32,
    cache_creation_input_tokens: i32,
    cache_creation_5m_input_tokens: i32,
    cache_creation_1h_input_tokens: i32,
    request_body: Option<String>,
    response_body: Option<String>,
    error: Option<String>,
}

impl CallLogger {
    pub fn new(path: PathBuf, max_body_bytes: usize) -> Self {
        Self {
            path,
            max_body_bytes: max_body_bytes.max(256),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn record(&self, record: CallLogRecord) {
        let json_record = JsonRecord {
            timestamp_ms: unix_millis(),
            route: record.route,
            model: &record.model,
            stream: record.stream,
            credential_id: record.credential_id,
            status: record.status,
            http_status: record.http_status,
            duration_ms: record.duration_ms,
            input_tokens: record.input_tokens,
            output_tokens: record.output_tokens,
            cache_read_input_tokens: record.cache.cache_read_input_tokens,
            cache_creation_input_tokens: record.cache.cache_creation_input_tokens,
            cache_creation_5m_input_tokens: record.cache.cache_creation_5m_input_tokens,
            cache_creation_1h_input_tokens: record.cache.cache_creation_1h_input_tokens,
            request_body: record
                .request_body
                .as_deref()
                .map(|body| sanitize_body(body, self.max_body_bytes)),
            response_body: record
                .response_body
                .as_deref()
                .map(|body| sanitize_body(body, self.max_body_bytes)),
            error: record
                .error
                .as_deref()
                .map(|e| truncate_with_ellipsis(e, self.max_body_bytes)),
        };

        let Ok(line) = serde_json::to_string(&json_record) else {
            return;
        };
        let path = self.path.clone();
        let lock = self.write_lock.clone();
        tokio::spawn(async move {
            if let Some(parent) = path.parent()
                && let Err(e) = tokio::fs::create_dir_all(parent).await
            {
                tracing::warn!("创建调用记录目录失败: {}", e);
                return;
            }

            let _guard = lock.lock();
            let write_result = tokio::task::block_in_place(|| {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)?;
                writeln!(file, "{}", line)
            });
            if let Err(e) = write_result {
                tracing::warn!("写入调用记录失败: {}", e);
            }
        });
    }
}

pub fn default_log_path(credentials_path: Option<PathBuf>) -> PathBuf {
    credentials_path
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("kiro_call_log.jsonl")
}

fn sanitize_body(body: &str, max_body_bytes: usize) -> String {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(mut value) => {
            redact_value(&mut value);
            let compact = serde_json::to_string(&value).unwrap_or_else(|_| body.to_string());
            truncate_with_ellipsis(&compact, max_body_bytes)
        }
        Err(_) => truncate_with_ellipsis(body, max_body_bytes),
    }
}

fn redact_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                let key_lc = key.to_ascii_lowercase();
                if key_lc.contains("token")
                    || key_lc.contains("apikey")
                    || key_lc.contains("api_key")
                    || key_lc.contains("authorization")
                    || key_lc.contains("password")
                    || key_lc.contains("secret")
                    || key_lc.contains("credential")
                {
                    *value = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_value(value);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_value(item);
            }
        }
        _ => {}
    }
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_redacts_nested_tokens_and_truncates_utf8_safely() {
        let body = r#"{"a":{"accessToken":"secret","text":"你好世界abcdef"}}"#;
        let sanitized = sanitize_body(body, 48);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("secret"));
        assert!(sanitized.is_char_boundary(sanitized.len()));
    }
}
