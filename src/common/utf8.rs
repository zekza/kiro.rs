//! UTF-8 安全字符串工具。

/// 安全截断 UTF-8 字符串，确保不会截断在多字节字符中间。
pub fn truncate_str_safe(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }

    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// 安全截断并在确实发生截断时追加省略号。
pub fn truncate_with_ellipsis(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    if max_bytes <= 3 {
        return truncate_str_safe(s, max_bytes).to_string();
    }
    format!("{}...", truncate_str_safe(s, max_bytes - 3))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_ascii() {
        assert_eq!(truncate_str_safe("abcdef", 3), "abc");
        assert_eq!(truncate_with_ellipsis("abcdef", 5), "ab...");
    }

    #[test]
    fn truncate_multibyte_at_boundary() {
        let value = "你好abc";
        assert_eq!(truncate_str_safe(value, 4), "你");
        assert_eq!(truncate_with_ellipsis(value, 7), "你...");
    }
}
