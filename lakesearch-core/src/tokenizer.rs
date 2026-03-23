//! Tokenizer for full-text indexing and query parsing.
//!
//! The `whitespace_lowercase` tokenizer splits text on non-alphanumeric
//! boundaries, lowercases, normalizes to NFC, and filters tokens that are
//! empty or exceed 256 bytes.

use unicode_normalization::UnicodeNormalization;

/// The name of the default tokenizer, used in metadata.
pub const DEFAULT_TOKENIZER: &str = "whitespace_lowercase";

/// Maximum byte length of a single token.
const MAX_TOKEN_BYTES: usize = 256;

/// Tokenizes text using the `whitespace_lowercase` strategy.
///
/// 1. NFC normalize (so combining characters merge before splitting)
/// 2. Split on non-alphanumeric character boundaries
/// 3. Lowercase (Unicode-aware)
/// 4. Filter tokens that are empty or exceed 256 bytes
#[must_use]
pub fn tokenize(text: &str) -> Vec<String> {
    if text.is_ascii() {
        tokenize_ascii(text)
    } else {
        tokenize_unicode(text)
    }
}

/// Fast path for ASCII text: byte-level splitting and lowercasing with no
/// Unicode table lookups, no char decoding, and no per-token allocation
/// for already-lowercase tokens.
fn tokenize_ascii(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut start = None;

    for (i, &b) in bytes.iter().enumerate() {
        if b.is_ascii_alphanumeric() {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start.take() {
            let raw = &bytes[s..i];
            push_ascii_token(raw, &mut tokens);
        }
    }
    if let Some(s) = start {
        push_ascii_token(&bytes[s..], &mut tokens);
    }
    tokens
}

fn push_ascii_token(raw: &[u8], tokens: &mut Vec<String>) {
    if raw.is_empty() || raw.len() > MAX_TOKEN_BYTES {
        return;
    }
    // Check if already lowercase to avoid allocation
    if raw.iter().all(|b| !b.is_ascii_uppercase()) {
        // Safety: input is ASCII, so valid UTF-8
        tokens.push(unsafe { std::str::from_utf8_unchecked(raw) }.to_owned());
    } else {
        let mut lowered = Vec::with_capacity(raw.len());
        for &b in raw {
            lowered.push(b.to_ascii_lowercase());
        }
        // Safety: lowercasing ASCII bytes produces valid UTF-8
        tokens.push(unsafe { String::from_utf8_unchecked(lowered) });
    }
}

/// Unicode path: NFC normalize, split on non-alphanumeric char boundaries,
/// Unicode-aware lowercase.
fn tokenize_unicode(text: &str) -> Vec<String> {
    let normalized: String = text.nfc().collect();

    let mut tokens = Vec::new();
    let mut start = None;

    for (i, ch) in normalized.char_indices() {
        if ch.is_alphanumeric() {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start.take() {
            push_unicode_token(&normalized[s..i], &mut tokens);
        }
    }
    if let Some(s) = start {
        push_unicode_token(&normalized[s..], &mut tokens);
    }
    tokens
}

fn push_unicode_token(raw: &str, tokens: &mut Vec<String>) {
    let lowered: String = raw.chars().flat_map(char::to_lowercase).collect();
    if !lowered.is_empty() && lowered.len() <= MAX_TOKEN_BYTES {
        tokens.push(lowered);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_split_and_lowercase() {
        assert_eq!(tokenize("Hello World"), vec!["hello", "world"]);
    }

    #[test]
    fn punctuation_splits() {
        assert_eq!(
            tokenize("error: connection_refused (timeout)"),
            vec!["error", "connection", "refused", "timeout"]
        );
    }

    #[test]
    fn unicode_lowercase() {
        assert_eq!(tokenize("Ünïcödé"), vec!["ünïcödé"]);
    }

    #[test]
    fn empty_input() {
        let result: Vec<String> = tokenize("");
        assert!(result.is_empty());
    }

    #[test]
    fn only_punctuation() {
        let result: Vec<String> = tokenize("!@#$%^&*()");
        assert!(result.is_empty());
    }

    #[test]
    fn numbers_are_kept() {
        assert_eq!(tokenize("port 443"), vec!["port", "443"]);
    }

    #[test]
    fn mixed_alphanumeric() {
        assert_eq!(tokenize("http2 TLS1.3"), vec!["http2", "tls1", "3"]);
    }

    #[test]
    fn filters_oversized_tokens() {
        let long = "a".repeat(257);
        let input = format!("ok {} fine", long);
        assert_eq!(tokenize(&input), vec!["ok", "fine"]);
    }

    #[test]
    fn exactly_max_length_token_kept() {
        let token = "a".repeat(256);
        let input = format!("before {} after", token);
        let result = tokenize(&input);
        assert_eq!(result.len(), 3);
        assert_eq!(result[1], token);
    }

    #[test]
    fn nfc_normalization() {
        // é as e + combining acute (NFD) should normalize to single é (NFC)
        let nfd = "e\u{0301}";
        let result = tokenize(nfd);
        assert_eq!(result, vec!["\u{00e9}"]);
    }

    #[test]
    fn all_tokens_nonempty_and_lowercase() {
        let input = "The Quick BROWN fox Jumped OVER 42 lazy Dogs!";
        let tokens = tokenize(input);
        for t in &tokens {
            assert!(!t.is_empty());
            assert_eq!(*t, t.to_lowercase());
        }
    }
}
