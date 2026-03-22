//! Tokenizer for full-text indexing and query parsing.
//!
//! The `whitespace_lowercase` tokenizer splits text on non-alphanumeric
//! boundaries, lowercases, normalizes to NFC, and filters tokens that are
//! empty or exceed 256 bytes.

use unicode_normalization::UnicodeNormalization;

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
    // NFC normalize first so combining characters (e.g. e + \u{0301}) merge
    // into composed forms (é) before we split on non-alphanumeric boundaries.
    // ASCII text is already NFC, so skip the allocation in the common case.
    let owned;
    let normalized = if text.is_ascii() {
        text
    } else {
        owned = text.nfc().collect::<String>();
        &owned
    };

    let mut tokens = Vec::new();
    let mut start = None;

    for (i, ch) in normalized.char_indices() {
        if ch.is_alphanumeric() {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start.take() {
            push_token(&normalized[s..i], &mut tokens);
        }
    }
    // Handle trailing token
    if let Some(s) = start {
        push_token(&normalized[s..], &mut tokens);
    }
    tokens
}

fn push_token(raw: &str, tokens: &mut Vec<String>) {
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
