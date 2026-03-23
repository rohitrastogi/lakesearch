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

/// A parsed query term with optional wildcard markers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryTerm {
    /// Exact term match.
    Exact(String),
    /// Prefix match: `conn*` → `Prefix("conn")`.
    Prefix(String),
    /// Suffix match: `*tion` → `Suffix("tion")`.
    Suffix(String),
}

/// Maximum number of terms a wildcard may expand to.
pub const MAX_WILDCARD_EXPANSION: usize = 1024;

/// Parses a query string into `QueryTerm`s.
///
/// Splits on whitespace, detects `*` wildcards at token boundaries,
/// and lowercases + NFC-normalizes the non-wildcard portion.
///
/// - `conn*` → `Prefix("conn")`
/// - `*tion` → `Suffix("tion")`
/// - `timeout` → `Exact("timeout")`
/// - `*` alone is ignored (matches everything, not useful)
#[must_use]
pub fn parse_query(text: &str) -> Vec<QueryTerm> {
    let mut terms = Vec::new();
    for raw in text.split_whitespace() {
        let is_prefix = raw.ends_with('*');
        let is_suffix = raw.starts_with('*');
        let stripped = raw.trim_matches('*');
        if stripped.is_empty() {
            continue;
        }
        // Tokenize the stripped portion (lowercases + normalizes)
        let tokens = tokenize(stripped);
        if tokens.is_empty() {
            continue;
        }
        // Use the first token (wildcard applies to a single term)
        let token = tokens.into_iter().next().expect("tokens is non-empty");
        let term = match (is_prefix, is_suffix) {
            (true, true) => {
                // *foo* — not supported, treat as prefix for now
                QueryTerm::Prefix(token)
            }
            (true, false) => QueryTerm::Prefix(token),
            (false, true) => QueryTerm::Suffix(token),
            (false, false) => QueryTerm::Exact(token),
        };
        terms.push(term);
    }
    terms
}

impl QueryTerm {
    /// Returns the inner term string regardless of variant.
    pub fn term(&self) -> &str {
        match self {
            Self::Exact(t) | Self::Prefix(t) | Self::Suffix(t) => t,
        }
    }

    /// Returns true if this is a wildcard (prefix or suffix) term.
    pub fn is_wildcard(&self) -> bool {
        !matches!(self, Self::Exact(_))
    }

    /// Returns true if a tokenized token matches this query term.
    pub fn matches_token(&self, token: &str) -> bool {
        match self {
            Self::Exact(q) => token == q,
            Self::Prefix(p) => token.starts_with(p.as_str()),
            Self::Suffix(s) => token.ends_with(s.as_str()),
        }
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

    #[test]
    fn parse_query_exact() {
        assert_eq!(
            parse_query("connection timeout"),
            vec![
                QueryTerm::Exact("connection".to_owned()),
                QueryTerm::Exact("timeout".to_owned()),
            ]
        );
    }

    #[test]
    fn parse_query_prefix() {
        assert_eq!(
            parse_query("conn*"),
            vec![QueryTerm::Prefix("conn".to_owned())]
        );
    }

    #[test]
    fn parse_query_suffix() {
        assert_eq!(
            parse_query("*tion"),
            vec![QueryTerm::Suffix("tion".to_owned())]
        );
    }

    #[test]
    fn parse_query_mixed() {
        assert_eq!(
            parse_query("conn* timeout *error"),
            vec![
                QueryTerm::Prefix("conn".to_owned()),
                QueryTerm::Exact("timeout".to_owned()),
                QueryTerm::Suffix("error".to_owned()),
            ]
        );
    }

    #[test]
    fn parse_query_lowercases() {
        assert_eq!(
            parse_query("CONN* *TION"),
            vec![
                QueryTerm::Prefix("conn".to_owned()),
                QueryTerm::Suffix("tion".to_owned()),
            ]
        );
    }

    #[test]
    fn parse_query_bare_star_ignored() {
        assert_eq!(parse_query("*"), Vec::<QueryTerm>::new());
        assert_eq!(
            parse_query("hello * world"),
            vec![
                QueryTerm::Exact("hello".to_owned()),
                QueryTerm::Exact("world".to_owned()),
            ]
        );
    }
}
