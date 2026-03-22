//! BM25 relevance scoring.
//!
//! Implements the Okapi BM25 formula with standard defaults (k1=1.2, b=0.75).
//! Scoring is computed at the row level at query time, not at the page level.

/// Default BM25 parameter k1 (term frequency saturation).
pub const K1: f64 = 1.2;

/// Default BM25 parameter b (document length normalization).
pub const B: f64 = 0.75;

/// Computes the IDF (inverse document frequency) component of BM25.
///
/// `doc_freq`: number of rows containing the term.
/// `total_rows`: total number of rows in the corpus (or segment).
///
/// Formula: `ln(1 + (N - df + 0.5) / (df + 0.5))`
#[must_use]
pub fn idf(doc_freq: u64, total_rows: u64) -> f64 {
    let n = total_rows as f64;
    let df = doc_freq as f64;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// Computes the BM25 score for a single term in a single row.
///
/// - `tf`: term frequency in this row
/// - `dl`: document length (total token count) of this row
/// - `avg_dl`: average document length across the corpus
/// - `doc_freq`: number of rows containing this term
/// - `total_rows`: total rows in the corpus
#[must_use]
pub fn score(tf: u32, dl: u32, avg_dl: f64, doc_freq: u64, total_rows: u64) -> f64 {
    let tf = tf as f64;
    let dl = dl as f64;
    let idf_val = idf(doc_freq, total_rows);
    idf_val * (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avg_dl))
}

/// Computes average document length from corpus statistics.
#[must_use]
pub fn avg_dl(total_tokens: u64, total_rows: u64) -> f64 {
    if total_rows == 0 {
        return 0.0;
    }
    total_tokens as f64 / total_rows as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f64, b: f64, epsilon: f64) -> bool {
        (a - b).abs() < epsilon
    }

    #[test]
    fn idf_rare_term() {
        // Term in 1 of 10000 rows → high IDF
        let result = idf(1, 10_000);
        assert!(result > 8.0, "rare term IDF should be high: {result}");
    }

    #[test]
    fn idf_common_term() {
        // Term in 5000 of 10000 rows → low IDF
        let result = idf(5000, 10_000);
        assert!(result < 1.0, "common term IDF should be low: {result}");
    }

    #[test]
    fn idf_all_rows() {
        // Term in every row → IDF near 0
        let result = idf(10_000, 10_000);
        assert!(result > 0.0, "IDF should be positive");
        assert!(
            result < 0.1,
            "universal term IDF should be near 0: {result}"
        );
    }

    #[test]
    fn idf_never_negative() {
        // With this formula, IDF is always positive
        for df in [1, 100, 999, 1000] {
            let result = idf(df, 1000);
            assert!(result > 0.0, "IDF should be positive for df={df}: {result}");
        }
    }

    #[test]
    fn score_basic() {
        // tf=3, dl=100, avg_dl=100, df=10, N=10000
        let s = score(3, 100, 100.0, 10, 10_000);
        assert!(s > 0.0);
        // With average-length doc, score is mainly driven by IDF and tf saturation
    }

    #[test]
    fn score_higher_tf_higher_score() {
        let s1 = score(1, 100, 100.0, 10, 10_000);
        let s2 = score(5, 100, 100.0, 10, 10_000);
        assert!(s2 > s1, "higher tf should give higher score");
    }

    #[test]
    fn score_shorter_doc_higher_score() {
        // Same tf in a shorter doc should score higher
        let s_short = score(2, 50, 100.0, 10, 10_000);
        let s_long = score(2, 200, 100.0, 10, 10_000);
        assert!(
            s_short > s_long,
            "shorter doc should score higher: {s_short} vs {s_long}"
        );
    }

    #[test]
    fn score_rarer_term_higher_score() {
        let s_rare = score(2, 100, 100.0, 5, 10_000);
        let s_common = score(2, 100, 100.0, 5000, 10_000);
        assert!(
            s_rare > s_common,
            "rarer term should score higher: {s_rare} vs {s_common}"
        );
    }

    #[test]
    fn avg_dl_basic() {
        assert!(approx_eq(avg_dl(50_000, 1000), 50.0, 1e-10));
    }

    #[test]
    fn avg_dl_zero_rows() {
        assert!(approx_eq(avg_dl(0, 0), 0.0, 1e-10));
    }

    // Golden test values cross-checked against a reference implementation.
    #[test]
    #[allow(clippy::approx_constant)]
    fn golden_scores() {
        struct Case {
            tf: u32,
            dl: u32,
            avg_dl: f64,
            df: u64,
            n: u64,
            expected: f64,
        }

        let cases = [
            Case {
                tf: 1,
                dl: 100,
                avg_dl: 100.0,
                df: 10,
                n: 10_000,
                expected: 6.859_065_109_813_038,
            },
            Case {
                tf: 3,
                dl: 100,
                avg_dl: 100.0,
                df: 10,
                n: 10_000,
                expected: 10.778_530_886_849_06,
            },
            Case {
                tf: 1,
                dl: 50,
                avg_dl: 100.0,
                df: 10,
                n: 10_000,
                expected: 8.622_824_709_479_248,
            },
            Case {
                tf: 1,
                dl: 200,
                avg_dl: 100.0,
                df: 10,
                n: 10_000,
                expected: 4.867_723_626_318_931,
            },
            Case {
                tf: 1,
                dl: 100,
                avg_dl: 100.0,
                df: 5000,
                n: 10_000,
                expected: 0.693_147_180_559_945_3,
            },
            Case {
                tf: 5,
                dl: 50,
                avg_dl: 200.0,
                df: 100,
                n: 1_000_000,
                expected: 18.327_401_291_422_82,
            },
        ];

        for (i, c) in cases.iter().enumerate() {
            let actual = score(c.tf, c.dl, c.avg_dl, c.df, c.n);
            assert!(
                approx_eq(actual, c.expected, 1e-6),
                "golden case {i}: expected {}, got {actual}",
                c.expected
            );
        }
    }
}
