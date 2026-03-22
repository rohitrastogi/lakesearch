//! Boolean set operations on sorted `DocId` arrays.
//!
//! All inputs must be sorted and deduplicated. All outputs are sorted and
//! deduplicated. These are the primitives for combining posting lists during
//! query evaluation.

use crate::types::DocId;

/// Returns the sorted intersection of two sorted `DocId` slices.
#[must_use]
pub fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut result = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// Returns the sorted union of two sorted `DocId` slices.
#[must_use]
pub fn union(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                result.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

/// Returns elements in `a` that are not in `b`. Both must be sorted.
#[must_use]
pub fn difference(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut result = Vec::with_capacity(a.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() {
        if j >= b.len() {
            result.extend_from_slice(&a[i..]);
            break;
        }
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                result.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersect_basic() {
        assert_eq!(intersect(&[1, 3, 5, 7], &[2, 3, 5, 8]), vec![3, 5]);
    }

    #[test]
    fn intersect_empty() {
        assert!(intersect(&[], &[1, 2, 3]).is_empty());
        assert!(intersect(&[1, 2, 3], &[]).is_empty());
    }

    #[test]
    fn intersect_disjoint() {
        assert!(intersect(&[1, 3, 5], &[2, 4, 6]).is_empty());
    }

    #[test]
    fn union_basic() {
        assert_eq!(union(&[1, 3, 5], &[2, 3, 6]), vec![1, 2, 3, 5, 6]);
    }

    #[test]
    fn union_empty() {
        assert_eq!(union(&[], &[1, 2]), vec![1, 2]);
        assert_eq!(union(&[1, 2], &[]), vec![1, 2]);
    }

    #[test]
    fn difference_basic() {
        assert_eq!(difference(&[1, 2, 3, 4, 5], &[2, 4]), vec![1, 3, 5]);
    }

    #[test]
    fn difference_empty_b() {
        assert_eq!(difference(&[1, 2, 3], &[]), vec![1, 2, 3]);
    }

    #[test]
    fn difference_empty_a() {
        assert!(difference(&[], &[1, 2, 3]).is_empty());
    }

    #[test]
    fn difference_identical() {
        assert!(difference(&[1, 2, 3], &[1, 2, 3]).is_empty());
    }

    mod proptest_boolean {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeSet;

        fn sorted_doc_ids(max_val: u32, max_len: usize) -> impl Strategy<Value = Vec<DocId>> {
            proptest::collection::vec(0u32..max_val, 0..max_len)
                .prop_map(|v| v.into_iter().collect::<BTreeSet<_>>().into_iter().collect())
        }

        proptest! {
            #[test]
            fn intersect_matches_btreeset(
                a in sorted_doc_ids(10_000, 500),
                b in sorted_doc_ids(10_000, 500)
            ) {
                let set_a: BTreeSet<DocId> = a.iter().copied().collect();
                let set_b: BTreeSet<DocId> = b.iter().copied().collect();
                let expected: Vec<DocId> = set_a.intersection(&set_b).copied().collect();
                prop_assert_eq!(intersect(&a, &b), expected);
            }

            #[test]
            fn union_matches_btreeset(
                a in sorted_doc_ids(10_000, 500),
                b in sorted_doc_ids(10_000, 500)
            ) {
                let set_a: BTreeSet<DocId> = a.iter().copied().collect();
                let set_b: BTreeSet<DocId> = b.iter().copied().collect();
                let expected: Vec<DocId> = set_a.union(&set_b).copied().collect();
                prop_assert_eq!(union(&a, &b), expected);
            }

            #[test]
            fn difference_matches_btreeset(
                a in sorted_doc_ids(10_000, 500),
                b in sorted_doc_ids(10_000, 500)
            ) {
                let set_a: BTreeSet<DocId> = a.iter().copied().collect();
                let set_b: BTreeSet<DocId> = b.iter().copied().collect();
                let expected: Vec<DocId> = set_a.difference(&set_b).copied().collect();
                prop_assert_eq!(difference(&a, &b), expected);
            }
        }
    }
}
