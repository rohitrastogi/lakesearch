//! Segment file format: build and read.
//!
//! A segment is the core index artifact — a binary file containing an FST term
//! dictionary, posting lists, a doc table, a file table, and corpus stats.
//! See `DESIGN.md` for the full layout specification.

mod build;
mod read;

pub use build::SegmentBuilder;
pub use read::SegmentReader;

#[cfg(test)]
mod tests;
