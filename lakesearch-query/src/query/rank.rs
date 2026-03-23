//! Stage 4: Ranking — sort by score and apply limits.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;

/// Concatenates, sorts by score descending, and limits batches.
pub(crate) fn rank_batches(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    with_score: bool,
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        return Ok(vec![]);
    }

    let combined = arrow::compute::concat_batches(schema, non_empty.iter().copied())
        .context("concatenating result batches")?;

    if combined.num_rows() == 0 {
        return Ok(vec![]);
    }

    let score_col_idx = if with_score {
        schema.column_with_name("score").map(|(idx, _)| idx)
    } else {
        None
    };

    let sorted = if let Some(idx) = score_col_idx {
        let sort_options = arrow::compute::SortOptions {
            descending: true,
            nulls_first: false,
        };
        let sort_col = arrow::compute::SortColumn {
            values: combined.column(idx).clone(),
            options: Some(sort_options),
        };
        let indices = arrow::compute::lexsort_to_indices(&[sort_col], limit)?;
        let columns: Vec<ArrayRef> = combined
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None).unwrap())
            .collect();
        RecordBatch::try_new(schema.clone(), columns).context("rebuilding sorted batch")?
    } else if let Some(lim) = limit {
        combined.slice(0, lim.min(combined.num_rows()))
    } else {
        combined
    };

    Ok(vec![sorted])
}
