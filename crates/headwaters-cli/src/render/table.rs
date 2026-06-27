//! Shared helpers for building human-readable `table` output.

use comfy_table::{Cell, ContentArrangement, Table, presets};

/// A borderless table with the given header row, content-fit to the terminal.
pub fn new(headers: &[&str]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(presets::NOTHING)
        .set_content_arrangement(ContentArrangement::Dynamic);
    if !headers.is_empty() {
        table.set_header(headers.iter().map(Cell::new));
    }
    table
}
