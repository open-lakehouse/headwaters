//! Output rendering: one [`Render`] implementation per command result, three
//! profiles selected by [`OutputMode`].
//!
//! - `table` — human-readable, for the terminal.
//! - `json` — the faithful wire message, a stable contract for scripts.
//! - `agent` — the same data interpreted and pruned for an LLM: known facets
//!   flattened, noise dropped, `_next` follow-up hints added.
//!
//! `json` and `agent` both emit a [`serde_json::Value`]; `table` writes text.

use std::io::Write;

pub mod facets;
pub mod nodeid;
pub mod table;

/// The output profile chosen by the global `-o/--output` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputMode {
    /// Human-readable tables and trees (default).
    Table,
    /// The faithful wire JSON — a stable contract for scripts.
    Json,
    /// Enriched, pruned JSON tuned for LLM agents.
    Agent,
}

/// Context passed to renderers (the flags that change how a result is shown).
#[derive(Debug, Clone, Copy)]
pub struct RenderCtx {
    /// Pass opaque facet bags through untouched instead of interpreting them.
    pub raw_facets: bool,
}

/// A command result that can be rendered in any [`OutputMode`].
pub trait Render {
    /// Write the human-readable form to `w`.
    fn table(&self, w: &mut dyn Write, ctx: RenderCtx) -> std::io::Result<()>;
    /// The faithful wire JSON.
    fn json(&self) -> serde_json::Value;
    /// The enriched/pruned JSON for agents. Defaults to [`Render::json`] for
    /// results that need no interpretation.
    fn agent(&self, _ctx: RenderCtx) -> serde_json::Value {
        self.json()
    }
}

/// Render `value` to stdout in the chosen `mode`.
pub fn emit(value: &dyn Render, mode: OutputMode, ctx: RenderCtx) -> std::io::Result<()> {
    let mut out = std::io::stdout();
    match mode {
        OutputMode::Table => value.table(&mut out, ctx)?,
        OutputMode::Json => {
            serde_json::to_writer_pretty(&mut out, &value.json())?;
            writeln!(out)?;
        }
        OutputMode::Agent => {
            serde_json::to_writer_pretty(&mut out, &value.agent(ctx))?;
            writeln!(out)?;
        }
    }
    Ok(())
}
