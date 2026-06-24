//! The processor registry: the composed set of [`FacetProcessor`]s run over
//! each event.
//!
//! [`with_well_known`](ProcessorRegistry::with_well_known) builds the built-in
//! set in a deterministic order. Custom processors are added with
//! [`register`](ProcessorRegistry::register) before the projector is spawned.
//! Dispatch is a `Vec` loop — adding a facet is a new processor + one register
//! line, never an edit to a central match.

use super::RawEvent;
use super::mutation::Mutation;
use super::processor::FacetProcessor;
use super::processors::core::{DatasetRefProcessor, JobEdgeProcessor, RunStateProcessor};

pub struct ProcessorRegistry {
    processors: Vec<Box<dyn FacetProcessor>>,
}

impl ProcessorRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self {
            processors: Vec::new(),
        }
    }

    /// The built-in processors, in a deterministic order: namespaces/jobs/runs
    /// and the datasets they imply. (Facet-specific processors — schema, column
    /// lineage, sources, parent, tags — are added in later phases.)
    pub fn with_well_known() -> Self {
        let mut r = Self::new();
        r.register(Box::new(RunStateProcessor))
            .register(Box::new(JobEdgeProcessor))
            .register(Box::new(DatasetRefProcessor));
        r
    }

    /// Append a processor (e.g. a custom facet processor).
    pub fn register(&mut self, p: Box<dyn FacetProcessor>) -> &mut Self {
        self.processors.push(p);
        self
    }

    /// Run every processor over one event, collecting all mutations.
    pub fn process(&self, ev: &RawEvent) -> Vec<Mutation> {
        let mut out = Vec::new();
        for p in &self.processors {
            p.process(ev, &mut out);
        }
        out
    }
}

impl Default for ProcessorRegistry {
    fn default() -> Self {
        Self::with_well_known()
    }
}
