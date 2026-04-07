mod batching_writer;
mod protoconv;
mod service;

pub use batching_writer::BatchingWriter;
pub use protoconv::convert_record;
pub use service::{IngestionResult, IngestionService};
