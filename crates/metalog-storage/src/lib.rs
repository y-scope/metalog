mod backend;
mod errors;
mod filesystem;
mod registry;
mod s3;

pub use backend::Backend;
pub use errors::{OpError, StorageError};
pub use filesystem::FilesystemBackend;
pub use registry::Registry;
pub use s3::S3Backend;
