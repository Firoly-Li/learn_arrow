use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArrowError {
    #[error("Not found")]
    NotFound,
    #[error("out of max size for active file")]
    OutOfMaxSizeForActiveFile,
    #[error("create active file error")]
    CreateActiveFileError,
}
