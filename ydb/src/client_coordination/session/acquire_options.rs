use crate::errors;
use derive_builder::Builder;
use std::time::Duration;

pub enum AcquireCount {
    Single,
    Exclusive,
    Custom(u64),
}

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
#[allow(dead_code)]
pub struct AcquireOptions {
    #[builder(setter(strip_option), default)]
    pub data: Option<Vec<u8>>,

    #[builder(default = "false")]
    pub(crate) ephemeral: bool,

    #[builder(default = "Duration::from_secs(20)")]
    pub(crate) timeout: Duration,
}
