//! Native async Gearman protocol client and worker, built on tokio.
//!
//! Implements the Gearman binary wire protocol directly (no dependency on
//! the C `libgearman`). See the `PROTOCOL` file in the gearmand C sources
//! for the canonical wire-format reference this crate was built against.

pub mod error;
pub mod protocol;

mod conn;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "worker")]
pub mod worker;

#[cfg(feature = "admin")]
pub mod admin;

pub use conn::Connection;
pub use error::{GearmanError, Result};
