use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::worker::job::JobHandler;

/// A function registered on a `Worker`.
pub(crate) struct RegisteredHandler {
    /// Runs each job grabbed for the function.
    pub(crate) handler: Arc<dyn JobHandler>,
    /// Sent as `CAN_DO_TIMEOUT` when set: the server fails the job back if
    /// it isn't completed in time. `None` registers with plain `CAN_DO`.
    pub(crate) timeout: Option<Duration>,
}

/// Registered handlers by function name.
pub(crate) type Registry = HashMap<String, RegisteredHandler>;
