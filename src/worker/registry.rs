use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::worker::job::JobHandler;

pub(crate) struct RegisteredHandler {
    pub(crate) handler: Arc<dyn JobHandler>,
    pub(crate) timeout: Option<Duration>,
}

pub(crate) type Registry = HashMap<String, RegisteredHandler>;
