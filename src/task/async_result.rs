/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct AsyncResult {
    pub task_id: String,
}

impl AsyncResult {
    pub fn new<S: Into<String>>(task_id: S) -> Self {
        Self {
            task_id: task_id.into(),
        }
    }
}
