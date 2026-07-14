use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime::now < UNIX_EPOCH")
        .as_secs()
}
