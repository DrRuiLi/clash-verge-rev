use crate::feat::traffic_quota::{self, TrafficStats};

#[tauri::command]
#[specta::specta]
pub fn get_traffic_stats() -> TrafficStats {
    traffic_quota::snapshot()
}
