use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io,
    path::{Path, PathBuf},
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{Datelike as _, NaiveDate};
use clash_verge_logging::{Type, logging};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tauri_plugin_mihomo::models::{Connection, ConnectionMetaData, Connections};

use crate::{
    config::{Config, IVerge},
    core::{CoreManager, handle::Handle, manager::RunningMode},
    process::AsyncHandler,
    utils::{
        dirs,
        notification::{NotificationEvent, notify_event},
    },
};

const STORE_VERSION: u32 = 1;
const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const RETAINED_DAYS: usize = 400;
const MAX_TRACKED_APPS: usize = 200;
const SNAPSHOT_APP_LIMIT: usize = 40;
const UNKNOWN_APP: &str = "unknown";
const OTHER_APP: &str = "other";
const ERROR_LOG_INTERVAL: u64 = 60;

static LEDGER: LazyLock<Mutex<Ledger>> = LazyLock::new(|| Mutex::new(Ledger::empty()));
static STOP: AtomicBool = AtomicBool::new(false);
static LAST_SAMPLE_ERROR: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum Route {
    Proxy,
    Direct,
}

#[derive(Clone, Copy)]
enum PeriodKind {
    Today,
    Week,
    Month,
}

struct QuotaDecision {
    crossed: bool,
    over_limit: bool,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct AppRecord {
    proxy_up: u64,
    proxy_down: u64,
    direct_up: u64,
    direct_down: u64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct DayRecord {
    proxy_up: u64,
    proxy_down: u64,
    direct_up: u64,
    direct_down: u64,
    #[serde(default)]
    apps: BTreeMap<String, AppRecord>,
    #[serde(default)]
    cap_tripped: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    days: BTreeMap<String, DayRecord>,
}

struct Ledger {
    days: BTreeMap<String, DayRecord>,
    seen: HashMap<String, (u64, u64)>,
    dirty: bool,
    loaded: bool,
}

impl Ledger {
    fn empty() -> Self {
        Self {
            days: BTreeMap::new(),
            seen: HashMap::new(),
            dirty: false,
            loaded: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, specta::Type)]
pub struct TrafficAppUsage {
    pub name: String,
    pub proxy_up: u64,
    pub proxy_down: u64,
    pub direct_up: u64,
    pub direct_down: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, specta::Type)]
pub struct TrafficPeriodUsage {
    pub proxy_up: u64,
    pub proxy_down: u64,
    pub direct_up: u64,
    pub direct_down: u64,
    pub apps: Vec<TrafficAppUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, specta::Type)]
pub struct TrafficStats {
    pub today: TrafficPeriodUsage,
    pub week: TrafficPeriodUsage,
    pub month: TrafficPeriodUsage,
    pub quota_tripped: bool,
}

struct PeriodBuild {
    proxy_up: u64,
    proxy_down: u64,
    direct_up: u64,
    direct_down: u64,
    apps: BTreeMap<String, AppRecord>,
}

impl PeriodBuild {
    const fn empty() -> Self {
        Self {
            proxy_up: 0,
            proxy_down: 0,
            direct_up: 0,
            direct_down: 0,
            apps: BTreeMap::new(),
        }
    }
}

pub fn start() {
    AsyncHandler::spawn(|| async {
        {
            let mut ledger = LEDGER.lock();
            ensure_loaded(&mut ledger);
        }
        let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_flush = tokio::time::Instant::now();
        loop {
            ticker.tick().await;
            if stop_requested() {
                flush();
                break;
            }
            sample_once().await;
            if last_flush.elapsed() >= FLUSH_INTERVAL {
                flush();
                last_flush = tokio::time::Instant::now();
            }
        }
    });
}

pub fn flush_and_stop() {
    STOP.store(true, Ordering::Relaxed);
    flush();
}

pub fn snapshot() -> TrafficStats {
    let mut ledger = LEDGER.lock();
    ensure_loaded(&mut ledger);
    build_snapshot(&ledger)
}

fn stop_requested() -> bool {
    STOP.load(Ordering::Relaxed) || Handle::global().is_exiting()
}

async fn sample_once() {
    if stop_requested() {
        return;
    }
    if matches!(*CoreManager::global().get_running_mode(), RunningMode::NotRunning) {
        return;
    }

    let connections = match Handle::mihomo().get_connections().await {
        Ok(connections) => connections,
        Err(err) => {
            log_sample_error(&err);
            return;
        }
    };
    if stop_requested() {
        return;
    }

    let (quota_enabled, quota_limit) = quota_settings().await;
    let decision = {
        let mut ledger = LEDGER.lock();
        ensure_loaded(&mut ledger);
        apply_connections(&mut ledger, &connections);
        quota_decision(&mut ledger, quota_enabled, quota_limit)
    };

    if stop_requested() {
        return;
    }
    if decision.over_limit {
        turn_proxy_off().await;
    }
    if decision.crossed {
        notify_event(NotificationEvent::TrafficQuotaReached).await;
    }
}

async fn quota_settings() -> (bool, u64) {
    let verge = Config::verge().await;
    let latest = verge.latest_arc();
    (
        latest.traffic_quota_enable.unwrap_or(false),
        latest.traffic_quota_bytes.unwrap_or(0),
    )
}

async fn turn_proxy_off() {
    let verge = Config::verge().await;
    let latest = verge.latest_arc();
    let disable_system_proxy = latest.enable_system_proxy.unwrap_or(false);
    let disable_tun = latest.enable_tun_mode.unwrap_or(false);
    drop(latest);
    drop(verge);
    if !disable_system_proxy && !disable_tun {
        return;
    }

    let patch = IVerge {
        enable_system_proxy: disable_system_proxy.then_some(false),
        enable_tun_mode: disable_tun.then_some(false),
        ..IVerge::default()
    };
    if let Err(err) = super::patch_verge(&patch, false).await {
        logging!(
            error,
            Type::ProxyMode,
            "traffic quota could not turn proxy off: {err:#}"
        );
    }
}

fn apply_connections(ledger: &mut Ledger, connections: &Connections) {
    let live = connections.connections.as_deref().unwrap_or(&[]);
    let mut live_ids = HashSet::with_capacity(live.len());
    for connection in live {
        live_ids.insert(connection.id.clone());
        record_connection(ledger, connection);
    }
    ledger.seen.retain(|id, _| live_ids.contains(id));
    prune_days(ledger);
}

fn record_connection(ledger: &mut Ledger, connection: &Connection) {
    let current = (connection.upload, connection.download);
    // Cumulative counters include bytes from before this process observed the connection.
    let Some(previous) = ledger.seen.insert(connection.id.clone(), current) else {
        return;
    };
    if connection.upload < previous.0 || connection.download < previous.1 {
        return;
    }
    let up = connection.upload - previous.0;
    let down = connection.download - previous.1;
    if up == 0 && down == 0 {
        return;
    }
    let Some(route) = classify_route(&connection.chains) else {
        return;
    };
    add_traffic(ledger, &process_name(&connection.metadata), route, up, down);
}

fn classify_route(chains: &[String]) -> Option<Route> {
    // mihomo seeds the chain with the real outbound and appends group names
    // after it, so index 0 is DIRECT / REJECT / the proxy node.
    let outbound = chains.first()?.as_str();
    if outbound.eq_ignore_ascii_case("DIRECT") {
        Some(Route::Direct)
    } else if outbound.is_empty()
        || outbound.eq_ignore_ascii_case("REJECT")
        || outbound.eq_ignore_ascii_case("REJECT-DROP")
        || outbound.eq_ignore_ascii_case("PASS")
    {
        None
    } else {
        Some(Route::Proxy)
    }
}

fn process_name(metadata: &ConnectionMetaData) -> String {
    let process = metadata.process.trim();
    if !process.is_empty() {
        return process.to_owned();
    }
    let path = metadata.process_path.trim();
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(UNKNOWN_APP)
        .to_owned()
}

fn add_traffic(ledger: &mut Ledger, app: &str, route: Route, up: u64, down: u64) {
    let today = local_day_key();
    let day = ledger.days.entry(today).or_default();
    add_route_bytes(day, route, up, down);
    let key = tracked_app_key(day, app);
    let entry = day.apps.entry(key).or_default();
    add_app_bytes(entry, route, up, down);
    ledger.dirty = true;
}

fn tracked_app_key(day: &DayRecord, app: &str) -> String {
    if day.apps.contains_key(app) || day.apps.len() < MAX_TRACKED_APPS {
        app.to_owned()
    } else {
        OTHER_APP.to_owned()
    }
}

const fn add_route_bytes(day: &mut DayRecord, route: Route, up: u64, down: u64) {
    let (up_slot, down_slot) = match route {
        Route::Proxy => (&mut day.proxy_up, &mut day.proxy_down),
        Route::Direct => (&mut day.direct_up, &mut day.direct_down),
    };
    *up_slot = up_slot.saturating_add(up);
    *down_slot = down_slot.saturating_add(down);
}

const fn add_app_bytes(app: &mut AppRecord, route: Route, up: u64, down: u64) {
    let (up_slot, down_slot) = match route {
        Route::Proxy => (&mut app.proxy_up, &mut app.proxy_down),
        Route::Direct => (&mut app.direct_up, &mut app.direct_down),
    };
    *up_slot = up_slot.saturating_add(up);
    *down_slot = down_slot.saturating_add(down);
}

fn quota_decision(ledger: &mut Ledger, enabled: bool, limit: u64) -> QuotaDecision {
    let today = local_day_key();
    let Some(day) = ledger.days.get_mut(&today) else {
        return QuotaDecision {
            crossed: false,
            over_limit: false,
        };
    };
    let used = day.proxy_up.saturating_add(day.proxy_down);
    let over_limit = enabled && limit > 0 && used >= limit;
    if over_limit {
        let crossed = !day.cap_tripped;
        if crossed {
            day.cap_tripped = true;
            ledger.dirty = true;
        }
        return QuotaDecision {
            crossed,
            over_limit: true,
        };
    }
    if day.cap_tripped {
        day.cap_tripped = false;
        ledger.dirty = true;
    }
    QuotaDecision {
        crossed: false,
        over_limit: false,
    }
}

fn prune_days(ledger: &mut Ledger) {
    if ledger.days.len() <= RETAINED_DAYS {
        return;
    }
    let overflow = ledger.days.len() - RETAINED_DAYS;
    let stale: Vec<String> = ledger.days.keys().take(overflow).cloned().collect();
    for key in stale {
        ledger.days.remove(&key);
    }
}

fn build_snapshot(ledger: &Ledger) -> TrafficStats {
    let today = chrono::Local::now().date_naive();
    let mut today_usage = PeriodBuild::empty();
    let mut week_usage = PeriodBuild::empty();
    let mut month_usage = PeriodBuild::empty();
    for (key, day) in &ledger.days {
        if include_day(key, today, PeriodKind::Today) {
            accumulate(&mut today_usage, day);
        }
        if include_day(key, today, PeriodKind::Week) {
            accumulate(&mut week_usage, day);
        }
        if include_day(key, today, PeriodKind::Month) {
            accumulate(&mut month_usage, day);
        }
    }
    let quota_tripped = ledger.days.get(&local_day_key()).is_some_and(|day| day.cap_tripped);
    TrafficStats {
        today: finish_period(today_usage),
        week: finish_period(week_usage),
        month: finish_period(month_usage),
        quota_tripped,
    }
}

fn include_day(key: &str, today: NaiveDate, kind: PeriodKind) -> bool {
    let Ok(day) = NaiveDate::parse_from_str(key, "%Y-%m-%d") else {
        return false;
    };
    match kind {
        PeriodKind::Today => day == today,
        PeriodKind::Week => day.iso_week() == today.iso_week(),
        PeriodKind::Month => day.year() == today.year() && day.month() == today.month(),
    }
}

fn accumulate(period: &mut PeriodBuild, day: &DayRecord) {
    period.proxy_up = period.proxy_up.saturating_add(day.proxy_up);
    period.proxy_down = period.proxy_down.saturating_add(day.proxy_down);
    period.direct_up = period.direct_up.saturating_add(day.direct_up);
    period.direct_down = period.direct_down.saturating_add(day.direct_down);
    for (name, app) in &day.apps {
        let entry = period.apps.entry(name.clone()).or_default();
        entry.proxy_up = entry.proxy_up.saturating_add(app.proxy_up);
        entry.proxy_down = entry.proxy_down.saturating_add(app.proxy_down);
        entry.direct_up = entry.direct_up.saturating_add(app.direct_up);
        entry.direct_down = entry.direct_down.saturating_add(app.direct_down);
    }
}

fn finish_period(period: PeriodBuild) -> TrafficPeriodUsage {
    let mut apps: Vec<TrafficAppUsage> = period
        .apps
        .into_iter()
        .map(|(name, app)| TrafficAppUsage {
            name,
            proxy_up: app.proxy_up,
            proxy_down: app.proxy_down,
            direct_up: app.direct_up,
            direct_down: app.direct_down,
        })
        .collect();
    apps.sort_by(|left, right| {
        proxy_total(right)
            .cmp(&proxy_total(left))
            .then_with(|| left.name.cmp(&right.name))
    });
    apps.truncate(SNAPSHOT_APP_LIMIT);
    TrafficPeriodUsage {
        proxy_up: period.proxy_up,
        proxy_down: period.proxy_down,
        direct_up: period.direct_up,
        direct_down: period.direct_down,
        apps,
    }
}

const fn proxy_total(app: &TrafficAppUsage) -> u64 {
    app.proxy_up.saturating_add(app.proxy_down)
}

fn local_day_key() -> String {
    chrono::Local::now().format("%Y-%m-%d").to_string()
}

fn ensure_loaded(ledger: &mut Ledger) {
    if ledger.loaded {
        return;
    }
    ledger.loaded = true;
    match read_store() {
        Ok(store) => ledger.days = store.days,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            logging!(warn, Type::File, "traffic stats could not be read: {err}");
        }
    }
}

fn flush() {
    let mut ledger = LEDGER.lock();
    if !ledger.loaded || !ledger.dirty {
        return;
    }
    match save_store(&ledger.days) {
        Ok(()) => ledger.dirty = false,
        Err(err) => logging!(warn, Type::File, "traffic stats could not be saved: {err}"),
    }
}

fn stats_path() -> io::Result<PathBuf> {
    Ok(dirs::app_home_dir()
        .map_err(io::Error::other)?
        .join("traffic-stats.json"))
}

fn read_store() -> io::Result<StoreFile> {
    let bytes = std::fs::read(stats_path()?)?;
    serde_json::from_slice(&bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

fn save_store(days: &BTreeMap<String, DayRecord>) -> io::Result<()> {
    let path = stats_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec_pretty(&StoreFile {
        version: STORE_VERSION,
        days: days.clone(),
    })
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, payload)?;
    std::fs::rename(temporary, path)
}

fn log_sample_error(err: &impl std::fmt::Display) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let previous = LAST_SAMPLE_ERROR.load(Ordering::Relaxed);
    if now.saturating_sub(previous) < ERROR_LOG_INTERVAL {
        return;
    }
    LAST_SAMPLE_ERROR.store(now, Ordering::Relaxed);
    logging!(warn, Type::Core, "traffic stats sample failed: {err}");
}
