use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::OpenOptions,
    io::{self, Write as _},
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
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
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
const STATS_FILE: &str = "traffic-stats.json";
const STATS_BACKUP_PREFIX: &str = "traffic-stats.json.bak-";
/// Directory names used by this fork and by earlier Clash Verge installs.
const LEGACY_APP_DIR_NAMES: &[&str] = &[
    "io.github.clash-verge-rev.clash-verge-rev",
    "io.github.clash-verge-rev.clash-verge-rev.dev",
    "clash-verge-rev",
    "clash-verge",
    "Clash Verge",
    "Clash Verge Rev",
];
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
struct AppRecord {
    #[serde(default, alias = "proxyUp")]
    proxy_up: u64,
    #[serde(default, alias = "proxyDown")]
    proxy_down: u64,
    #[serde(default, alias = "directUp")]
    direct_up: u64,
    #[serde(default, alias = "directDown")]
    direct_down: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
struct DayRecord {
    #[serde(default, alias = "proxyUp")]
    proxy_up: u64,
    #[serde(default, alias = "proxyDown")]
    proxy_down: u64,
    #[serde(default, alias = "directUp")]
    direct_up: u64,
    #[serde(default, alias = "directDown")]
    direct_down: u64,
    #[serde(default)]
    apps: BTreeMap<String, AppRecord>,
    #[serde(default, alias = "capTripped")]
    cap_tripped: bool,
}

/// Canonical document. Unknown JSON fields are ignored here and kept on the raw value.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
struct StoreFile {
    #[serde(default, deserialize_with = "deserialize_version")]
    version: u32,
    #[serde(default)]
    days: BTreeMap<String, DayRecord>,
}

struct Ledger {
    days: BTreeMap<String, DayRecord>,
    seen: HashMap<String, (u64, u64)>,
    dirty: bool,
    loaded: bool,
    /// Original JSON object, so a save can put counters back without dropping unknown fields.
    preserved: Option<Value>,
    version: u32,
    /// A parse failure whose original bytes could not be copied aside. Do not replace that file.
    block_save: bool,
}

impl Ledger {
    fn empty() -> Self {
        Self {
            days: BTreeMap::new(),
            seen: HashMap::new(),
            dirty: false,
            loaded: false,
            preserved: None,
            version: STORE_VERSION,
            block_save: false,
        }
    }
}

#[derive(Debug)]
struct LoadedStore {
    days: BTreeMap<String, DayRecord>,
    preserved: Option<Value>,
    version: u32,
}

#[derive(Debug)]
struct LoadOutcome {
    days: BTreeMap<String, DayRecord>,
    preserved: Option<Value>,
    version: u32,
    migrated: bool,
    block_save: bool,
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
    // A newer writer may keep a different history. Dropping those days would be a downgrade.
    if ledger.version > STORE_VERSION || ledger.days.len() <= RETAINED_DAYS {
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
    let Ok(current) = stats_path() else {
        ledger.block_save = true;
        logging!(warn, Type::File, "traffic stats path is unavailable");
        return;
    };
    let legacy = legacy_stats_paths(&current);
    match prepare_store(&current, &legacy) {
        Ok(outcome) => {
            ledger.days = outcome.days;
            ledger.preserved = outcome.preserved;
            ledger.version = outcome.version;
            ledger.block_save = outcome.block_save;
            ledger.dirty = outcome.migrated;
        }
        Err(err) => {
            ledger.block_save = true;
            logging!(warn, Type::File, "traffic stats could not be read: {err}");
        }
    }
}

fn flush() {
    let mut ledger = LEDGER.lock();
    if !ledger.loaded || !ledger.dirty || ledger.block_save {
        return;
    }
    match save_ledger(&ledger) {
        Ok(()) => ledger.dirty = false,
        Err(err) => logging!(warn, Type::File, "traffic stats could not be saved: {err}"),
    }
}

fn stats_path() -> io::Result<PathBuf> {
    Ok(dirs::app_home_dir().map_err(io::Error::other)?.join(STATS_FILE))
}

fn save_ledger(ledger: &Ledger) -> io::Result<()> {
    let path = stats_path()?;
    let bytes = encode_store(&ledger.days, ledger.preserved.as_ref(), ledger.version)?;
    atomic_write(&path, &bytes)
}

fn prepare_store(current: &Path, legacy: &[PathBuf]) -> io::Result<LoadOutcome> {
    let mut outcome = load_stats(current, legacy)?;
    if outcome.migrated && !outcome.block_save {
        match encode_store(&outcome.days, outcome.preserved.as_ref(), outcome.version) {
            Ok(bytes) => {
                if atomic_write(current, &bytes).is_ok() {
                    outcome.migrated = false;
                } else {
                    logging!(warn, Type::File, "traffic stats migration could not be written yet");
                }
            }
            Err(err) => logging!(warn, Type::File, "traffic stats migration could not be encoded: {err}"),
        }
    }
    Ok(outcome)
}

fn load_stats(current: &Path, legacy: &[PathBuf]) -> io::Result<LoadOutcome> {
    match std::fs::read(current) {
        Ok(bytes) => Ok(load_existing(current, &bytes, legacy)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(load_missing(current, legacy)),
        Err(err) => Err(err),
    }
}

fn load_existing(current: &Path, bytes: &[u8], legacy: &[PathBuf]) -> LoadOutcome {
    match parse_store_bytes(bytes) {
        Ok(store) if !store.days.is_empty() => outcome_from(store, false, false),
        Ok(store) => fill_empty_store(current, store, legacy),
        Err(reason) => recover_unreadable(current, bytes, legacy, &reason),
    }
}

fn fill_empty_store(current: &Path, mut store: LoadedStore, legacy: &[PathBuf]) -> LoadOutcome {
    let Some(extra) = merge_sources(&source_paths(current, legacy)) else {
        return outcome_from(store, false, false);
    };
    if extra.days.is_empty() {
        return outcome_from(store, false, false);
    }
    merge_loaded(&mut store, extra);
    outcome_from(store, true, false)
}

fn recover_unreadable(current: &Path, bytes: &[u8], legacy: &[PathBuf], reason: &str) -> LoadOutcome {
    if let Err(err) = backup_original(current, bytes) {
        logging!(
            warn,
            Type::File,
            "traffic stats could not be parsed ({reason}) and the original could not be backed up ({err}); leaving it untouched"
        );
        return empty_outcome(true);
    }
    logging!(
        warn,
        Type::File,
        "traffic stats could not be parsed ({reason}); copied the original to a traffic-stats.json.bak file before continuing"
    );
    if let Some(store) = merge_sources(&source_paths(current, legacy))
        && (!store.days.is_empty() || store.preserved.is_some())
    {
        return outcome_from(store, true, false);
    }
    empty_outcome(false).with_migrated()
}

fn load_missing(current: &Path, legacy: &[PathBuf]) -> LoadOutcome {
    merge_sources(&source_paths(current, legacy))
        .map_or_else(|| empty_outcome(false), |store| outcome_from(store, true, false))
}

fn source_paths(current: &Path, legacy: &[PathBuf]) -> Vec<PathBuf> {
    let mut paths = recovery_paths(current);
    paths.extend(legacy.iter().cloned());
    paths.retain(|path| path != current);
    paths
}

fn outcome_from(store: LoadedStore, migrated: bool, block_save: bool) -> LoadOutcome {
    LoadOutcome {
        days: store.days,
        preserved: store.preserved,
        version: store.version,
        migrated,
        block_save,
    }
}

const fn empty_outcome(block_save: bool) -> LoadOutcome {
    LoadOutcome {
        days: BTreeMap::new(),
        preserved: None,
        version: STORE_VERSION,
        migrated: false,
        block_save,
    }
}

impl LoadOutcome {
    const fn with_migrated(mut self) -> Self {
        self.migrated = true;
        self
    }
}

fn backup_original(current: &Path, bytes: &[u8]) -> io::Result<PathBuf> {
    let dir = current
        .parent()
        .ok_or_else(|| io::Error::other("traffic stats path has no directory"))?;
    std::fs::create_dir_all(dir)?;
    let stamp = chrono::Local::now().format("%Y%m%d%H%M%S");
    for index in 0..100 {
        let name = backup_file_name(&stamp.to_string(), index);
        let dest = dir.join(name);
        match OpenOptions::new().write(true).create_new(true).open(&dest) {
            Ok(mut file) => {
                file.write_all(bytes)?;
                file.sync_all()?;
                return Ok(dest);
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::other("could not allocate a traffic stats backup name"))
}

fn backup_file_name(stamp: &str, index: u32) -> String {
    if index == 0 {
        format!("{STATS_BACKUP_PREFIX}{stamp}")
    } else {
        format!("{STATS_BACKUP_PREFIX}{stamp}-{index}")
    }
}

fn recovery_paths(current: &Path) -> Vec<PathBuf> {
    let Some(dir) = current.parent() else {
        return Vec::new();
    };
    let mut paths = vec![dir.join("traffic-stats.json.old")];
    let Ok(entries) = std::fs::read_dir(dir) else {
        return paths;
    };
    let mut backups: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(STATS_BACKUP_PREFIX))
        })
        .collect();
    backups.sort();
    backups.reverse();
    paths.extend(backups);
    paths
}

fn legacy_stats_paths(current: &Path) -> Vec<PathBuf> {
    candidate_files(&legacy_roots(), &portable_stats_paths(), current)
}

fn candidate_files(roots: &[PathBuf], portable: &[PathBuf], current: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for root in roots {
        for name in LEGACY_APP_DIR_NAMES {
            paths.push(root.join(name).join(STATS_FILE));
        }
    }
    paths.extend(portable.iter().cloned());
    paths.retain(|path| path != current);
    paths.sort();
    paths.dedup();
    paths
}

fn legacy_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    push_some(&mut roots, ::dirs::data_dir());
    push_some(&mut roots, ::dirs::data_local_dir());
    push_some(&mut roots, ::dirs::config_dir());
    push_some(&mut roots, ::dirs::config_local_dir());
    roots
}

fn portable_stats_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        paths.push(dir.join(STATS_FILE));
        paths.push(dir.join(".config").join(STATS_FILE));
        paths.push(dir.join(".config").join(dirs::APP_ID).join(STATS_FILE));
        paths.push(dir.join("data").join(STATS_FILE));
    }
    for name in ["Clash Verge", "Clash Verge Rev"] {
        push_env_stats(&mut paths, "LOCALAPPDATA", name);
        push_env_stats(&mut paths, "ProgramFiles", name);
        push_env_stats(&mut paths, "ProgramFiles(x86)", name);
    }
    for name in LEGACY_APP_DIR_NAMES {
        push_env_stats(&mut paths, "APPDATA", name);
        push_env_stats(&mut paths, "LOCALAPPDATA", name);
    }
    paths
}

fn push_some(paths: &mut Vec<PathBuf>, path: Option<PathBuf>) {
    if let Some(path) = path {
        paths.push(path);
    }
}

fn push_env_stats(paths: &mut Vec<PathBuf>, var: &str, name: &str) {
    let Ok(root) = std::env::var(var) else {
        return;
    };
    if root.is_empty() {
        return;
    }
    paths.push(PathBuf::from(root).join(name).join(STATS_FILE));
}

fn merge_sources(paths: &[PathBuf]) -> Option<LoadedStore> {
    let mut merged: Option<LoadedStore> = None;
    for path in paths {
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(store) = parse_store_bytes(&bytes) else {
            continue;
        };
        match merged.as_mut() {
            Some(base) => merge_loaded(base, store),
            None => merged = Some(store),
        }
    }
    merged
}

fn merge_loaded(base: &mut LoadedStore, extra: LoadedStore) {
    let extra_is_newer = extra.version > base.version;
    if extra_is_newer {
        base.version = extra.version;
        let previous = base.preserved.take();
        base.preserved = extra.preserved;
        copy_missing_keys(&mut base.preserved, previous);
    } else {
        copy_missing_keys(&mut base.preserved, extra.preserved);
    }
    for (day, record) in extra.days {
        merge_day(base.days.entry(day).or_default(), &record);
    }
}

fn copy_missing_keys(dest: &mut Option<Value>, src: Option<Value>) {
    let Some(Value::Object(src)) = src else {
        return;
    };
    if !dest.as_ref().is_some_and(Value::is_object) {
        *dest = Some(Value::Object(src));
        return;
    }
    let Some(Value::Object(dest_map)) = dest else {
        return;
    };
    for (key, value) in src {
        dest_map.entry(key).or_insert(value);
    }
}

fn merge_day(into: &mut DayRecord, extra: &DayRecord) {
    into.proxy_up = into.proxy_up.max(extra.proxy_up);
    into.proxy_down = into.proxy_down.max(extra.proxy_down);
    into.direct_up = into.direct_up.max(extra.direct_up);
    into.direct_down = into.direct_down.max(extra.direct_down);
    into.cap_tripped = into.cap_tripped || extra.cap_tripped;
    for (name, app) in &extra.apps {
        merge_app(into.apps.entry(name.clone()).or_default(), app);
    }
}

fn merge_app(into: &mut AppRecord, extra: &AppRecord) {
    into.proxy_up = into.proxy_up.max(extra.proxy_up);
    into.proxy_down = into.proxy_down.max(extra.proxy_down);
    into.direct_up = into.direct_up.max(extra.direct_up);
    into.direct_down = into.direct_down.max(extra.direct_down);
}

fn parse_store_bytes(bytes: &[u8]) -> Result<LoadedStore, String> {
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err("traffic stats file is empty".to_owned());
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|err| err.to_string())?;
    let Value::Object(map) = value else {
        return Err("traffic stats file is not a JSON object".to_owned());
    };
    interpret_object(map).ok_or_else(|| "traffic stats file is not a recognizable store".to_owned())
}

fn interpret_object(map: Map<String, Value>) -> Option<LoadedStore> {
    if map.contains_key("days") || map.contains_key("version") || map.contains_key("accounted") {
        return Some(store_from_document(map));
    }
    if !is_bare_day_map(&map) {
        return None;
    }
    let days = days_from_object(&map);
    let mut wrapped = Map::new();
    wrapped.insert("days".to_owned(), Value::Object(map));
    Some(LoadedStore {
        days,
        preserved: Some(Value::Object(wrapped)),
        version: 0,
    })
}

fn is_bare_day_map(map: &Map<String, Value>) -> bool {
    let mut saw_day = false;
    for key in map.keys() {
        if !is_date_key(key) {
            return false;
        }
        saw_day = true;
    }
    saw_day
}

fn store_from_document(mut map: Map<String, Value>) -> LoadedStore {
    let parsed = serde_json::from_value::<StoreFile>(Value::Object(map.clone())).ok();
    let version = parsed
        .as_ref()
        .map(|store| store.version)
        .or_else(|| map.get("version").and_then(version_of))
        .unwrap_or(0);
    let days = if let Some(value) = map.get("days") {
        days_from_parsed(value, parsed)
    } else {
        lift_top_level_dates(&mut map)
    };
    LoadedStore {
        days,
        preserved: Some(Value::Object(map)),
        version,
    }
}

fn days_from_parsed(days_value: &Value, parsed: Option<StoreFile>) -> BTreeMap<String, DayRecord> {
    let manual = days_from_value(days_value).unwrap_or_default();
    let Some(parsed) = parsed else {
        return manual;
    };
    let Some(object) = days_value.as_object() else {
        return manual;
    };
    if parsed.days.len() == object.len() {
        parsed.days
    } else {
        manual
    }
}

fn lift_top_level_dates(map: &mut Map<String, Value>) -> BTreeMap<String, DayRecord> {
    let dates: Vec<String> = map.keys().filter(|key| is_date_key(key)).cloned().collect();
    if dates.is_empty() {
        return BTreeMap::new();
    }
    let mut days_obj = Map::new();
    for key in dates {
        if let Some(value) = map.remove(&key) {
            days_obj.insert(key, value);
        }
    }
    let days = days_from_object(&days_obj);
    map.insert("days".to_owned(), Value::Object(days_obj));
    days
}

fn days_from_value(value: &Value) -> Option<BTreeMap<String, DayRecord>> {
    value.as_object().map(days_from_object)
}

fn days_from_object(map: &Map<String, Value>) -> BTreeMap<String, DayRecord> {
    let mut days = BTreeMap::new();
    for (key, value) in map {
        if let Some(day) = day_from_value(value) {
            days.insert(key.clone(), day);
        }
    }
    days
}

fn day_from_value(value: &Value) -> Option<DayRecord> {
    let obj = value.as_object()?;
    if let Ok(day) = serde_json::from_value::<DayRecord>(value.clone()) {
        return Some(day);
    }
    Some(DayRecord {
        proxy_up: u64_field(obj, &["proxy_up", "proxyUp"]),
        proxy_down: u64_field(obj, &["proxy_down", "proxyDown"]),
        direct_up: u64_field(obj, &["direct_up", "directUp"]),
        direct_down: u64_field(obj, &["direct_down", "directDown"]),
        apps: obj.get("apps").and_then(apps_from_value).unwrap_or_default(),
        cap_tripped: bool_field(obj, &["cap_tripped", "capTripped"]),
    })
}

fn apps_from_value(value: &Value) -> Option<BTreeMap<String, AppRecord>> {
    let map = value.as_object()?;
    let mut apps = BTreeMap::new();
    for (name, app_value) in map {
        if let Some(app) = app_from_value(app_value) {
            apps.insert(name.clone(), app);
        }
    }
    Some(apps)
}

fn app_from_value(value: &Value) -> Option<AppRecord> {
    let obj = value.as_object()?;
    if let Ok(app) = serde_json::from_value::<AppRecord>(value.clone()) {
        return Some(app);
    }
    Some(AppRecord {
        proxy_up: u64_field(obj, &["proxy_up", "proxyUp"]),
        proxy_down: u64_field(obj, &["proxy_down", "proxyDown"]),
        direct_up: u64_field(obj, &["direct_up", "directUp"]),
        direct_down: u64_field(obj, &["direct_down", "directDown"]),
    })
}

fn u64_field(obj: &Map<String, Value>, names: &[&str]) -> u64 {
    for name in names {
        if let Some(value) = obj.get(*name).and_then(value_u64) {
            return value;
        }
    }
    0
}

fn bool_field(obj: &Map<String, Value>, names: &[&str]) -> bool {
    for name in names {
        if let Some(value) = obj.get(*name).and_then(value_bool) {
            return value;
        }
    }
    false
}

fn value_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok())),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn value_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(number) => number.as_u64().map(|value| value != 0),
        Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn version_of(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        Value::String(text) => text.trim().trim_start_matches(['v', 'V']).parse().ok(),
        _ => None,
    }
}

fn is_date_key(key: &str) -> bool {
    NaiveDate::parse_from_str(key, "%Y-%m-%d").is_ok()
}

fn deserialize_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(version_of(&value).unwrap_or(0))
}

fn encode_store(days: &BTreeMap<String, DayRecord>, preserved: Option<&Value>, version: u32) -> io::Result<Vec<u8>> {
    let mut root = match preserved {
        Some(Value::Object(map)) => map.clone(),
        _ => Map::new(),
    };
    let stored_version = root.get("version").and_then(version_of).unwrap_or(0);
    let write_version = write_version(stored_version.max(version));
    root.insert("version".to_owned(), Value::from(write_version));
    overlay_days(&mut root, days, write_version <= STORE_VERSION);
    serde_json::to_vec_pretty(&Value::Object(root)).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

const fn write_version(version: u32) -> u32 {
    if version > STORE_VERSION {
        version
    } else {
        STORE_VERSION
    }
}

fn overlay_days(root: &mut Map<String, Value>, days: &BTreeMap<String, DayRecord>, authoritative: bool) {
    if root
        .get("days")
        .is_some_and(|value| !value.is_object() && !value.is_null())
    {
        return;
    }
    let mut days_obj = match root.remove("days") {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    if authoritative {
        days_obj.retain(|key, value| days.contains_key(key) || !value.is_object());
    }
    for (date, day) in days {
        let entry = days_obj
            .entry(date.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        overlay_day(entry, day);
    }
    root.insert("days".to_owned(), Value::Object(days_obj));
}

fn overlay_day(slot: &mut Value, day: &DayRecord) {
    if !slot.is_object() {
        *slot = Value::Object(Map::new());
    }
    let Some(obj) = slot.as_object_mut() else {
        return;
    };
    insert_u64(obj, "proxy_up", day.proxy_up);
    insert_u64(obj, "proxy_down", day.proxy_down);
    insert_u64(obj, "direct_up", day.direct_up);
    insert_u64(obj, "direct_down", day.direct_down);
    obj.insert("cap_tripped".to_owned(), Value::from(day.cap_tripped));
    overlay_apps(obj, &day.apps);
}

fn overlay_apps(obj: &mut Map<String, Value>, apps: &BTreeMap<String, AppRecord>) {
    if obj
        .get("apps")
        .is_some_and(|value| !value.is_object() && !value.is_null())
    {
        return;
    }
    let mut apps_obj = match obj.remove("apps") {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    apps_obj.retain(|key, value| apps.contains_key(key) || !value.is_object());
    for (name, app) in apps {
        let entry = apps_obj
            .entry(name.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        overlay_app(entry, app);
    }
    obj.insert("apps".to_owned(), Value::Object(apps_obj));
}

fn overlay_app(slot: &mut Value, app: &AppRecord) {
    if !slot.is_object() {
        *slot = Value::Object(Map::new());
    }
    let Some(obj) = slot.as_object_mut() else {
        return;
    };
    insert_u64(obj, "proxy_up", app.proxy_up);
    insert_u64(obj, "proxy_down", app.proxy_down);
    insert_u64(obj, "direct_up", app.direct_up);
    insert_u64(obj, "direct_down", app.direct_down);
}

fn insert_u64(obj: &mut Map<String, Value>, key: &str, value: u64) {
    obj.insert(key.to_owned(), Value::from(value));
}

fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_file_name("traffic-stats.json.tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    if let Err(err) = replace_file(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(err);
    }
    Ok(())
}

/// Windows refuses to rename onto an existing file, so move the destination aside first.
fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    match std::fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(_) if to.exists() => replace_existing(from, to),
        Err(err) => Err(err),
    }
}

fn replace_existing(from: &Path, to: &Path) -> io::Result<()> {
    let aside = to.with_file_name("traffic-stats.json.old");
    let _ = std::fs::remove_file(&aside);
    if let Err(err) = std::fs::rename(to, &aside) {
        let _ = std::fs::remove_file(from);
        return Err(err);
    }
    if let Err(err) = std::fs::rename(from, to) {
        let _ = std::fs::rename(&aside, to);
        return Err(err);
    }
    let _ = std::fs::remove_file(&aside);
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{
        LoadOutcome, LoadedStore, STATS_BACKUP_PREFIX, candidate_files, encode_store, parse_store_bytes, prepare_store,
        version_of,
    };
    use serde_json::Value;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "verge-traffic-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let created = fs::create_dir_all(&path);
            assert!(created.is_ok(), "{created:?}");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_bytes(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            let created = fs::create_dir_all(parent);
            assert!(created.is_ok(), "{created:?}");
        }
        let written = fs::write(path, bytes);
        assert!(written.is_ok(), "{written:?}");
    }

    fn read_bytes(path: &Path) -> Vec<u8> {
        let read = fs::read(path);
        assert!(read.is_ok(), "{read:?}");
        read.unwrap_or_default()
    }

    fn read_json(path: &Path) -> Value {
        let parsed = serde_json::from_slice::<Value>(&read_bytes(path));
        assert!(parsed.is_ok(), "{parsed:?}");
        parsed.unwrap_or(Value::Null)
    }

    fn must_parse(bytes: &[u8]) -> LoadedStore {
        let parsed = parse_store_bytes(bytes);
        assert!(parsed.is_ok(), "{parsed:?}");
        parsed.ok().unwrap_or_else(|| LoadedStore {
            days: Default::default(),
            preserved: None,
            version: 0,
        })
    }

    fn must_prepare(current: &Path, legacy: &[PathBuf]) -> LoadOutcome {
        let prepared = prepare_store(current, legacy);
        assert!(prepared.is_ok(), "{prepared:?}");
        prepared.ok().unwrap_or_else(|| LoadOutcome {
            days: Default::default(),
            preserved: None,
            version: 0,
            migrated: false,
            block_save: true,
        })
    }

    fn must_encode(store: &LoadedStore) -> Value {
        let encoded = encode_store(&store.days, store.preserved.as_ref(), store.version);
        assert!(encoded.is_ok(), "{encoded:?}");
        let bytes = encoded.unwrap_or_default();
        let parsed = serde_json::from_slice::<Value>(&bytes);
        assert!(parsed.is_ok(), "{parsed:?}");
        parsed.unwrap_or(Value::Null)
    }

    #[test]
    fn missing_and_unknown_fields_survive_a_save() {
        let raw = br#"{
            "version": 3,
            "future": {"keep": true},
            "days": {
                "2026-01-02": {
                    "proxy_up": 10,
                    "note": "keep-me",
                    "apps": {"chrome": {"proxy_down": 4, "label": "x"}}
                }
            }
        }"#;
        let store = must_parse(raw);
        let day = store.days.get("2026-01-02");
        assert_eq!(day.map(|day| day.proxy_up), Some(10));
        assert_eq!(day.map(|day| day.proxy_down), Some(0));
        assert_eq!(day.map(|day| day.direct_up), Some(0));
        assert_eq!(
            day.and_then(|day| day.apps.get("chrome")).map(|app| app.proxy_down),
            Some(4)
        );
        let saved = must_encode(&store);
        assert_eq!(saved.get("version").and_then(version_of), Some(3));
        assert_eq!(saved.pointer("/future/keep"), Some(&Value::Bool(true)));
        assert_eq!(
            saved.pointer("/days/2026-01-02/note"),
            Some(&Value::String("keep-me".to_owned()))
        );
        assert_eq!(saved.pointer("/days/2026-01-02/proxy_up"), Some(&Value::from(10)));
        assert_eq!(
            saved.pointer("/days/2026-01-02/apps/chrome/label"),
            Some(&Value::String("x".to_owned()))
        );
    }

    #[test]
    fn serde_defaults_accept_a_partial_document() {
        let parsed = serde_json::from_str::<super::StoreFile>(
            r#"{"extra":true,"version":"1","days":{"2026-01-02":{"proxyUp":4,"capTripped":true}}}"#,
        );
        assert!(parsed.is_ok(), "{parsed:?}");
        let Ok(store) = parsed else {
            return;
        };
        assert_eq!(store.version, 1);
        let day = store.days.get("2026-01-02");
        assert_eq!(day.map(|day| day.proxy_up), Some(4));
        assert_eq!(day.map(|day| day.direct_down), Some(0));
        assert_eq!(day.map(|day| day.cap_tripped), Some(true));
    }

    #[test]
    fn bare_day_map_and_string_counters_migrate() {
        let store = must_parse(
            br#"{"2026-03-04":{"proxyUp":"8","proxyDown":9,"directUp":1,"directDown":2,"capTripped":"true"}}"#,
        );
        let day = store.days.get("2026-03-04");
        assert_eq!(day.map(|day| day.proxy_up), Some(8));
        assert_eq!(day.map(|day| day.proxy_down), Some(9));
        assert_eq!(day.map(|day| day.direct_up), Some(1));
        assert_eq!(day.map(|day| day.direct_down), Some(2));
        assert_eq!(day.map(|day| day.cap_tripped), Some(true));
        let saved = must_encode(&store);
        assert_eq!(saved.get("version").and_then(version_of), Some(1));
        assert_eq!(saved.pointer("/days/2026-03-04/proxy_up"), Some(&Value::from(8)));
    }

    #[test]
    fn corrupt_file_is_backed_up_before_a_fresh_store_is_written() {
        let scratch = Scratch::new("corrupt");
        let current = scratch.path().join("traffic-stats.json");
        write_bytes(&current, b"{not-json");
        let outcome = must_prepare(&current, &[]);
        assert!(!outcome.block_save);
        assert!(outcome.days.is_empty());
        let saved = read_json(&current);
        assert_eq!(saved.get("version").and_then(version_of), Some(1));
        assert_eq!(
            saved.get("days").and_then(Value::as_object).map(serde_json::Map::len),
            Some(0)
        );
        let backups: Vec<PathBuf> = fs::read_dir(scratch.path())
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(STATS_BACKUP_PREFIX))
            })
            .collect();
        assert_eq!(backups.len(), 1);
        assert_eq!(read_bytes(&backups[0]), b"{not-json");
    }

    #[test]
    fn legacy_files_merge_when_the_current_file_is_absent_or_empty() {
        let scratch = Scratch::new("legacy");
        let current = scratch.path().join("current").join("traffic-stats.json");
        let older = scratch.path().join("roaming").join("traffic-stats.json");
        let local = scratch.path().join("local").join("traffic-stats.json");
        write_bytes(
            &older,
            br#"{"version":1,"days":{"2026-04-01":{"proxy_up":5,"proxy_down":1},"2026-04-02":{"proxy_up":2}}}"#,
        );
        write_bytes(
            &local,
            br#"{"version":1,"accounted":{"abc":[1,2]},"days":{"2026-04-01":{"proxy_up":9,"direct_down":4},"2026-04-03":{"proxy_up":7}}}"#,
        );
        let migrated = must_prepare(&current, &[older.clone(), local]);
        assert!(!migrated.block_save);
        let saved = read_json(&current);
        assert_eq!(saved.pointer("/days/2026-04-01/proxy_up"), Some(&Value::from(9)));
        assert_eq!(saved.pointer("/days/2026-04-01/proxy_down"), Some(&Value::from(1)));
        assert_eq!(saved.pointer("/days/2026-04-01/direct_down"), Some(&Value::from(4)));
        assert_eq!(saved.pointer("/days/2026-04-02/proxy_up"), Some(&Value::from(2)));
        assert_eq!(saved.pointer("/days/2026-04-03/proxy_up"), Some(&Value::from(7)));
        assert_eq!(
            saved.pointer("/accounted/abc"),
            Some(&Value::Array(vec![Value::from(1), Value::from(2)]))
        );

        write_bytes(&current, br#"{"version":1,"days":{}}"#);
        let filled = must_prepare(&current, std::slice::from_ref(&older));
        assert_eq!(
            read_json(&current).pointer("/days/2026-04-01/proxy_up"),
            Some(&Value::from(5))
        );
        assert!(!filled.days.is_empty());

        write_bytes(&current, br#"{"version":1,"days":{"2026-04-01":{"proxy_up":3}}}"#);
        let kept = must_prepare(&current, &[older]);
        assert_eq!(kept.days.get("2026-04-01").map(|day| day.proxy_up), Some(3));
        assert!(!kept.days.contains_key("2026-04-02"));
        assert_eq!(
            read_json(&current).pointer("/days/2026-04-01/proxy_up"),
            Some(&Value::from(3))
        );
    }

    #[test]
    fn crash_leftover_replaces_a_missing_current_file() {
        let scratch = Scratch::new("old");
        let current = scratch.path().join("traffic-stats.json");
        write_bytes(
            &scratch.path().join("traffic-stats.json.old"),
            br#"{"version":1,"days":{"2026-05-06":{"proxy_up":11}}}"#,
        );
        let outcome = must_prepare(&current, &[]);
        assert_eq!(outcome.days.get("2026-05-06").map(|day| day.proxy_up), Some(11));
        assert_eq!(
            read_json(&current).pointer("/days/2026-05-06/proxy_up"),
            Some(&Value::from(11))
        );
    }

    #[test]
    fn candidate_paths_cover_old_directories_without_repeating_the_current_file() {
        let current = PathBuf::from("/roaming/io.github.clash-verge-rev.clash-verge-rev/traffic-stats.json");
        let roots = vec![PathBuf::from("/roaming"), PathBuf::from("/local")];
        let portable = vec![PathBuf::from("/install/Clash Verge/traffic-stats.json")];
        let paths = candidate_files(&roots, &portable, &current);
        assert!(paths.contains(&portable[0]));
        assert!(paths.iter().any(|path| {
            path.ends_with("local/io.github.clash-verge-rev.clash-verge-rev/traffic-stats.json")
                || path.ends_with(r"local\io.github.clash-verge-rev.clash-verge-rev\traffic-stats.json")
        }));
        assert!(
            paths
                .iter()
                .any(|path| path.ends_with("clash-verge/traffic-stats.json"))
        );
        assert!(!paths.contains(&current));
        assert_eq!(
            paths.len(),
            paths.iter().collect::<std::collections::BTreeSet<_>>().len()
        );
    }
}
