use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use serde_json::Value;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAYS_IN_MONTH: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const DAYS_IN_MONTH_LEAP: [u32; 13] = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const DAYS_BEFORE_MONTH: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
const DAYS_BEFORE_MONTH_LEAP: [i64; 13] = [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

#[inline(always)]
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[inline(always)]
fn parse_csv_line<'a>(mut line: &'a [u8], expected_fields: usize) -> Option<Vec<&'a [u8]>> {
    if line.ends_with(b"\n") {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return None;
    }
    let mut fields = Vec::with_capacity(expected_fields);
    for part in line.split(|&b| b == b',') {
        if part.starts_with(b"\"") && part.ends_with(b"\"") && part.len() >= 2 {
            let inner = &part[1..part.len() - 1];
            if memchr::memchr(b'\"', inner).is_some() {
                return None;
            }
            fields.push(inner);
        } else if memchr::memchr(b'\"', part).is_some() {
            return None;
        } else {
            fields.push(part);
        }
    }
    if fields.len() == expected_fields {
        Some(fields)
    } else {
        None
    }
}

fn parse_iso_f1(s: &[u8]) -> Option<i64> {
    if s.len() != 20 || s[10] != b'T' || s[19] != b'Z' {
        return None;
    }
    for &idx in &[0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        if !s[idx].is_ascii_digit() {
            return None;
        }
    }
    if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = ((s[0] - b'0') as i64) * 1000
        + ((s[1] - b'0') as i64) * 100
        + ((s[2] - b'0') as i64) * 10
        + ((s[3] - b'0') as i64);
    let m = ((s[5] - b'0') as usize) * 10 + ((s[6] - b'0') as usize);
    let d = ((s[8] - b'0') as u32) * 10 + ((s[9] - b'0') as u32);
    let h = ((s[11] - b'0') as i64) * 10 + ((s[12] - b'0') as i64);
    let mi = ((s[14] - b'0') as i64) * 10 + ((s[15] - b'0') as i64);
    let se = ((s[17] - b'0') as i64) * 10 + ((s[18] - b'0') as i64);

    if m < 1 || m > 12 {
        return None;
    }
    let leap = is_leap_year(y);
    let dim = if leap {
        DAYS_IN_MONTH_LEAP[m]
    } else {
        DAYS_IN_MONTH[m]
    };
    if d < 1 || d > dim {
        return None;
    }
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }

    let leap_days = (y - 1969) / 4;
    let mut days = (y - 1970) * 365 + leap_days;
    days += if leap {
        DAYS_BEFORE_MONTH_LEAP[m]
    } else {
        DAYS_BEFORE_MONTH[m]
    } + (d as i64 - 1);

    let sec = days * 86400 + h * 3600 + mi * 60 + se;
    Some(sec * 1000)
}

fn parse_iso_f2(s: &[u8]) -> Option<i64> {
    if s.len() != 25 || s[10] != b'T' || (s[19] != b'+' && s[19] != b'-') || s[22] != b':' {
        return None;
    }
    for &idx in &[0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 23, 24] {
        if !s[idx].is_ascii_digit() {
            return None;
        }
    }
    if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = ((s[0] - b'0') as i64) * 1000
        + ((s[1] - b'0') as i64) * 100
        + ((s[2] - b'0') as i64) * 10
        + ((s[3] - b'0') as i64);
    let m = ((s[5] - b'0') as usize) * 10 + ((s[6] - b'0') as usize);
    let d = ((s[8] - b'0') as u32) * 10 + ((s[9] - b'0') as u32);
    let h = ((s[11] - b'0') as i64) * 10 + ((s[12] - b'0') as i64);
    let mi = ((s[14] - b'0') as i64) * 10 + ((s[15] - b'0') as i64);
    let se = ((s[17] - b'0') as i64) * 10 + ((s[18] - b'0') as i64);

    if m < 1 || m > 12 {
        return None;
    }
    let leap = is_leap_year(y);
    let dim = if leap {
        DAYS_IN_MONTH_LEAP[m]
    } else {
        DAYS_IN_MONTH[m]
    };
    if d < 1 || d > dim {
        return None;
    }
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }

    let off_h = ((s[20] - b'0') as i64) * 10 + ((s[21] - b'0') as i64);
    let off_m = ((s[23] - b'0') as i64) * 10 + ((s[24] - b'0') as i64);
    if off_h > 23 || off_m > 59 {
        return None;
    }

    let leap_days = (y - 1969) / 4;
    let mut days = (y - 1970) * 365 + leap_days;
    days += if leap {
        DAYS_BEFORE_MONTH_LEAP[m]
    } else {
        DAYS_BEFORE_MONTH[m]
    } + (d as i64 - 1);

    let sec = days * 86400 + h * 3600 + mi * 60 + se;
    let base_ms = sec * 1000;
    let offset_ms = (off_h * 3600 + off_m * 60) * 1000;
    Some(if s[19] == b'+' {
        base_ms - offset_ms
    } else {
        base_ms + offset_ms
    })
}

fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
    }
    if s.len() > 20 {
        return Some(i64::MAX);
    }
    let mut val: i64 = 0;
    for &b in s {
        val = val.saturating_mul(10).saturating_add((b - b'0') as i64);
    }
    Some(val)
}

fn parse_timestamp(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        None
    } else if s.ends_with(b"Z") {
        parse_iso_f1(s)
    } else if s.len() == 25 && (s[19] == b'+' || s[19] == b'-') {
        parse_iso_f2(s)
    } else if s[0].is_ascii_digit() {
        parse_f3(s)
    } else {
        None
    }
}

struct Dsu {
    names: Vec<String>,
    name_to_id: HashMap<String, u32>,
    parent: Vec<u32>,
    canonical: Vec<u32>,
}

impl Dsu {
    fn new() -> Self {
        Self {
            names: Vec::new(),
            name_to_id: HashMap::new(),
            parent: Vec::new(),
            canonical: Vec::new(),
        }
    }

    fn get_or_create(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.name_to_id.get(name) {
            id
        } else {
            let id = self.names.len() as u32;
            self.names.push(name.to_string());
            self.name_to_id.insert(name.to_string(), id);
            self.parent.push(id);
            self.canonical.push(id);
            id
        }
    }

    fn find(&mut self, mut x: u32) -> u32 {
        let mut path = Vec::new();
        while self.parent[x as usize] != x {
            path.push(x);
            x = self.parent[x as usize];
        }
        for node in path {
            self.parent[node as usize] = x;
        }
        x
    }

    fn union(&mut self, name_a: &str, name_b: &str) {
        let u = self.get_or_create(name_a);
        let v = self.get_or_create(name_b);
        let ru = self.find(u);
        let rv = self.find(v);
        if ru != rv {
            let cu = self.canonical[ru as usize];
            let cv = self.canonical[rv as usize];
            let min_c = if self.names[cu as usize] < self.names[cv as usize] {
                cu
            } else {
                cv
            };
            self.parent[rv as usize] = ru;
            self.canonical[ru as usize] = min_c;
        }
    }

    fn get_canonical<'a>(&'a mut self, name: &'a str) -> String {
        if let Some(&id) = self.name_to_id.get(name) {
            let root = self.find(id);
            let canon_id = self.canonical[root as usize];
            self.names[canon_id as usize].clone()
        } else {
            name.to_string()
        }
    }
}

fn list_csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if dir.exists() && dir.is_dir() {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_file() && p.extension().map_or(false, |ext| ext == "csv") {
                    files.push(p);
                }
            }
        }
    }
    files.sort();
    files
}

#[derive(Clone)]
struct RawEvent {
    user_id: String,
    ts_bytes: Vec<u8>,
    event_type: String,
    prod_id: String,
    price_bytes: Vec<u8>,
}

#[derive(Clone)]
struct ValidEvent {
    person_id: String,
    ts: i64,
    event_type: String,
    price_cents: i64,
    category: String,
}

#[derive(Clone)]
struct ValidClick {
    ts: i64,
    click_id: String,
    campaign_id: String,
}

struct UserDayAgg {
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    cat_spend: HashMap<String, i64>,
}

struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = std::env::var("DATA_DIR")
        .unwrap_or_else(|_| if args.len() > 1 { args[1].clone() } else { "/data".into() });
    let out_dir = std::env::var("OUTPUT_DIR")
        .unwrap_or_else(|_| if args.len() > 2 { args[2].clone() } else { "/output".into() });

    let data_path = Path::new(&data_dir);
    let out_path = Path::new(&out_dir);

    // 1. Read manifest.json
    let manifest_bytes = fs::read(data_path.join("manifest.json")).expect("Failed to read manifest.json");
    let manifest: Value = serde_json::from_slice(&manifest_bytes).expect("Failed to parse manifest.json");
    let window_start_str = manifest["window_start"].as_str().expect("window_start missing");
    let window_end_str = manifest["window_end"].as_str().expect("window_end missing");
    let window_start_ms = parse_timestamp(window_start_str.as_bytes()).expect("invalid window_start");
    let window_end_ms = parse_timestamp(window_end_str.as_bytes()).expect("invalid window_end");
    let click_window_start_ms = window_start_ms - 7 * 86400 * 1000;

    // 2. Read products.csv
    let mut products = HashMap::new();
    let prod_file = data_path.join("products.csv");
    if prod_file.exists() {
        let content = fs::read(&prod_file).unwrap_or_default();
        let mut lines = content.split(|&b| b == b'\n');
        lines.next();
        for line in lines {
            if let Some(fields) = parse_csv_line(line, 2) {
                let pid = String::from_utf8_lossy(fields[0]).to_string();
                let cat = String::from_utf8_lossy(fields[1]).to_string();
                products.insert(pid, cat);
            }
        }
    }

    // 3. Read campaigns.csv
    let mut campaigns = HashMap::new();
    let camp_file = data_path.join("campaigns.csv");
    if camp_file.exists() {
        let content = fs::read(&camp_file).unwrap_or_default();
        let mut lines = content.split(|&b| b == b'\n');
        lines.next();
        for line in lines {
            if let Some(fields) = parse_csv_line(line, 2) {
                let cid = String::from_utf8_lossy(fields[0]).to_string();
                let channel = String::from_utf8_lossy(fields[1]).to_string();
                campaigns.insert(cid, channel);
            }
        }
    }

    // 4. Read links/*.csv into DSU
    let mut dsu = Dsu::new();
    for lf in list_csv_files(&data_path.join("links")) {
        let content = fs::read(&lf).unwrap_or_default();
        let mut lines = content.split(|&b| b == b'\n');
        lines.next();
        for line in lines {
            if let Some(fields) = parse_csv_line(line, 2) {
                if !fields[0].is_empty() && !fields[1].is_empty() {
                    let a = String::from_utf8_lossy(fields[0]);
                    let b = String::from_utf8_lossy(fields[1]);
                    dsu.union(&a, &b);
                }
            }
        }
    }

    // 5. Ingest & Dedup Events in Parallel with Rayon
    let event_files = list_csv_files(&data_path.join("events"));
    let partial_events: Vec<HashMap<String, (i64, RawEvent)>> = event_files
        .par_iter()
        .map(|ef| {
            let mut map = HashMap::new();
            if let Ok(content) = fs::read(ef) {
                let mut lines = content.split(|&b| b == b'\n');
                lines.next();
                for line in lines {
                    if let Some(fields) = parse_csv_line(line, 7) {
                        if fields[0].is_empty() {
                            continue;
                        }
                        let eid = String::from_utf8_lossy(fields[0]).to_string();
                        if let Some(ingest_ms) = parse_iso_f1(fields[3]) {
                            let raw = RawEvent {
                                user_id: String::from_utf8_lossy(fields[1]).to_string(),
                                ts_bytes: fields[2].to_vec(),
                                event_type: String::from_utf8_lossy(fields[4]).to_string(),
                                prod_id: String::from_utf8_lossy(fields[5]).to_string(),
                                price_bytes: fields[6].to_vec(),
                            };
                            match map.get_mut(&eid) {
                                Some(existing) => {
                                    if ingest_ms < existing.0 {
                                        *existing = (ingest_ms, raw);
                                    }
                                }
                                None => {
                                    map.insert(eid, (ingest_ms, raw));
                                }
                            }
                        }
                    }
                }
            }
            map
        })
        .collect();

    let mut raw_events_by_id: HashMap<String, (i64, RawEvent)> = HashMap::new();
    for part in partial_events {
        for (eid, (ingest_ms, raw)) in part {
            match raw_events_by_id.get_mut(&eid) {
                Some(existing) => {
                    if ingest_ms < existing.0 {
                        *existing = (ingest_ms, raw);
                    }
                }
                None => {
                    raw_events_by_id.insert(eid, (ingest_ms, raw));
                }
            }
        }
    }

    // 6. Validate Kept Event Copies
    let mut valid_events: Vec<ValidEvent> = Vec::with_capacity(raw_events_by_id.len());
    for (_eid, (ingest_ms, raw)) in raw_events_by_id {
        if raw.user_id.is_empty() {
            continue;
        }
        let ts_ms = match parse_timestamp(&raw.ts_bytes) {
            Some(t) if t >= window_start_ms && t < window_end_ms => t,
            _ => continue,
        };
        if raw.event_type != "view" && raw.event_type != "cart" && raw.event_type != "purchase" {
            continue;
        }
        if raw.prod_id.is_empty() {
            continue;
        }

        let mut price_cents = 0;
        if raw.event_type == "purchase" {
            if raw.price_bytes.is_empty() {
                continue;
            }
            let mut is_digits = true;
            for &b in &raw.price_bytes {
                if !b.is_ascii_digit() {
                    is_digits = false;
                    break;
                }
            }
            if !is_digits {
                continue;
            }
            let mut p: i64 = 0;
            for &b in &raw.price_bytes {
                p = p.saturating_mul(10).saturating_add((b - b'0') as i64);
            }
            if p < 1 || p > 10_000_000 {
                continue;
            }
            price_cents = p;
        }

        if ingest_ms - ts_ms > 86_400_000 {
            continue;
        }

        let person_id = dsu.get_canonical(&raw.user_id);
        let category = products.get(&raw.prod_id).cloned().unwrap_or_else(|| "unknown".into());

        valid_events.push(ValidEvent {
            person_id,
            ts: ts_ms,
            event_type: raw.event_type,
            price_cents,
            category,
        });
    }

    // 7. Ingest & Dedup Clicks in Parallel with Rayon
    let click_files = list_csv_files(&data_path.join("clicks"));
    let partial_clicks: Vec<HashMap<String, (String, String, Vec<u8>)>> = click_files
        .par_iter()
        .map(|cf| {
            let mut map = HashMap::new();
            if let Ok(content) = fs::read(cf) {
                let mut lines = content.split(|&b| b == b'\n');
                lines.next();
                for line in lines {
                    if let Some(fields) = parse_csv_line(line, 4) {
                        if fields[0].is_empty() {
                            continue;
                        }
                        let cid = String::from_utf8_lossy(fields[0]).to_string();
                        if !map.contains_key(&cid) {
                            let uid = String::from_utf8_lossy(fields[1]).to_string();
                            let camp = String::from_utf8_lossy(fields[2]).to_string();
                            map.insert(cid, (uid, camp, fields[3].to_vec()));
                        }
                    }
                }
            }
            map
        })
        .collect();

    let mut raw_clicks_by_id: HashMap<String, (String, String, Vec<u8>)> = HashMap::new();
    for part in partial_clicks {
        for (cid, val) in part {
            raw_clicks_by_id.entry(cid).or_insert(val);
        }
    }

    // 8. Validate Clicks & Group by Person
    let mut clicks_by_person: HashMap<String, Vec<ValidClick>> = HashMap::new();
    for (cid, (uid, camp, ts_bytes)) in raw_clicks_by_id {
        if uid.is_empty() {
            continue;
        }
        let ts_ms = match parse_timestamp(&ts_bytes) {
            Some(t) if t >= click_window_start_ms && t < window_end_ms => t,
            _ => continue,
        };
        if !campaigns.contains_key(&camp) {
            continue;
        }

        let person_id = dsu.get_canonical(&uid);
        clicks_by_person
            .entry(person_id)
            .or_default()
            .push(ValidClick {
                ts: ts_ms,
                click_id: cid,
                campaign_id: camp,
            });
    }

    for clicks in clicks_by_person.values_mut() {
        clicks.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.click_id.cmp(&b.click_id)));
    }

    // 9. Sessionize Events & Aggregate in Parallel with Rayon
    let mut events_by_person: HashMap<String, Vec<ValidEvent>> = HashMap::new();
    for ev in valid_events {
        events_by_person.entry(ev.person_id.clone()).or_default().push(ev);
    }

    let session_results: Vec<(
        Vec<((String, i32), (i64, i64, i64, HashMap<String, i64>))>,
        Vec<((String, i32), (i64, i64, i64, i64))>,
    )> = events_by_person
        .into_par_iter()
        .map(|(person_id, mut ev_list)| {
            ev_list.sort_unstable_by_key(|e| e.ts);

            let mut sessions: Vec<Vec<ValidEvent>> = Vec::new();
            let mut curr_sess: Vec<ValidEvent> = Vec::new();
            let mut last_distinct_ts: Option<i64> = None;

            for ev in ev_list {
                let ts = ev.ts;
                if curr_sess.is_empty() {
                    curr_sess.push(ev);
                    last_distinct_ts = Some(ts);
                } else if Some(ts) == last_distinct_ts {
                    curr_sess.push(ev);
                } else if ts - last_distinct_ts.unwrap() > 1_800_000 {
                    sessions.push(std::mem::take(&mut curr_sess));
                    curr_sess.push(ev);
                    last_distinct_ts = Some(ts);
                } else {
                    curr_sess.push(ev);
                    last_distinct_ts = Some(ts);
                }
            }
            if !curr_sess.is_empty() {
                sessions.push(curr_sess);
            }

            let p_clicks = clicks_by_person.get(&person_id);
            let mut local_user_daily = Vec::new();
            let mut local_camp_events = Vec::new();

            for sess in sessions {
                let sess_start_ms = sess[0].ts;
                let sess_day = (sess_start_ms / 86_400_000) as i32;

                let sess_events_count = sess.len() as i64;
                let mut sess_revenue = 0i64;
                let mut sess_purchases = 0i64;
                let mut sess_cat_spend: HashMap<String, i64> = HashMap::new();

                for ev in &sess {
                    if ev.event_type == "purchase" {
                        sess_revenue += ev.price_cents;
                        sess_purchases += 1;
                        *sess_cat_spend.entry(ev.category.clone()).or_default() += ev.price_cents;
                    }
                }

                local_user_daily.push((
                    (person_id.clone(), sess_day),
                    (1i64, sess_events_count, sess_revenue, sess_cat_spend),
                ));

                // Attribution
                let min_click_ts = sess_start_ms - 604_800_000;
                let mut eligible_clicks: Vec<&ValidClick> = Vec::new();
                if let Some(clicks) = p_clicks {
                    for c in clicks {
                        if c.ts >= min_click_ts && c.ts <= sess_start_ms {
                            eligible_clicks.push(c);
                        }
                    }
                }

                let mut credited_camp = "organic".to_string();
                if !eligible_clicks.is_empty() {
                    let best = eligible_clicks
                        .iter()
                        .min_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.click_id.cmp(&b.click_id)))
                        .unwrap();
                    credited_camp = best.campaign_id.clone();
                }

                let mut assisting_camps: HashSet<String> = HashSet::new();
                for c in eligible_clicks {
                    assisting_camps.insert(c.campaign_id.clone());
                }

                let purch = if sess_purchases > 0 { 1i64 } else { 0i64 };
                local_camp_events.push(((credited_camp, sess_day), (1i64, purch, sess_revenue, 0i64)));

                for acamp in assisting_camps {
                    local_camp_events.push(((acamp, sess_day), (0i64, 0i64, 0i64, 1i64)));
                }
            }

            (local_user_daily, local_camp_events)
        })
        .collect();

    let mut user_daily_map: HashMap<(String, i32), UserDayAgg> = HashMap::new();
    let mut campaign_daily_map: HashMap<(String, i32), CampDayAgg> = HashMap::new();

    for (user_records, camp_records) in session_results {
        for (ukey, (sess_cnt, ev_cnt, rev, cat_sp)) in user_records {
            let uentry = user_daily_map.entry(ukey).or_insert_with(|| UserDayAgg {
                sessions: 0,
                events: 0,
                revenue_cents: 0,
                cat_spend: HashMap::new(),
            });
            uentry.sessions += sess_cnt;
            uentry.events += ev_cnt;
            uentry.revenue_cents += rev;
            for (cat, sp) in cat_sp {
                *uentry.cat_spend.entry(cat).or_default() += sp;
            }
        }

        for (ckey, (sess_cnt, purch_cnt, rev, assist_cnt)) in camp_records {
            let centry = campaign_daily_map.entry(ckey).or_insert_with(|| CampDayAgg {
                sessions: 0,
                purchasing_sessions: 0,
                revenue_cents: 0,
                assisted_sessions: 0,
            });
            centry.sessions += sess_cnt;
            centry.purchasing_sessions += purch_cnt;
            centry.revenue_cents += rev;
            centry.assisted_sessions += assist_cnt;
        }
    }

    // 10. Write user_daily Parquet
    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    let mut u_user_ids = Vec::with_capacity(user_daily_map.len());
    let mut u_days = Vec::with_capacity(user_daily_map.len());
    let mut u_sessions = Vec::with_capacity(user_daily_map.len());
    let mut u_events = Vec::with_capacity(user_daily_map.len());
    let mut u_revenue = Vec::with_capacity(user_daily_map.len());
    let mut u_top_cat: Vec<Option<String>> = Vec::with_capacity(user_daily_map.len());

    for ((pid, s_day), udata) in user_daily_map {
        let mut top_cat: Option<String> = None;
        if udata.revenue_cents > 0 {
            let mut cats: Vec<(String, i64)> = udata.cat_spend.into_iter().collect();
            cats.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            top_cat = Some(cats[0].0.clone());
        }
        u_user_ids.push(pid);
        u_days.push(s_day);
        u_sessions.push(udata.sessions);
        u_events.push(udata.events);
        u_revenue.push(udata.revenue_cents);
        u_top_cat.push(top_cat);
    }

    let u_batch = RecordBatch::try_new(
        user_schema.clone(),
        vec![
            Arc::new(StringArray::from(u_user_ids)),
            Arc::new(Date32Array::from(u_days)),
            Arc::new(Int64Array::from(u_sessions)),
            Arc::new(Int64Array::from(u_events)),
            Arc::new(Int64Array::from(u_revenue)),
            Arc::new(StringArray::from(
                u_top_cat.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("Failed to create user_daily batch");

    let u_out_folder = out_path.join("user_daily");
    fs::create_dir_all(&u_out_folder).expect("Failed to create user_daily dir");
    let u_file = File::create(u_out_folder.join("part-0.parquet")).expect("Failed to create user parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut u_writer = ArrowWriter::try_new(u_file, user_schema, Some(props)).expect("Failed to init user writer");
    u_writer.write(&u_batch).expect("Failed to write user batch");
    u_writer.close().expect("Failed to close user writer");

    // 11. Write campaign_daily Parquet
    let camp_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));

    let mut c_camp_ids = Vec::with_capacity(campaign_daily_map.len());
    let mut c_channels = Vec::with_capacity(campaign_daily_map.len());
    let mut c_days = Vec::with_capacity(campaign_daily_map.len());
    let mut c_sessions = Vec::with_capacity(campaign_daily_map.len());
    let mut c_purchasing = Vec::with_capacity(campaign_daily_map.len());
    let mut c_revenue = Vec::with_capacity(campaign_daily_map.len());
    let mut c_assisted = Vec::with_capacity(campaign_daily_map.len());

    for ((cid, s_day), cdata) in campaign_daily_map {
        let channel = if cid == "organic" {
            "organic".to_string()
        } else {
            campaigns.get(&cid).cloned().unwrap_or_default()
        };
        c_camp_ids.push(cid);
        c_channels.push(channel);
        c_days.push(s_day);
        c_sessions.push(cdata.sessions);
        c_purchasing.push(cdata.purchasing_sessions);
        c_revenue.push(cdata.revenue_cents);
        c_assisted.push(cdata.assisted_sessions);
    }

    let c_batch = RecordBatch::try_new(
        camp_schema.clone(),
        vec![
            Arc::new(StringArray::from(c_camp_ids)),
            Arc::new(StringArray::from(c_channels)),
            Arc::new(Date32Array::from(c_days)),
            Arc::new(Int64Array::from(c_sessions)),
            Arc::new(Int64Array::from(c_purchasing)),
            Arc::new(Int64Array::from(c_revenue)),
            Arc::new(Int64Array::from(c_assisted)),
        ],
    )
    .expect("Failed to create campaign_daily batch");

    let c_out_folder = out_path.join("campaign_daily");
    fs::create_dir_all(&c_out_folder).expect("Failed to create campaign_daily dir");
    let c_file = File::create(c_out_folder.join("part-0.parquet")).expect("Failed to create campaign parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut c_writer = ArrowWriter::try_new(c_file, camp_schema, Some(props)).expect("Failed to init camp writer");
    c_writer.write(&c_batch).expect("Failed to write camp batch");
    c_writer.close().expect("Failed to close camp writer");
}
