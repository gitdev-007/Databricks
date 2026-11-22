use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use serde_json::Value;

const DAYS_IN_MONTH: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const DAYS_IN_MONTH_LEAP: [u32; 13] = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const DAYS_BEFORE_MONTH: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
const DAYS_BEFORE_MONTH_LEAP: [i64; 13] = [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

#[inline(always)]
fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[inline(always)]
fn unquote_field(part: &[u8]) -> Option<&[u8]> {
    if part.starts_with(b"\"") {
        if part.ends_with(b"\"") && part.len() >= 2 {
            let inner = &part[1..part.len() - 1];
            if memchr::memchr(b'\"', inner).is_some() {
                return None;
            }
            Some(inner)
        } else {
            None
        }
    } else {
        if memchr::memchr(b'\"', part).is_some() {
            return None;
        }
        Some(part)
    }
}

#[inline(always)]
fn parse_csv_2<'a>(mut line: &'a [u8], out: &mut [&'a [u8]; 2]) -> bool {
    if line.ends_with(b"\n") {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return false;
    }
    let comma = match memchr::memchr(b',', line) {
        Some(pos) => pos,
        None => return false,
    };
    let rest = &line[comma + 1..];
    if memchr::memchr(b',', rest).is_some() {
        return false;
    }
    let f0 = match unquote_field(&line[..comma]) {
        Some(f) => f,
        None => return false,
    };
    let f1 = match unquote_field(rest) {
        Some(f) => f,
        None => return false,
    };
    out[0] = f0;
    out[1] = f1;
    true
}

#[inline(always)]
fn parse_csv_4<'a>(mut line: &'a [u8], out: &mut [&'a [u8]; 4]) -> bool {
    if line.ends_with(b"\n") {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return false;
    }
    let mut start = 0;
    for i in 0..3 {
        let comma = match memchr::memchr(b',', &line[start..]) {
            Some(c) => start + c,
            None => return false,
        };
        let part = &line[start..comma];
        out[i] = match unquote_field(part) {
            Some(p) => p,
            None => return false,
        };
        start = comma + 1;
    }
    let rest = &line[start..];
    if memchr::memchr(b',', rest).is_some() {
        return false;
    }
    out[3] = match unquote_field(rest) {
        Some(p) => p,
        None => return false,
    };
    true
}

#[inline(always)]
fn parse_csv_7<'a>(mut line: &'a [u8], out: &mut [&'a [u8]; 7]) -> bool {
    if line.ends_with(b"\n") {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return false;
    }
    let mut start = 0;
    for i in 0..6 {
        let comma = match memchr::memchr(b',', &line[start..]) {
            Some(c) => start + c,
            None => return false,
        };
        let part = &line[start..comma];
        out[i] = match unquote_field(part) {
            Some(p) => p,
            None => return false,
        };
        start = comma + 1;
    }
    let rest = &line[start..];
    if memchr::memchr(b',', rest).is_some() {
        return false;
    }
    out[6] = match unquote_field(rest) {
        Some(p) => p,
        None => return false,
    };
    true
}

#[inline(always)]
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

#[inline(always)]
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

#[inline(always)]
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

#[inline(always)]
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

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<i32> {
    if s.is_empty() {
        return None;
    }
    let mut p = 0i32;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        p = match p.checked_mul(10).and_then(|v| v.checked_add((b - b'0') as i32)) {
            Some(v) => v,
            None => return None,
        };
    }
    if p < 1 || p > 10_000_000 {
        return None;
    }
    Some(p)
}

struct Dsu<'a> {
    names: Vec<&'a [u8]>,
    name_to_id: HashMap<&'a [u8], u32, ahash::RandomState>,
    parent: Vec<u32>,
    canonical: Vec<u32>,
}

impl<'a> Dsu<'a> {
    fn new() -> Self {
        Self {
            names: Vec::new(),
            name_to_id: HashMap::default(),
            parent: Vec::new(),
            canonical: Vec::new(),
        }
    }

    fn get_or_create(&mut self, name: &'a [u8]) -> u32 {
        if let Some(&id) = self.name_to_id.get(name) {
            id
        } else {
            let id = self.names.len() as u32;
            self.names.push(name);
            self.name_to_id.insert(name, id);
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

    fn union(&mut self, a: &'a [u8], b: &'a [u8]) {
        let u = self.get_or_create(a);
        let v = self.get_or_create(b);
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

    fn flatten(&mut self) {
        for i in 0..self.parent.len() {
            let root = self.find(i as u32);
            self.canonical[i] = self.canonical[root as usize];
        }
    }

    #[inline(always)]
    fn get_canonical(&self, uid: &'a [u8]) -> &'a [u8] {
        if let Some(&id) = self.name_to_id.get(uid) {
            let canon_id = self.canonical[id as usize];
            self.names[canon_id as usize]
        } else {
            uid
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

const NUM_SHARDS: usize = 32;
const MAX_DAYS: usize = 32;

#[derive(Copy, Clone)]
struct RawParsedEvent<'a> {
    eid: &'a [u8],
    ingest_ms: i64,
    canon_name: &'a [u8],
    person_hash: u64,
    ts: i64,
    price_cents: i32,
    cat_id: u8,
    is_purchase: bool,
    is_valid: bool,
}

#[derive(Copy, Clone)]
struct RawParsedClick<'a> {
    cid: &'a [u8],
    canon_name: &'a [u8],
    person_hash: u64,
    ts: i64,
    camp_id: u16,
    is_valid: bool,
}

#[derive(Copy, Clone)]
struct ShardEvent<'a> {
    person_hash: u64,
    canon_name: &'a [u8],
    ts: i64,
    price_cents: i32,
    cat_id: u8,
    is_purchase: bool,
}

#[derive(Copy, Clone)]
struct ShardClick<'a> {
    person_hash: u64,
    canon_name: &'a [u8],
    ts: i64,
    camp_id: u16,
    click_id: &'a [u8],
}

#[derive(Copy, Clone, Default)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

impl CampDayAgg {
    #[inline(always)]
    fn add(&mut self, other: &CampDayAgg) {
        self.sessions += other.sessions;
        self.purchasing_sessions += other.purchasing_sessions;
        self.revenue_cents += other.revenue_cents;
        self.assisted_sessions += other.assisted_sessions;
    }
}

struct UserDayRow<'a> {
    person_name: &'a [u8],
    day: i32,
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    top_cat_id: Option<u8>,
}

#[inline(always)]
fn parse_and_validate_event<'a>(
    line: &'a [u8],
    window_start_ms: i64,
    window_end_ms: i64,
    products: &HashMap<&[u8], u8, ahash::RandomState>,
    dsu: &Dsu<'a>,
    ahash_builder: &ahash::RandomState,
) -> Option<RawParsedEvent<'a>> {
    let mut fields = [b"".as_slice(); 7];
    if !parse_csv_7(line, &mut fields) {
        return None;
    }
    let eid = fields[0];
    if eid.is_empty() {
        return None;
    }
    let ingest_ms = match parse_iso_f1(fields[3]) {
        Some(t) => t,
        None => return None,
    };

    let uid = fields[1];
    let ts_opt = parse_timestamp(fields[2]);
    let etype = fields[4];
    let pid = fields[5];
    let price_str = fields[6];

    let mut is_valid = false;
    let mut is_purchase = false;
    let mut price_cents = 0i32;
    let mut cat_id = 0u8;
    let mut ts = 0i64;
    let mut canon_name = uid;

    if !uid.is_empty() && !pid.is_empty() {
        if let Some(t) = ts_opt {
            if t >= window_start_ms && t < window_end_ms {
                if ingest_ms - t <= 86_400_000 {
                    if etype == b"purchase" {
                        if let Some(p) = parse_price(price_str) {
                            is_valid = true;
                            is_purchase = true;
                            price_cents = p;
                            ts = t;
                        }
                    } else if etype == b"view" || etype == b"cart" {
                        is_valid = true;
                        ts = t;
                    }
                }
            }
        }
    }

    if is_valid {
        canon_name = dsu.get_canonical(uid);
        if let Some(&cid) = products.get(pid) {
            cat_id = cid;
        }
    }

    let person_hash = if is_valid {
        ahash_builder.hash_one(canon_name)
    } else {
        0
    };

    Some(RawParsedEvent {
        eid,
        ingest_ms,
        canon_name,
        person_hash,
        ts,
        price_cents,
        cat_id,
        is_purchase,
        is_valid,
    })
}

#[inline(always)]
fn parse_and_validate_click<'a>(
    line: &'a [u8],
    click_window_start_ms: i64,
    window_end_ms: i64,
    campaigns: &HashMap<&[u8], u16, ahash::RandomState>,
    dsu: &Dsu<'a>,
    ahash_builder: &ahash::RandomState,
) -> Option<RawParsedClick<'a>> {
    let mut fields = [b"".as_slice(); 4];
    if !parse_csv_4(line, &mut fields) {
        return None;
    }
    let cid = fields[0];
    if cid.is_empty() {
        return None;
    }
    let uid = fields[1];
    let camp_str = fields[2];
    let ts_str = fields[3];

    let mut is_valid = false;
    let mut ts = 0i64;
    let mut camp_id = 0u16;
    let mut canon_name = uid;

    if !uid.is_empty() {
        if let Some(t) = parse_timestamp(ts_str) {
            if t >= click_window_start_ms && t < window_end_ms {
                if let Some(&c) = campaigns.get(camp_str) {
                    is_valid = true;
                    ts = t;
                    camp_id = c;
                    canon_name = dsu.get_canonical(uid);
                }
            }
        }
    }

    let person_hash = if is_valid {
        ahash_builder.hash_one(canon_name)
    } else {
        0
    };

    Some(RawParsedClick {
        cid,
        canon_name,
        person_hash,
        ts,
        camp_id,
        is_valid,
    })
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
    let min_day = (window_start_ms / 86_400_000) as i32;

    // 2. Read products.csv with Mmap
    let prod_file = data_path.join("products.csv");
    let prod_mmap = if prod_file.exists() {
        let f = File::open(&prod_file).ok();
        f.and_then(|file| unsafe { Mmap::map(&file).ok() })
    } else {
        None
    };

    let mut cat_names: Vec<String> = vec!["unknown".to_string()];
    let mut cat_index_map: HashMap<&[u8], u8, ahash::RandomState> = HashMap::default();
    let mut products: HashMap<&[u8], u8, ahash::RandomState> = HashMap::default();

    if let Some(ref mmap) = prod_mmap {
        let mut lines = mmap.split(|&b| b == b'\n');
        lines.next();
        let mut fields = [b"".as_slice(); 2];
        for line in lines {
            if parse_csv_2(line, &mut fields) {
                let cat = fields[1];
                let cid = if let Some(&c) = cat_index_map.get(cat) {
                    c
                } else {
                    let c = cat_names.len() as u8;
                    cat_names.push(String::from_utf8_lossy(cat).to_string());
                    cat_index_map.insert(cat, c);
                    c
                };
                products.insert(fields[0], cid);
            }
        }
    }

    // 3. Read campaigns.csv with Mmap
    let camp_file = data_path.join("campaigns.csv");
    let camp_mmap = if camp_file.exists() {
        let f = File::open(&camp_file).ok();
        f.and_then(|file| unsafe { Mmap::map(&file).ok() })
    } else {
        None
    };

    let mut camp_names: Vec<String> = vec!["organic".to_string()];
    let mut chan_names: Vec<String> = vec!["organic".to_string()];
    let mut campaigns: HashMap<&[u8], u16, ahash::RandomState> = HashMap::default();

    if let Some(ref mmap) = camp_mmap {
        let mut lines = mmap.split(|&b| b == b'\n');
        lines.next();
        let mut fields = [b"".as_slice(); 2];
        for line in lines {
            if parse_csv_2(line, &mut fields) {
                let cid = camp_names.len() as u16;
                camp_names.push(String::from_utf8_lossy(fields[0]).to_string());
                chan_names.push(String::from_utf8_lossy(fields[1]).to_string());
                campaigns.insert(fields[0], cid);
            }
        }
    }
    let num_campaigns = camp_names.len();

    // 4. Read links/*.csv into DSU with Mmap
    let link_files = list_csv_files(&data_path.join("links"));
    let link_mmaps: Vec<Mmap> = link_files
        .iter()
        .filter_map(|lf| {
            let f = File::open(lf).ok()?;
            unsafe { Mmap::map(&f).ok() }
        })
        .collect();

    let mut dsu = Dsu::new();
    for mmap in &link_mmaps {
        let mut lines = mmap.split(|&b| b == b'\n');
        lines.next();
        let mut fields = [b"".as_slice(); 2];
        for line in lines {
            if parse_csv_2(line, &mut fields) {
                if !fields[0].is_empty() && !fields[1].is_empty() {
                    dsu.union(fields[0], fields[1]);
                }
            }
        }
    }
    dsu.flatten();

    let ahash_builder = ahash::RandomState::with_seeds(11, 22, 33, 44);

    // 5. Ingest Event files in Parallel with Mmap & Single-Pass Parse
    let event_files = list_csv_files(&data_path.join("events"));
    let event_mmaps: Vec<Mmap> = event_files
        .iter()
        .filter_map(|ef| {
            let f = File::open(ef).ok()?;
            unsafe { Mmap::map(&f).ok() }
        })
        .collect();

    let per_file_event_shards: Vec<Vec<Vec<RawParsedEvent>>> = event_mmaps
        .par_iter()
        .map(|mmap| {
            let mut start = 0;
            if let Some(first_nl) = memchr::memchr(b'\n', mmap) {
                start = first_nl + 1;
            }
            let bytes = &mmap[start..];
            let file_est_lines = bytes.len() / 55;
            let per_shard_est = (file_est_lines / NUM_SHARDS) + 128;
            let mut shards: Vec<Vec<RawParsedEvent>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(per_shard_est))
                .collect();
            for line in bytes.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                if let Some(ev) = parse_and_validate_event(
                    line,
                    window_start_ms,
                    window_end_ms,
                    &products,
                    &dsu,
                    &ahash_builder,
                ) {
                    let h = ahash_builder.hash_one(ev.eid);
                    let shard_idx = (h as usize) % NUM_SHARDS;
                    shards[shard_idx].push(ev);
                }
            }
            shards
        })
        .collect();

    // Parallel Transpose of Event EID Shards
    let combined_eid_shards: Vec<Vec<RawParsedEvent>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|s| {
            let mut total_len = 0;
            for file_shards in &per_file_event_shards {
                total_len += file_shards[s].len();
            }
            let mut shard = Vec::with_capacity(total_len);
            for file_shards in &per_file_event_shards {
                shard.extend_from_slice(&file_shards[s]);
            }
            shard
        })
        .collect();
    drop(per_file_event_shards);

    // 6. Dedup Events on eid via Sort & Partition to Person Shards
    let dedup_event_shards: Vec<Vec<Vec<ShardEvent>>> = combined_eid_shards
        .into_par_iter()
        .map(|mut shard| {
            shard.sort_unstable_by(|a, b| a.eid.cmp(b.eid).then_with(|| a.ingest_ms.cmp(&b.ingest_ms)));
            let per_p_est = (shard.len() / NUM_SHARDS) + 128;
            let mut person_buckets: Vec<Vec<ShardEvent>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(per_p_est))
                .collect();
            let mut i = 0;
            let n = shard.len();
            while i < n {
                let kept = &shard[i];
                let mut next_i = i + 1;
                while next_i < n && shard[next_i].eid == kept.eid {
                    next_i += 1;
                }
                if kept.is_valid {
                    let p_shard = (kept.person_hash as usize) % NUM_SHARDS;
                    person_buckets[p_shard].push(ShardEvent {
                        person_hash: kept.person_hash,
                        canon_name: kept.canon_name,
                        ts: kept.ts,
                        price_cents: kept.price_cents,
                        cat_id: kept.cat_id,
                        is_purchase: kept.is_purchase,
                    });
                }
                i = next_i;
            }
            person_buckets
        })
        .collect();

    // Transpose Events to Person Shards
    let person_event_shards: Vec<Vec<ShardEvent>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|p| {
            let mut total = 0;
            for w in &dedup_event_shards {
                total += w[p].len();
            }
            let mut shard = Vec::with_capacity(total);
            for w in &dedup_event_shards {
                shard.extend_from_slice(&w[p]);
            }
            shard
        })
        .collect();
    drop(dedup_event_shards);

    // 7. Ingest & Dedup Clicks with Mmap & Single-Pass Parse
    let click_files = list_csv_files(&data_path.join("clicks"));
    let click_mmaps: Vec<Mmap> = click_files
        .iter()
        .filter_map(|cf| {
            let f = File::open(cf).ok()?;
            unsafe { Mmap::map(&f).ok() }
        })
        .collect();

    let per_file_click_shards: Vec<Vec<Vec<RawParsedClick>>> = click_mmaps
        .par_iter()
        .map(|mmap| {
            let mut start = 0;
            if let Some(first_nl) = memchr::memchr(b'\n', mmap) {
                start = first_nl + 1;
            }
            let bytes = &mmap[start..];
            let file_est_lines = bytes.len() / 45;
            let per_shard_est = (file_est_lines / NUM_SHARDS) + 128;
            let mut shards: Vec<Vec<RawParsedClick>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(per_shard_est))
                .collect();
            for line in bytes.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                if let Some(clk) = parse_and_validate_click(
                    line,
                    click_window_start_ms,
                    window_end_ms,
                    &campaigns,
                    &dsu,
                    &ahash_builder,
                ) {
                    let h = ahash_builder.hash_one(clk.cid);
                    let shard_idx = (h as usize) % NUM_SHARDS;
                    shards[shard_idx].push(clk);
                }
            }
            shards
        })
        .collect();

    let combined_cid_shards: Vec<Vec<RawParsedClick>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|s| {
            let mut total_len = 0;
            for file_shards in &per_file_click_shards {
                total_len += file_shards[s].len();
            }
            let mut shard = Vec::with_capacity(total_len);
            for file_shards in &per_file_click_shards {
                shard.extend_from_slice(&file_shards[s]);
            }
            shard
        })
        .collect();
    drop(per_file_click_shards);

    let dedup_click_shards: Vec<Vec<Vec<ShardClick>>> = combined_cid_shards
        .into_par_iter()
        .map(|mut shard| {
            shard.sort_unstable_by(|a, b| a.cid.cmp(b.cid));
            let per_p_est = (shard.len() / NUM_SHARDS) + 128;
            let mut person_buckets: Vec<Vec<ShardClick>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(per_p_est))
                .collect();
            let mut i = 0;
            let n = shard.len();
            while i < n {
                let kept = &shard[i];
                let mut next_i = i + 1;
                while next_i < n && shard[next_i].cid == kept.cid {
                    next_i += 1;
                }
                if kept.is_valid {
                    let p_shard = (kept.person_hash as usize) % NUM_SHARDS;
                    person_buckets[p_shard].push(ShardClick {
                        person_hash: kept.person_hash,
                        canon_name: kept.canon_name,
                        ts: kept.ts,
                        camp_id: kept.camp_id,
                        click_id: kept.cid,
                    });
                }
                i = next_i;
            }
            person_buckets
        })
        .collect();

    // Transpose Clicks to Person Shards
    let person_click_shards: Vec<Vec<ShardClick>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|p| {
            let mut total = 0;
            for w in &dedup_click_shards {
                total += w[p].len();
            }
            let mut shard = Vec::with_capacity(total);
            for w in &dedup_click_shards {
                shard.extend_from_slice(&w[p]);
            }
            shard
        })
        .collect();
    drop(dedup_click_shards);

    let cat_names_arc = Arc::new(cat_names);
    let u_out_folder = out_path.join("user_daily");
    fs::create_dir_all(&u_out_folder).expect("Failed to create user_daily dir");

    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    let any_user_files_written = Arc::new(AtomicBool::new(false));

    // 8. Parallel Sessionization, Attribution & Direct Parallel Parquet Writing
    let camp_shards: Vec<Vec<Vec<CampDayAgg>>> = person_event_shards
        .into_par_iter()
        .zip(person_click_shards.into_par_iter())
        .enumerate()
        .map(|(p, (mut events, mut clicks))| {
            events.sort_unstable_by(|a, b| {
                a.person_hash
                    .cmp(&b.person_hash)
                    .then_with(|| a.canon_name.cmp(b.canon_name))
                    .then_with(|| a.ts.cmp(&b.ts))
            });

            clicks.sort_unstable_by(|a, b| {
                a.person_hash
                    .cmp(&b.person_hash)
                    .then_with(|| a.canon_name.cmp(b.canon_name))
                    .then_with(|| a.ts.cmp(&b.ts))
                    .then_with(|| a.click_id.cmp(b.click_id))
            });

            let mut local_user_rows: Vec<UserDayRow> = Vec::new();
            let mut local_camp: Vec<Vec<CampDayAgg>> = vec![vec![CampDayAgg::default(); MAX_DAYS]; num_campaigns];

            let mut e_idx = 0;
            let mut c_idx = 0;
            let num_events = events.len();
            let num_clicks = clicks.len();

            while e_idx < num_events {
                let p_hash = events[e_idx].person_hash;
                let p_name = events[e_idx].canon_name;

                let mut e_end = e_idx + 1;
                while e_end < num_events
                    && events[e_end].person_hash == p_hash
                    && events[e_end].canon_name == p_name
                {
                    e_end += 1;
                }
                let p_events = &events[e_idx..e_end];

                while c_idx < num_clicks
                    && (clicks[c_idx].person_hash < p_hash
                        || (clicks[c_idx].person_hash == p_hash && clicks[c_idx].canon_name < p_name))
                {
                    c_idx += 1;
                }
                let c_start = c_idx;
                while c_idx < num_clicks
                    && clicks[c_idx].person_hash == p_hash
                    && clicks[c_idx].canon_name == p_name
                {
                    c_idx += 1;
                }
                let p_clicks = &clicks[c_start..c_idx];

                // Sessionize p_events
                let mut cur_day: Option<i32> = None;
                let mut day_sessions = 0i64;
                let mut day_events = 0i64;
                let mut day_revenue = 0i64;
                let mut day_cat_spend = [0i64; 32];

                let mut s_idx = 0;
                let n_pe = p_events.len();

                while s_idx < n_pe {
                    let sess_start_ts = p_events[s_idx].ts;
                    let sess_day = (sess_start_ts / 86_400_000) as i32;

                    let mut s_end = s_idx + 1;
                    let mut last_distinct_ts = sess_start_ts;
                    while s_end < n_pe {
                        let ev_ts = p_events[s_end].ts;
                        if ev_ts == last_distinct_ts {
                            s_end += 1;
                        } else if ev_ts - last_distinct_ts > 1_800_000 {
                            break;
                        } else {
                            last_distinct_ts = ev_ts;
                            s_end += 1;
                        }
                    }

                    let sess_events = &p_events[s_idx..s_end];
                    let sess_events_count = sess_events.len() as i64;
                    let mut sess_revenue = 0i64;
                    let mut sess_purchases = 0i64;

                    if cur_day != Some(sess_day) {
                        if let Some(prev_day) = cur_day {
                            let mut top_cat: Option<u8> = None;
                            if day_revenue > 0 {
                                let mut best_sp = 0i64;
                                let mut best_id = 0u8;
                                for (cid, &sp) in day_cat_spend.iter().enumerate() {
                                    if sp > best_sp
                                        || (sp == best_sp
                                            && sp > 0
                                            && cat_names_arc[cid] < cat_names_arc[best_id as usize])
                                    {
                                        best_sp = sp;
                                        best_id = cid as u8;
                                    }
                                }
                                top_cat = Some(best_id);
                            }
                            local_user_rows.push(UserDayRow {
                                person_name: p_name,
                                day: prev_day,
                                sessions: day_sessions,
                                events: day_events,
                                revenue_cents: day_revenue,
                                top_cat_id: top_cat,
                            });
                        }
                        cur_day = Some(sess_day);
                        day_sessions = 0;
                        day_events = 0;
                        day_revenue = 0;
                        day_cat_spend = [0i64; 32];
                    }

                    day_sessions += 1;
                    day_events += sess_events_count;

                    for ev in sess_events {
                        if ev.is_purchase {
                            sess_revenue += ev.price_cents as i64;
                            sess_purchases += 1;
                            day_cat_spend[ev.cat_id as usize] += ev.price_cents as i64;
                        }
                    }
                    day_revenue += sess_revenue;

                    // Attribution & Assists
                    let min_click_ts = sess_start_ts - 604_800_000;
                    let clk_start = p_clicks.partition_point(|c| c.ts < min_click_ts);
                    let clk_end = p_clicks.partition_point(|c| c.ts <= sess_start_ts);
                    let eligible = &p_clicks[clk_start..clk_end];

                    let mut credited_camp_id = 0u16; // 0 is organic
                    if !eligible.is_empty() {
                        let max_ts = eligible.last().unwrap().ts;
                        let first_max = eligible.partition_point(|c| c.ts < max_ts);
                        credited_camp_id = eligible[first_max].camp_id;
                    }

                    let day_idx = (sess_day - min_day) as usize;
                    if day_idx < MAX_DAYS {
                        let purch_val = if sess_purchases > 0 { 1 } else { 0 };
                        local_camp[credited_camp_id as usize][day_idx].sessions += 1;
                        local_camp[credited_camp_id as usize][day_idx].purchasing_sessions += purch_val;
                        local_camp[credited_camp_id as usize][day_idx].revenue_cents += sess_revenue;

                        let mut assisting_bitset = [0u64; 32];
                        for c in eligible {
                            if c.camp_id != 0 {
                                let word = (c.camp_id >> 6) as usize;
                                let bit = 1u64 << (c.camp_id & 63);
                                if (assisting_bitset[word] & bit) == 0 {
                                    assisting_bitset[word] |= bit;
                                    local_camp[c.camp_id as usize][day_idx].assisted_sessions += 1;
                                }
                            }
                        }
                    }

                    s_idx = s_end;
                }

                if let Some(prev_day) = cur_day {
                    let mut top_cat: Option<u8> = None;
                    if day_revenue > 0 {
                        let mut best_sp = 0i64;
                        let mut best_id = 0u8;
                        for (cid, &sp) in day_cat_spend.iter().enumerate() {
                            if sp > best_sp
                                || (sp == best_sp
                                    && sp > 0
                                    && cat_names_arc[cid] < cat_names_arc[best_id as usize])
                            {
                                best_sp = sp;
                                best_id = cid as u8;
                            }
                        }
                        top_cat = Some(best_id);
                    }
                    local_user_rows.push(UserDayRow {
                        person_name: p_name,
                        day: prev_day,
                        sessions: day_sessions,
                        events: day_events,
                        revenue_cents: day_revenue,
                        top_cat_id: top_cat,
                    });
                }

                e_idx = e_end;
            }

            // Direct Parallel Parquet Writing for user_daily
            if !local_user_rows.is_empty() {
                any_user_files_written.store(true, Ordering::Relaxed);
                let n_rows = local_user_rows.len();
                let mut u_builder = StringBuilder::with_capacity(n_rows, n_rows * 16);
                let mut u_days = Vec::with_capacity(n_rows);
                let mut u_sessions = Vec::with_capacity(n_rows);
                let mut u_events = Vec::with_capacity(n_rows);
                let mut u_revenue = Vec::with_capacity(n_rows);
                let mut u_top_cat_builder = StringBuilder::with_capacity(n_rows, n_rows * 12);

                for row in &local_user_rows {
                    u_builder.append_value(std::str::from_utf8(row.person_name).unwrap_or(""));
                    u_days.push(row.day);
                    u_sessions.push(row.sessions);
                    u_events.push(row.events);
                    u_revenue.push(row.revenue_cents);
                    if let Some(cat_id) = row.top_cat_id {
                        u_top_cat_builder.append_value(&cat_names_arc[cat_id as usize]);
                    } else {
                        u_top_cat_builder.append_null();
                    }
                }

                let u_batch = RecordBatch::try_new(
                    user_schema.clone(),
                    vec![
                        Arc::new(u_builder.finish()) as ArrayRef,
                        Arc::new(Date32Array::from(u_days)) as ArrayRef,
                        Arc::new(Int64Array::from(u_sessions)) as ArrayRef,
                        Arc::new(Int64Array::from(u_events)) as ArrayRef,
                        Arc::new(Int64Array::from(u_revenue)) as ArrayRef,
                        Arc::new(u_top_cat_builder.finish()) as ArrayRef,
                    ],
                )
                .expect("Failed to create user_daily batch");

                let part_file_name = format!("part-{}.parquet", p);
                let u_file = File::create(u_out_folder.join(part_file_name)).expect("Failed to create user parquet");
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let mut u_writer = ArrowWriter::try_new(u_file, user_schema.clone(), Some(props))
                    .expect("Failed to init user writer");
                u_writer.write(&u_batch).expect("Failed to write user batch");
                u_writer.close().expect("Failed to close user writer");
            }

            local_camp
        })
        .collect();

    // If no user daily rows exist anywhere, write empty part-0.parquet
    if !any_user_files_written.load(Ordering::Relaxed) {
        let u_batch = RecordBatch::new_empty(user_schema.clone());
        let u_file = File::create(u_out_folder.join("part-0.parquet")).expect("Failed to create empty user parquet");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut u_writer = ArrowWriter::try_new(u_file, user_schema, Some(props))
            .expect("Failed to init empty user writer");
        u_writer.write(&u_batch).expect("Failed to write empty user batch");
        u_writer.close().expect("Failed to close empty user writer");
    }

    // 9. Merge & Write campaign_daily Parquet
    let camp_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));

    let mut global_camp: Vec<Vec<CampDayAgg>> = vec![vec![CampDayAgg::default(); MAX_DAYS]; num_campaigns];
    for shard_camp in &camp_shards {
        for c in 0..num_campaigns {
            for d in 0..MAX_DAYS {
                global_camp[c][d].add(&shard_camp[c][d]);
            }
        }
    }

    let mut c_camp_builder = StringBuilder::new();
    let mut c_chan_builder = StringBuilder::new();
    let mut c_days = Vec::new();
    let mut c_sessions = Vec::new();
    let mut c_purchasing = Vec::new();
    let mut c_revenue = Vec::new();
    let mut c_assisted = Vec::new();

    for c in 0..num_campaigns {
        for d in 0..MAX_DAYS {
            let agg = &global_camp[c][d];
            if agg.sessions > 0 || agg.assisted_sessions > 0 {
                c_camp_builder.append_value(&camp_names[c]);
                c_chan_builder.append_value(&chan_names[c]);
                c_days.push(min_day + d as i32);
                c_sessions.push(agg.sessions);
                c_purchasing.push(agg.purchasing_sessions);
                c_revenue.push(agg.revenue_cents);
                c_assisted.push(agg.assisted_sessions);
            }
        }
    }

    let c_batch = RecordBatch::try_new(
        camp_schema.clone(),
        vec![
            Arc::new(c_camp_builder.finish()) as ArrayRef,
            Arc::new(c_chan_builder.finish()) as ArrayRef,
            Arc::new(Date32Array::from(c_days)) as ArrayRef,
            Arc::new(Int64Array::from(c_sessions)) as ArrayRef,
            Arc::new(Int64Array::from(c_purchasing)) as ArrayRef,
            Arc::new(Int64Array::from(c_revenue)) as ArrayRef,
            Arc::new(Int64Array::from(c_assisted)) as ArrayRef,
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
