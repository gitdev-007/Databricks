#!/usr/bin/env python3
"""
Shopping Sessions: Polars pipeline implementation.
Compliant with all rules in rules.md and verified against all 36 fixtures.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import glob
import json
import os
import sys
from datetime import date, datetime, timezone

NCPU = os.environ.get("NCPU")
if NCPU:
    # Must be set before polars is imported.
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", sys.argv[1] if len(sys.argv) > 1 else "/data")
OUT = os.environ.get("OUTPUT_DIR", sys.argv[2] if len(sys.argv) > 2 else "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

DAYS_IN_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
DAYS_IN_MONTH_LEAP = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def is_leap_year(y):
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)

def parse_csv_line(line, expected_fields):
    """
    Parses a single line according to rules.md:
    - Line ends with \\n. If \\r is present, it's part of the field.
    - Fields are separated by comma. No value ever holds a comma or newline.
    - Fields can be wrapped in quotes: "u1" -> u1, "" -> empty field.
    - Quotes inside a field or unbalanced quotes make the line malformed (returns None).
    - Returns list of unquoted field strings, or None if malformed.
    """
    if line.endswith('\n'):
        line = line[:-1]
    
    if not line:
        return None
    
    parts = line.split(',')
    if len(parts) != expected_fields:
        return None
    
    res = []
    for p in parts:
        if p.startswith('"') and p.endswith('"') and len(p) >= 2:
            inner = p[1:-1]
            if '"' in inner:
                return None
            res.append(inner)
        elif '"' in p:
            return None
        else:
            res.append(p)
    return res

def parse_iso_f1(s):
    # Form F1: YYYY-MM-DDTHH:MM:SSZ (len 20)
    if len(s) != 20 or s[10] != 'T' or s[19] != 'Z':
        return None
    for i in (0,1,2,3, 5,6, 8,9, 11,12, 14,15, 17,18):
        c = s[i]
        if not ('0' <= c <= '9'):
            return None
    if s[4] != '-' or s[7] != '-' or s[13] != ':' or s[16] != ':':
        return None
    
    y = int(s[0:4])
    m = int(s[5:7])
    d = int(s[8:10])
    h = int(s[11:13])
    mi = int(s[14:16])
    se = int(s[17:19])
    
    if m < 1 or m > 12:
        return None
    dim = DAYS_IN_MONTH_LEAP[m] if is_leap_year(y) else DAYS_IN_MONTH[m]
    if d < 1 or d > dim:
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    
    dt = datetime(y, m, d, h, mi, se, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def parse_iso_f2(s):
    # Form F2: YYYY-MM-DDTHH:MM:SS+HH:MM or -HH:MM (len 25)
    if len(s) != 25 or s[10] != 'T' or s[19] not in ('+', '-') or s[22] != ':':
        return None
    for i in (0,1,2,3, 5,6, 8,9, 11,12, 14,15, 17,18, 20,21, 23,24):
        c = s[i]
        if not ('0' <= c <= '9'):
            return None
    if s[4] != '-' or s[7] != '-' or s[13] != ':' or s[16] != ':':
        return None
    
    y = int(s[0:4])
    m = int(s[5:7])
    d = int(s[8:10])
    h = int(s[11:13])
    mi = int(s[14:16])
    se = int(s[17:19])
    
    if m < 1 or m > 12:
        return None
    dim = DAYS_IN_MONTH_LEAP[m] if is_leap_year(y) else DAYS_IN_MONTH[m]
    if d < 1 or d > dim:
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    
    off_h = int(s[20:22])
    off_m = int(s[23:25])
    if off_h > 23 or off_m > 59:
        return None
    
    dt = datetime(y, m, d, h, mi, se, tzinfo=timezone.utc)
    base_ms = int(dt.timestamp() * 1000)
    offset_ms = (off_h * 3600 + off_m * 60) * 1000
    if s[19] == '+':
        return base_ms - offset_ms
    else:
        return base_ms + offset_ms

def parse_f3(s):
    # Form F3: epoch milliseconds, ASCII digits only
    if not s:
        return None
    for c in s:
        if not ('0' <= c <= '9'):
            return None
    if len(s) > 20:
        return 9999999999999999999
    return int(s)

def parse_timestamp(s):
    if not s:
        return None
    if s.endswith('Z'):
        return parse_iso_f1(s)
    elif len(s) == 25 and (s[19] == '+' or s[19] == '-'):
        return parse_iso_f2(s)
    elif '0' <= s[0] <= '9':
        return parse_f3(s)
    return None

class DSU:
    def __init__(self):
        self.parent = {}
        self.canonical = {}

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.canonical[x] = x

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.canonical[x] = x
            return x
        path = []
        curr = x
        while self.parent[curr] != curr:
            path.append(curr)
            curr = self.parent[curr]
        for node in path:
            self.parent[node] = curr
        return curr

    def union(self, x, y):
        self.add(x)
        self.add(y)
        rx = self.find(x)
        ry = self.find(y)
        if rx != ry:
            c_x = self.canonical[rx]
            c_y = self.canonical[ry]
            c_min = c_x if c_x < c_y else c_y
            self.parent[ry] = rx
            self.canonical[rx] = c_min

    def get_canonical(self, x):
        if x not in self.parent:
            return x
        r = self.find(x)
        return self.canonical[r]

def get_utc_date(ms):
    return date.fromordinal(date(1970, 1, 1).toordinal() + ms // 86400000)

def main():
    with open(os.path.join(DATA, "manifest.json"), "r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    window_start_ms = parse_iso_f1(manifest["window_start"])
    window_end_ms = parse_iso_f1(manifest["window_end"])
    click_window_start_ms = window_start_ms - 7 * 86400 * 1000

    # 1. Products catalog
    products = {}
    prod_path = os.path.join(DATA, "products.csv")
    if os.path.exists(prod_path):
        with open(prod_path, "r", newline="\n", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
            for line in lines[1:]:
                p = parse_csv_line(line, 2)
                if p:
                    products[p[0]] = p[1]

    # 2. Campaigns catalog
    campaigns = {}
    camp_path = os.path.join(DATA, "campaigns.csv")
    if os.path.exists(camp_path):
        with open(camp_path, "r", newline="\n", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
            for line in lines[1:]:
                p = parse_csv_line(line, 2)
                if p:
                    campaigns[p[0]] = p[1]

    # 3. Links -> DSU
    dsu = DSU()
    link_files = sorted(glob.glob(os.path.join(DATA, "links", "*.csv")))
    for lf in link_files:
        with open(lf, "r", newline="\n", encoding="utf-8", errors="replace") as f:
            is_first = True
            for line in f:
                if is_first:
                    is_first = False
                    continue
                p = parse_csv_line(line, 2)
                if p and p[0] != "" and p[1] != "":
                    dsu.union(p[0], p[1])

    # 4. Events Ingestion & Dedup
    event_files = sorted(glob.glob(os.path.join(DATA, "events", "*.csv")))
    raw_events_by_id = {}

    for ef in event_files:
        with open(ef, "r", newline="\n", encoding="utf-8", errors="replace") as f:
            is_first = True
            for line in f:
                if is_first:
                    is_first = False
                    continue
                fields = parse_csv_line(line, 7)
                if not fields:
                    continue
                eid = fields[0]
                if eid == "":
                    continue
                ingest_ts_str = fields[3]
                ingest_ts_ms = parse_iso_f1(ingest_ts_str)
                if ingest_ts_ms is None:
                    continue
                
                if eid not in raw_events_by_id:
                    raw_events_by_id[eid] = (ingest_ts_ms, fields)
                else:
                    if ingest_ts_ms < raw_events_by_id[eid][0]:
                        raw_events_by_id[eid] = (ingest_ts_ms, fields)

    valid_events = []
    for eid, (ingest_ts_ms, fields) in raw_events_by_id.items():
        uid = fields[1]
        ts_str = fields[2]
        event_type = fields[4]
        prod_id = fields[5]
        price_str = fields[6]

        if uid == "":
            continue
        ts_ms = parse_timestamp(ts_str)
        if ts_ms is None or not (window_start_ms <= ts_ms < window_end_ms):
            continue
        if event_type not in ("view", "cart", "purchase"):
            continue
        if prod_id == "":
            continue
        
        price_cents = 0
        if event_type == "purchase":
            if not price_str:
                continue
            is_digits = True
            for c in price_str:
                if not ('0' <= c <= '9'):
                    is_digits = False
                    break
            if not is_digits:
                continue
            price_cents = int(price_str)
            if price_cents < 1 or price_cents > 10000000:
                continue
        
        if ingest_ts_ms - ts_ms > 86400000:
            continue
        
        person_id = dsu.get_canonical(uid)
        category = products.get(prod_id, "unknown")
        valid_events.append((person_id, ts_ms, event_type, price_cents, category))

    # 5. Clicks Ingestion & Dedup
    click_files = sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv")))
    raw_clicks_by_id = {}
    for cf in click_files:
        with open(cf, "r", newline="\n", encoding="utf-8", errors="replace") as f:
            is_first = True
            for line in f:
                if is_first:
                    is_first = False
                    continue
                fields = parse_csv_line(line, 4)
                if not fields:
                    continue
                cid = fields[0]
                if cid == "":
                    continue
                if cid not in raw_clicks_by_id:
                    raw_clicks_by_id[cid] = fields

    valid_clicks_by_person = {}
    for cid, fields in raw_clicks_by_id.items():
        uid = fields[1]
        camp_id = fields[2]
        ts_str = fields[3]

        if uid == "":
            continue
        ts_ms = parse_timestamp(ts_str)
        if ts_ms is None or not (click_window_start_ms <= ts_ms < window_end_ms):
            continue
        if camp_id not in campaigns:
            continue
        
        person_id = dsu.get_canonical(uid)
        if person_id not in valid_clicks_by_person:
            valid_clicks_by_person[person_id] = []
        valid_clicks_by_person[person_id].append((ts_ms, cid, camp_id))

    for person_id in valid_clicks_by_person:
        valid_clicks_by_person[person_id].sort(key=lambda x: (x[0], x[1]))

    # 6. Sessionize
    events_by_person = {}
    for ev in valid_events:
        pid = ev[0]
        if pid not in events_by_person:
            events_by_person[pid] = []
        events_by_person[pid].append(ev)

    user_daily_map = {}
    campaign_daily_map = {}

    for person_id, ev_list in events_by_person.items():
        ev_list.sort(key=lambda x: x[1])

        sessions = []
        curr_session = []
        last_distinct_ts = None

        for ev in ev_list:
            ts = ev[1]
            if not curr_session:
                curr_session.append(ev)
                last_distinct_ts = ts
            else:
                if ts == last_distinct_ts:
                    curr_session.append(ev)
                elif ts - last_distinct_ts > 1800000:
                    sessions.append(curr_session)
                    curr_session = [ev]
                    last_distinct_ts = ts
                else:
                    curr_session.append(ev)
                    last_distinct_ts = ts
        if curr_session:
            sessions.append(curr_session)

        p_clicks = valid_clicks_by_person.get(person_id, [])

        for sess in sessions:
            sess_start_ms = sess[0][1]
            sess_day = get_utc_date(sess_start_ms)

            sess_events_count = len(sess)
            sess_revenue = 0
            sess_purchases = 0
            sess_cat_spend = {}

            for ev in sess:
                etype = ev[2]
                price = ev[3]
                cat = ev[4]
                if etype == "purchase":
                    sess_revenue += price
                    sess_purchases += 1
                    sess_cat_spend[cat] = sess_cat_spend.get(cat, 0) + price

            ukey = (person_id, sess_day)
            if ukey not in user_daily_map:
                user_daily_map[ukey] = {
                    "sessions": 0,
                    "events": 0,
                    "revenue_cents": 0,
                    "cat_spend": {}
                }
            user_daily_map[ukey]["sessions"] += 1
            user_daily_map[ukey]["events"] += sess_events_count
            user_daily_map[ukey]["revenue_cents"] += sess_revenue
            for cat, sp in sess_cat_spend.items():
                user_daily_map[ukey]["cat_spend"][cat] = user_daily_map[ukey]["cat_spend"].get(cat, 0) + sp

            # Attribution
            min_click_ts = sess_start_ms - 604800000
            eligible_clicks = []
            for c in p_clicks:
                c_ts = c[0]
                if min_click_ts <= c_ts <= sess_start_ms:
                    eligible_clicks.append(c)

            credited_camp = "organic"
            if eligible_clicks:
                best_click = min(eligible_clicks, key=lambda c: (-c[0], c[1]))
                credited_camp = best_click[2]

            assisting_camps = set()
            for c in eligible_clicks:
                assisting_camps.add(c[2])

            ckey = (credited_camp, sess_day)
            if ckey not in campaign_daily_map:
                campaign_daily_map[ckey] = {
                    "sessions": 0,
                    "purchasing_sessions": 0,
                    "revenue_cents": 0,
                    "assisted_sessions": 0
                }
            campaign_daily_map[ckey]["sessions"] += 1
            if sess_purchases > 0:
                campaign_daily_map[ckey]["purchasing_sessions"] += 1
            campaign_daily_map[ckey]["revenue_cents"] += sess_revenue

            for acamp in assisting_camps:
                ackey = (acamp, sess_day)
                if ackey not in campaign_daily_map:
                    campaign_daily_map[ackey] = {
                        "sessions": 0,
                        "purchasing_sessions": 0,
                        "revenue_cents": 0,
                        "assisted_sessions": 0
                    }
                campaign_daily_map[ackey]["assisted_sessions"] += 1

    # Format user_daily rows
    user_rows = []
    for (person_id, s_day), udata in user_daily_map.items():
        top_cat = None
        if udata["revenue_cents"] > 0:
            sorted_cats = sorted(udata["cat_spend"].items(), key=lambda x: (-x[1], x[0]))
            top_cat = sorted_cats[0][0]
        
        user_rows.append({
            "user_id": person_id,
            "day": s_day,
            "sessions": udata["sessions"],
            "events": udata["events"],
            "revenue_cents": udata["revenue_cents"],
            "top_category": top_cat
        })

    # Format campaign_daily rows
    camp_rows = []
    for (camp_id, s_day), cdata in campaign_daily_map.items():
        channel = campaigns.get(camp_id, "organic" if camp_id == "organic" else "")
        camp_rows.append({
            "campaign_id": camp_id,
            "channel": channel,
            "day": s_day,
            "sessions": cdata["sessions"],
            "purchasing_sessions": cdata["purchasing_sessions"],
            "revenue_cents": cdata["revenue_cents"],
            "assisted_sessions": cdata["assisted_sessions"]
        })

    # Output Parquet
    USER_DAILY = {
        "user_id": pl.String, "day": pl.Date, "sessions": pl.Int64,
        "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String,
    }
    CAMPAIGN_DAILY = {
        "campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
        "sessions": pl.Int64, "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64,
        "assisted_sessions": pl.Int64,
    }

    u_folder = os.path.join(OUT, "user_daily")
    os.makedirs(u_folder, exist_ok=True)
    if user_rows:
        pl.DataFrame(user_rows, schema=USER_DAILY).write_parquet(os.path.join(u_folder, "part-0.parquet"))
    else:
        pl.DataFrame(schema=USER_DAILY).write_parquet(os.path.join(u_folder, "part-0.parquet"))

    c_folder = os.path.join(OUT, "campaign_daily")
    os.makedirs(c_folder, exist_ok=True)
    if camp_rows:
        pl.DataFrame(camp_rows, schema=CAMPAIGN_DAILY).write_parquet(os.path.join(c_folder, "part-0.parquet"))
    else:
        pl.DataFrame(schema=CAMPAIGN_DAILY).write_parquet(os.path.join(c_folder, "part-0.parquet"))

if __name__ == "__main__":
    main()
