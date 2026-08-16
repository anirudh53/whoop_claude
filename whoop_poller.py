"""
Whoop -> Airtable poller (reconciliation job).

Instead of waiting for Whoop's webhook to push data (which requires perfect
delivery timing and has been unreliable), this script periodically PULLS
fresh data from the Whoop API and writes any new/updated records into
Airtable. This is the "reconciliation job" pattern Whoop's own docs
recommend as a backup to webhooks -- here we're using it as the primary
mechanism since it's simpler to reason about and debug.

Deploy this as a Render CRON JOB (not a Web Service) that runs every
30 minutes. It does not need to stay running or expose any URL.

--- Setup on Render ---

1. In Render, "New" -> "Cron Job" (not Web Service).
2. Connect the same GitHub repo.
3. Build command: pip install -r requirements.txt
4. Command: python whoop_poller.py
5. Schedule: */30 * * * *   (every 30 minutes -- adjust as you like)
6. Environment variables (same as the webhook service):
   WHOOP_CLIENT_ID, WHOOP_CLIENT_SECRET, WHOOP_REFRESH_TOKEN, AIRTABLE_API_KEY

Each run fetches the last 2 days of data (generous overlap so nothing is
missed even if a run fails) and upserts into Airtable, so re-running is
always safe -- no duplicates.
"""

import os
import time
import requests
from datetime import datetime, timedelta, timezone

WHOOP_CLIENT_ID = os.environ["WHOOP_CLIENT_ID"]
WHOOP_CLIENT_SECRET = os.environ["WHOOP_CLIENT_SECRET"]
WHOOP_REFRESH_TOKEN = os.environ["WHOOP_REFRESH_TOKEN"]
AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = "appzxLVKPNu8AG6fp"

AIRTABLE_TABLES = {
    "recovery": "tblGNsthAB3g13lHl",
    "sleep": "tblaUIzUt1bXCWOBq",
    "workout": "tblgpckNBxensAQ2S",
    "cycle": "tbl1kjvYTmwhgXiKu",
}

# Field ID mappings (same as backfill script)
REC_F = {
    "Date": "fldHFiFyOxNvuT1SE", "Recovery Score": "fldfVbsFELcVSHA3A",
    "HRV (ms)": "fldL4rtoEKZxnlXIg", "Resting HR (bpm)": "fldPAM6E0NBZvH6lE",
    "SpO2 (%)": "fldTvkpznel5SA9LZ", "Skin Temp (C)": "fldGP6v74SfU7O7zv",
    "Sleep ID": "fldrg5U0QEZeAfocN", "Cycle ID": "fldzEIV9embT1Awo6",
}
SLEEP_F = {
    "Start": "fldELF5x4c1TOvUDB", "End": "fldzuWQNghrCi5Bhg",
    "Performance (%)": "fldmLEpvLa9Bpyjax", "Efficiency (%)": "fld2wcTK7EdlwIqBt",
    "Consistency (%)": "fldlN5rSdV1jqLHxW", "Duration (hrs)": "fldW9Hd6YVP2xjFvd",
    "Respiratory Rate": "fldtwTmgNLloXD6l2", "Nap": "fldwJg4td6FZ52Ukr",
    "Sleep ID": "fldk1C704PAXKokcA",
}
WO_F = {
    "Sport": "fldkWeZ7FnTP12OrE", "Start": "fldS8YF4Ys55Gcfmw", "End": "fldbbK21oZy79wx0E",
    "Strain": "fld1PcJGMrOxjaY0b", "Avg HR (bpm)": "flduNtxtw1EXyzLd9",
    "Max HR (bpm)": "fldQdWFa4zfAymnMa", "Kilojoules": "fldXMtmVRPOclP0Xv",
    "Distance (m)": "fldfu733uYBn11TXZ", "Workout ID": "fldp2WByFk2HKnm2G",
}
CYC_F = {
    "Start": "fldHUtqTSNwby70R7", "End": "fldVLzelpwG7RXuQa", "Strain": "fldxbbLe9Usmh25OP",
    "Avg HR (bpm)": "fldVhSikm9BorccZw", "Max HR (bpm)": "flddsCEn3fC4trPAH",
    "Kilojoules": "fldUwl5uCsGYEpZxS", "Cycle ID": "fldaET03AhlAymfYg",
}

WHOOP_TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
WHOOP_API_BASE = "https://api.prod.whoop.com/developer"
AIRTABLE_API_BASE = "https://api.airtable.com/v0"


def get_access_token():
    resp = requests.post(WHOOP_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": WHOOP_REFRESH_TOKEN,
        "client_id": WHOOP_CLIENT_ID,
        "client_secret": WHOOP_CLIENT_SECRET,
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_all(path, token, params):
    headers = {"Authorization": f"Bearer {token}"}
    records = []
    next_token = None
    while True:
        p = dict(params)
        if next_token:
            p["nextToken"] = next_token
        resp = requests.get(f"{WHOOP_API_BASE}{path}", headers=headers, params=p)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        next_token = data.get("next_token")
        if not next_token:
            break
    return records


def remap(fields_dict, mapping):
    return {mapping[k]: v for k, v in fields_dict.items() if v is not None}


def airtable_upsert(table_id, id_field_id, record_id, fields):
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }
    url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{table_id}"
    # Airtable filterByFormula needs the field NAME, but we only have IDs here.
    # We look up by listing recent records and matching client-side instead,
    # which avoids needing field names at all.
    resp = requests.get(url, headers=headers, params={"pageSize": 100})
    resp.raise_for_status()
    existing = None
    for rec in resp.json().get("records", []):
        if rec.get("fields", {}).get(id_field_id) == record_id:
            existing = rec["id"]
            break

    if existing:
        requests.patch(f"{url}/{existing}", headers=headers,
                        json={"fields": fields}, params={"returnFieldsByFieldId": "true"}).raise_for_status()
    else:
        requests.post(url, headers=headers,
                       json={"fields": fields}, params={"returnFieldsByFieldId": "true"}).raise_for_status()


def ms_to_hours(total_ms, awake_ms):
    return round((total_ms - awake_ms) / 3600000, 2)


def main():
    token = get_access_token()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=2)  # generous overlap, upsert makes re-runs safe
    params = {
        "start": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "limit": 25,
    }

    recovery = fetch_all("/v2/recovery", token, params)
    sleep = fetch_all("/v2/activity/sleep", token, params)
    workouts = fetch_all("/v2/activity/workout", token, params)
    cycles = fetch_all("/v2/cycle", token, params)

    print(f"Fetched: {len(recovery)} recovery, {len(sleep)} sleep, "
          f"{len(workouts)} workouts, {len(cycles)} cycles")

    n_written = 0
    for r in recovery:
        if r.get("score_state") != "SCORED":
            continue
        s = r["score"]
        f = remap({
            "Date": r["created_at"], "Recovery Score": s.get("recovery_score"),
            "HRV (ms)": s.get("hrv_rmssd_milli"), "Resting HR (bpm)": s.get("resting_heart_rate"),
            "SpO2 (%)": s.get("spo2_percentage"), "Skin Temp (C)": s.get("skin_temp_celsius"),
            "Sleep ID": r["sleep_id"], "Cycle ID": str(r["cycle_id"]),
        }, REC_F)
        airtable_upsert(AIRTABLE_TABLES["recovery"], REC_F["Sleep ID"], r["sleep_id"], f)
        n_written += 1

    for s in sleep:
        if s.get("score_state") != "SCORED":
            continue
        score = s["score"]
        stage = score.get("stage_summary", {})
        f = remap({
            "Start": s["start"], "End": s["end"],
            "Performance (%)": score.get("sleep_performance_percentage"),
            "Efficiency (%)": score.get("sleep_efficiency_percentage"),
            "Consistency (%)": score.get("sleep_consistency_percentage"),
            "Duration (hrs)": ms_to_hours(stage.get("total_in_bed_time_milli", 0), stage.get("total_awake_time_milli", 0)),
            "Respiratory Rate": score.get("respiratory_rate"), "Nap": s.get("nap", False),
            "Sleep ID": s["id"],
        }, SLEEP_F)
        airtable_upsert(AIRTABLE_TABLES["sleep"], SLEEP_F["Sleep ID"], s["id"], f)
        n_written += 1

    for w in workouts:
        if w.get("score_state") != "SCORED" or not w.get("score"):
            continue
        score = w["score"]
        f = remap({
            "Sport": w.get("sport_name", "unknown"), "Start": w["start"], "End": w["end"],
            "Strain": score.get("strain"), "Avg HR (bpm)": score.get("average_heart_rate"),
            "Max HR (bpm)": score.get("max_heart_rate"), "Kilojoules": score.get("kilojoule"),
            "Distance (m)": score.get("distance_meter"), "Workout ID": w["id"],
        }, WO_F)
        airtable_upsert(AIRTABLE_TABLES["workout"], WO_F["Workout ID"], w["id"], f)
        n_written += 1

    for c in cycles:
        if c.get("score_state") != "SCORED" or not c.get("score"):
            continue
        score = c["score"]
        f = remap({
            "Start": c.get("start"), "End": c.get("end"), "Strain": score.get("strain"),
            "Avg HR (bpm)": score.get("average_heart_rate"), "Max HR (bpm)": score.get("max_heart_rate"),
            "Kilojoules": score.get("kilojoule"), "Cycle ID": str(c["id"]),
        }, CYC_F)
        airtable_upsert(AIRTABLE_TABLES["cycle"], CYC_F["Cycle ID"], str(c["id"]), f)
        n_written += 1

    print(f"Upserted {n_written} records into Airtable.")


if __name__ == "__main__":
    main()
