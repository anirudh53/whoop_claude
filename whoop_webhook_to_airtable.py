"""
Whoop webhook receiver -> Airtable.

This is a small Flask app you deploy somewhere always-on (a free-tier cloud
function, a $5/mo VPS, or even a Raspberry Pi at home with port forwarding).
It listens for Whoop's webhook events (workout.updated, sleep.updated,
recovery.updated, cycle.updated), fetches the full record from Whoop's v2
API, and writes it into your "Whoop Data" Airtable base.

Once this is running continuously, new workouts/sleep/recovery show up in
Airtable automatically, and you can ask Claude for insights anytime --
Claude reads live from Airtable, no manual export needed.

--- Setup ---

1. pip install flask requests

2. This script reads secrets from environment variables -- NOT hardcoded
   in this file. Set these in your hosting provider's dashboard (e.g.
   Render's "Environment" tab), never in the code itself:
   - WHOOP_CLIENT_ID / WHOOP_CLIENT_SECRET (from developer.whoop.com)
   - WHOOP_REFRESH_TOKEN (from whoop_tokens.json, produced by whoop_export.py)
   - AIRTABLE_API_KEY (from airtable.com/create/tokens, scoped to the
     "Whoop Data" base with data.records:read + data.records:write)
   AIRTABLE_BASE_ID is not secret and stays hardcoded below.

3. This MUST run on a real always-on host with a public HTTPS URL --
   Colab, Jupyter, or your own laptop will NOT work, since Whoop needs to
   reach it any time an event fires. Easiest free options:
   - Render.com / Railway.app free tier (just point it at this file)
   - Google Cloud Run / AWS Lambda + API Gateway
   - A home server + Cloudflare Tunnel (avoids router port-forwarding)

4. In the WHOOP Developer Dashboard, add your deployed URL + "/webhook" as
   a Webhook URL (e.g. https://your-app.onrender.com/webhook), model
   version v2.

5. Whoop will now POST to /webhook every time an event is scored. This
   script verifies the signature, fetches the full record, and upserts it
   into the matching Airtable table.
"""

import os
import hashlib
import hmac
import base64
import json
import time
import requests
from flask import Flask, request, jsonify

# ---- Secrets come from environment variables, set in your hosting ----
# ---- provider's dashboard -- never hardcode them here. ----
WHOOP_CLIENT_ID = os.environ["WHOOP_CLIENT_ID"]
WHOOP_CLIENT_SECRET = os.environ["WHOOP_CLIENT_SECRET"]
WHOOP_REFRESH_TOKEN = os.environ["WHOOP_REFRESH_TOKEN"]
WHOOP_CLIENT_SECRET_FOR_WEBHOOK_VERIFY = WHOOP_CLIENT_SECRET  # Whoop signs webhooks with your client secret

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = "appzxLVKPNu8AG6fp"  # not secret, safe to hardcode

AIRTABLE_TABLES = {
    "recovery": "tblGNsthAB3g13lHl",
    "sleep": "tblaUIzUt1bXCWOBq",
    "workout": "tblgpckNBxensAQ2S",
    "cycle": "tbl1kjvYTmwhgXiKu",
}

WHOOP_TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
WHOOP_API_BASE = "https://api.prod.whoop.com/developer"
AIRTABLE_API_BASE = "https://api.airtable.com/v0"

app = Flask(__name__)

_access_token_cache = {"token": None, "expires_at": 0}


def get_whoop_access_token():
    """Get a fresh Whoop access token using the stored refresh token."""
    if _access_token_cache["token"] and time.time() < _access_token_cache["expires_at"]:
        return _access_token_cache["token"]

    resp = requests.post(WHOOP_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": WHOOP_REFRESH_TOKEN,
        "client_id": WHOOP_CLIENT_ID,
        "client_secret": WHOOP_CLIENT_SECRET,
    })
    resp.raise_for_status()
    data = resp.json()
    _access_token_cache["token"] = data["access_token"]
    _access_token_cache["expires_at"] = time.time() + data.get("expires_in", 3600) - 60
    return data["access_token"]


def verify_webhook_signature(raw_body, timestamp, signature):
    """Whoop signs webhooks as base64(hmac_sha256(client_secret, timestamp + body))."""
    message = timestamp.encode() + raw_body
    expected = base64.b64encode(
        hmac.new(WHOOP_CLIENT_SECRET_FOR_WEBHOOK_VERIFY.encode(), message, hashlib.sha256).digest()
    ).decode()
    return hmac.compare_digest(expected, signature)


def airtable_upsert(table_id, id_field_name, record_id, fields):
    """Create or update an Airtable record, keyed on an ID field, so re-sent
    webhooks (Whoop may retry) don't create duplicates."""
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    # Look for an existing record with this ID
    search_url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{table_id}"
    resp = requests.get(search_url, headers=headers, params={
        "filterByFormula": f"{{{id_field_name}}} = '{record_id}'",
        "maxRecords": 1,
    })
    resp.raise_for_status()
    existing = resp.json().get("records", [])

    if existing:
        rec_id = existing[0]["id"]
        requests.patch(f"{search_url}/{rec_id}", headers=headers,
                        json={"fields": fields}).raise_for_status()
    else:
        requests.post(search_url, headers=headers,
                       json={"fields": fields}).raise_for_status()


def ms_to_hours(total_ms, awake_ms):
    return round((total_ms - awake_ms) / 3600000, 2)


@app.route("/webhook", methods=["POST"])
def webhook():
    raw_body = request.get_data()
    timestamp = request.headers.get("X-WHOOP-Signature-Timestamp", "")
    signature = request.headers.get("X-WHOOP-Signature", "")

    if not verify_webhook_signature(raw_body, timestamp, signature):
        return jsonify({"error": "invalid signature"}), 401

    payload = request.get_json()
    event_type = payload.get("type", "")
    user_id = payload.get("user_id")
    resource_id = payload.get("id")

    token = get_whoop_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    if event_type.startswith("recovery"):
        r = requests.get(f"{WHOOP_API_BASE}/v2/recovery", headers=headers,
                          params={"limit": 1}).json()
        rec = next((x for x in r.get("records", []) if x.get("sleep_id") == resource_id or True), None)
        if rec and rec.get("score"):
            s = rec["score"]
            airtable_upsert(AIRTABLE_TABLES["recovery"], "Sleep ID", rec["sleep_id"], {
                "Date": rec["created_at"],
                "Recovery Score": s.get("recovery_score"),
                "HRV (ms)": s.get("hrv_rmssd_milli"),
                "Resting HR (bpm)": s.get("resting_heart_rate"),
                "SpO2 (%)": s.get("spo2_percentage"),
                "Skin Temp (C)": s.get("skin_temp_celsius"),
                "Sleep ID": rec["sleep_id"],
                "Cycle ID": str(rec["cycle_id"]),
            })

    elif event_type.startswith("sleep"):
        s = requests.get(f"{WHOOP_API_BASE}/v2/activity/sleep/{resource_id}",
                          headers=headers).json()
        if s.get("score"):
            score = s["score"]
            stage = score.get("stage_summary", {})
            airtable_upsert(AIRTABLE_TABLES["sleep"], "Sleep ID", s["id"], {
                "Start": s["start"],
                "End": s["end"],
                "Performance (%)": score.get("sleep_performance_percentage"),
                "Efficiency (%)": score.get("sleep_efficiency_percentage"),
                "Consistency (%)": score.get("sleep_consistency_percentage"),
                "Duration (hrs)": ms_to_hours(
                    stage.get("total_in_bed_time_milli", 0),
                    stage.get("total_awake_time_milli", 0)),
                "Respiratory Rate": score.get("respiratory_rate"),
                "Nap": s.get("nap", False),
                "Sleep ID": s["id"],
            })

    elif event_type.startswith("workout"):
        w = requests.get(f"{WHOOP_API_BASE}/v2/activity/workout/{resource_id}",
                          headers=headers).json()
        if w.get("score"):
            score = w["score"]
            airtable_upsert(AIRTABLE_TABLES["workout"], "Workout ID", w["id"], {
                "Sport": w.get("sport_name", "unknown"),
                "Start": w["start"],
                "End": w["end"],
                "Strain": score.get("strain"),
                "Avg HR (bpm)": score.get("average_heart_rate"),
                "Max HR (bpm)": score.get("max_heart_rate"),
                "Kilojoules": score.get("kilojoule"),
                "Distance (m)": score.get("distance_meter"),
                "Workout ID": w["id"],
            })

    elif event_type.startswith("cycle"):
        c = requests.get(f"{WHOOP_API_BASE}/v2/cycle/{resource_id}",
                          headers=headers).json()
        if c.get("score"):
            score = c["score"]
            airtable_upsert(AIRTABLE_TABLES["cycle"], "Cycle ID", str(c["id"]), {
                "Start": c["start"],
                "End": c.get("end"),
                "Strain": score.get("strain"),
                "Avg HR (bpm)": score.get("average_heart_rate"),
                "Max HR (bpm)": score.get("max_heart_rate"),
                "Kilojoules": score.get("kilojoule"),
                "Cycle ID": str(c["id"]),
            })

    return jsonify({"status": "ok"}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "alive"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
