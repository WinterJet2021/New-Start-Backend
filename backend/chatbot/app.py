# chatbot/app.py
import os
import json
import re
import sqlite3
import logging
from datetime import datetime, timezone
from contextlib import contextmanager
from collections import OrderedDict

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv, find_dotenv
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from normalizer import build_solver_payload_from_db_rows

# ------------------------------------
# Setup & Config
# ------------------------------------
env_path = find_dotenv()
load_dotenv(env_path)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
RASA_URL = os.getenv("RASA_URL", "http://localhost:5005/model/parse")
DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "nurse_prefs.db"))
PLACEHOLDER_NURSES = int(os.getenv("PLACEHOLDER_NURSES", "12"))

# FastAPI solver endpoint
SOLVER_URL = os.getenv("SOLVER_URL", "http://127.0.0.1:8000/solve")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NurseBot")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN) if LINE_CHANNEL_ACCESS_TOKEN else None
handler = WebhookHandler(LINE_CHANNEL_SECRET) if LINE_CHANNEL_SECRET else None

# Simple in-memory pending conversation state per nurse
# { nurse_id: {"kind": "...", "data": {...}} }
PENDING_STATE = {}

app = Flask(__name__)
CORS(app)

# ------------------------------------
# Database Helpers
# ------------------------------------
@contextmanager
def db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB Error: {e}")
        raise
    finally:
        conn.close()


def init_db():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS nurses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            line_user_id TEXT UNIQUE,
            name TEXT,
            level INTEGER,
            employment_type TEXT,
            unit TEXT
        )
        """
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS preferences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nurse_id INTEGER,
            preference_type TEXT,
            data TEXT,
            created_at TEXT
        )
        """
        )
    logger.info(f"Database initialized at: {DB_PATH}")


def drop_db():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS preferences")
        c.execute("DROP TABLE IF EXISTS nurses")


def seed_placeholders(count: int = PLACEHOLDER_NURSES):
    if count <= 0:
        return 0
    with db_connection() as conn:
        c = conn.cursor()
        # only seed if table exists and is empty
        row = c.execute("SELECT COUNT(*) FROM nurses").fetchone()
        if row and row[0] > 0:
            return 0
        import random, calendar

        today = datetime.now()
        year, month = today.year, today.month
        for i in range(1, count + 1):
            # diversify levels (about 40% level 2)
            level = 2 if (i % 5 in (0, 1)) else 1
            c.execute(
                "INSERT INTO nurses (line_user_id, name, level, employment_type, unit) VALUES (?, ?, ?, ?, ?)",
                (f"PLACEHOLDER_{i}", f"Nurse {i}", level, "full_time", "ER"),
            )
            nid = c.lastrowid
            # seed a shift preference and 1-2 day-offs
            shift = random.choice(["M", "A", "N"])
            # choose 3 days of week
            day_options = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            days = random.sample(day_options, k=3)
            priority = random.choice(["low", "medium", "high"])
            pref_shifts = json.dumps(
                {"shift": shift, "days": days, "priority": priority},
                ensure_ascii=False,
            )
            c.execute(
                "INSERT INTO preferences (nurse_id, preference_type, data, created_at) VALUES (?,?,?,?)",
                (
                    nid,
                    "preferred_shifts",
                    pref_shifts,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            # day offs
            for _ in range(random.randint(1, 2)):
                day = random.randint(1, max(28, calendar.monthrange(year, month)[1]))
                rank = random.choice([1, 2, 3])
                pref_dayoff = json.dumps(
                    {"date": f"{year:04}-{month:02}-{day:02}", "rank": rank},
                    ensure_ascii=False,
                )
                c.execute(
                    "INSERT INTO preferences (nurse_id, preference_type, data, created_at) VALUES (?,?,?,?)",
                    (
                        nid,
                        "preferred_days_off",
                        pref_dayoff,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
    logger.info(f"Seeded {count} placeholder nurses with preferences")
    return count


# Boot: init and seed if empty
init_db()
try:
    with db_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM nurses").fetchone()[0]
    if total == 0:
        seed_placeholders()
except sqlite3.OperationalError:
    # In case tables didn’t exist yet for some reason
    init_db()
    seed_placeholders()

# ------------------------------------
# DB Operations
# ------------------------------------
def normalize_employment_type(value):
    if not value:
        return None
    v = str(value).strip().lower()
    if v in ("full-time", "full time", "fulltime", "ft"):
        return "full_time"
    if v in ("part-time", "part time", "parttime", "pt"):
        return "part_time"
    if v in ("contract", "temp"):
        return "contract"
    return v


def get_or_create_nurse(line_user_id, name=None):
    try:
        with db_connection() as conn:
            c = conn.cursor()
            nurse = c.execute(
                "SELECT id FROM nurses WHERE line_user_id = ?", (line_user_id,)
            ).fetchone()
            if nurse:
                return nurse[0]
            c.execute(
                "INSERT INTO nurses (line_user_id, name, level, employment_type, unit) VALUES (?, ?, ?, ?, ?)",
                (line_user_id, name or "Unknown", 1, "full_time", "ER"),
            )
            new_id = c.lastrowid
            logger.info(f"New nurse created: ID={new_id}, LINE_ID={line_user_id}")
            return new_id
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            init_db()
            return get_or_create_nurse(line_user_id, name)
        raise
    except sqlite3.IntegrityError:
        with db_connection() as conn:
            row = conn.execute(
                "SELECT id FROM nurses WHERE line_user_id = ?", (line_user_id,)
            ).fetchone()
            if row:
                return row[0]
        raise


def update_nurse_details(nurse_id, level=None, unit=None, employment_type=None):
    emp_type = normalize_employment_type(employment_type) if employment_type else None
    with db_connection() as conn:
        c = conn.cursor()
        updates, params = [], []
        if level is not None:
            updates.append("level = ?")
            params.append(level)
        if emp_type:
            updates.append("employment_type = ?")
            params.append(emp_type)
        if unit:
            updates.append("unit = ?")
            params.append(unit)
        if updates:
            params.append(nurse_id)
            c.execute(
                f"UPDATE nurses SET {', '.join(updates)} WHERE id = ?", params
            )


def insert_preference(nurse_id, pref_type, data_dict):
    with db_connection() as conn:
        conn.execute(
            """
            INSERT INTO preferences (nurse_id, preference_type, data, created_at)
            VALUES (?, ?, ?, ?)
        """,
            (
                nurse_id,
                pref_type,
                json.dumps(data_dict, ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
    logger.info(
        f"Preference saved for nurse_id={nurse_id}: {pref_type} -> {data_dict}"
    )

# ------------------------------------
# Helpers for text normalization
# ------------------------------------
DAY_MAP = {
    "mon": "Mon",
    "monday": "Mon",
    "จันทร์": "Mon",
    "tue": "Tue",
    "tuesday": "Tue",
    "อังคาร": "Tue",
    "wed": "Wed",
    "wednesday": "Wed",
    "พุธ": "Wed",
    "thu": "Thu",
    "thursday": "Thu",
    "พฤหัส": "Thu",
    "พฤหัสบดี": "Thu",
    "fri": "Fri",
    "friday": "Fri",
    "ศุกร์": "Fri",
    "sat": "Sat",
    "saturday": "Sat",
    "เสาร์": "Sat",
    "sun": "Sun",
    "sunday": "Sun",
    "อาทิตย์": "Sun",
}
SHIFT_MAP = {
    "morning": "M",
    "เช้า": "M",
    "m": "M",
    "afternoon": "A",
    "บ่าย": "A",
    "a": "A",
    "night": "N",
    "กลางคืน": "N",
    "n": "N",
}

YES_TOKENS = {"yes", "y", "ok", "okay", "ใช่", "ค่ะ", "ครับ", "ได้"}
NO_TOKENS = {"no", "n", "ไม่", "ไม่ค่ะ", "ไม่ครับ", "ไม่ใช่"}


def normalize_day_list(raw_days):
    if not raw_days:
        return []
    items = raw_days if isinstance(raw_days, list) else re.split(r"[,\s/]+", raw_days)
    out = []
    for token in items:
        t = str(token).strip().lower()
        out.append(DAY_MAP.get(t, t.title()[:3]))
    # dedupe, keep order
    seen, result = set(), []
    for d in out:
        if d not in seen:
            seen.add(d)
            result.append(d)
    return result


def set_pending(nurse_id, kind, data):
    """Store the next expected step for this nurse."""
    PENDING_STATE[nurse_id] = {"kind": kind, "data": data}


def clear_pending(nurse_id):
    PENDING_STATE.pop(nurse_id, None)


def parse_dayoff_date(raw):
    """
    Convert various date formats from Rasa into ISO + human-readable string.
    - Supports: '2025-11-17', '2025-11-17T00:00:00+07:00'
    - Fallback: day number in current/next month.
    """
    if not raw and raw != 0:
        return None, None

    s = str(raw).strip()

    # ISO-like 'YYYY-MM-DD...'
    m_iso = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m_iso:
        d = datetime.strptime(m_iso.group(1), "%Y-%m-%d").date()
        return d.isoformat(), d.strftime("%A, %d %B %Y")

    # Fallback: just a day number
    m = re.search(r"(\d{1,2})", s)
    if not m:
        return None, None

    day = int(m.group(1))
    now = datetime.now()
    try:
        if day < now.day:
            month = now.month + 1 if now.month < 12 else 1
            year = now.year if month != 1 else now.year + 1
        else:
            month = now.month
            year = now.year
        date_obj = datetime(year, month, day).date()
        return date_obj.isoformat(), date_obj.strftime("%A, %d %B %Y")
    except Exception:
        return None, None


def handle_pending_message(nurse_id, user_message, line_name):
    """
    If this nurse has a pending confirmation, consume the message here.
    Returns a reply string, or None if there is no pending state.
    """
    state = PENDING_STATE.get(nurse_id)
    if not state:
        return None

    kind = state["kind"]
    data = state.get("data", {})
    low = user_message.strip().lower()

    # Confirm DAY-OFF request
    if kind == "confirm_day_off":
        if low in YES_TOKENS:
            clear_pending(nurse_id)
            insert_preference(
                nurse_id,
                "preferred_days_off",
                {"date": data.get("date_iso"), "rank": data.get("rank", 2)},
            )
            pretty = data.get("pretty_date") or data.get("date_iso") or "that day"
            return (
                f"Your request for {pretty} has been submitted. "
                "You will receive an update after supervisor review.💙"
            )

        if low in NO_TOKENS:
            clear_pending(nurse_id)
            pretty = data.get("pretty_date") or data.get("date_iso") or "that day"
            return f"Okay {line_name}, I will not submit the day-off request for {pretty}."

        pretty = data.get("pretty_date") or data.get("date_iso") or "this request"
        return f"Please reply Yes or No to confirm your day-off request for {pretty}."

    # Confirm SHIFT PREFERENCE update
    if kind == "confirm_shift_pref":
        shift_code = (data.get("shift") or "M").upper()
        pretty_shift = {"M": "morning", "A": "afternoon", "N": "night"}.get(
            shift_code, "that"
        )

        if low in YES_TOKENS:
            clear_pending(nurse_id)
            insert_preference(
                nurse_id,
                "preferred_shifts",
                {
                    "shift": shift_code,
                    "days": data.get("days") or [],
                    "priority": data.get("priority", "medium"),
                },
            )
            return f"{pretty_shift.capitalize()} preference Saved ✅"

        if low in NO_TOKENS:
            clear_pending(nurse_id)
            return f"Okay {line_name}, I did not change your shift preferences."

        return f"Please reply Yes or No to confirm your {pretty_shift} shift preference."

    # Unknown kind
    clear_pending(nurse_id)
    return None

# ------------------------------------
# LINE Webhook + Rasa Integration (dev-safe)
# ------------------------------------
@app.post("/callback_test")
def callback_test():
    data = request.get_json(force=True) or {}
    text = data.get("text") or (((data.get("events") or [{}])[0]).get("message") or {}).get(
        "text", ""
    )
    user_id = data.get("user_id") or (
        ((data.get("events") or [{}])[0]).get("source") or {}
    ).get("userId", "DEV_USER")

    try:
        nurse_id = get_or_create_nurse(user_id, "Dev User")
    except Exception as e:
        logger.error(f"Dev test nurse create failed: {e}")
        return jsonify({"ok": False, "error": "nurse_create"}), 500

    # First handle any pending confirmation (Yes/No)
    pending_reply = handle_pending_message(nurse_id, text, "Dev User")
    if pending_reply:
        return jsonify(
            {
                "ok": True,
                "pending_handled": True,
                "reply": pending_reply,
                "intent": "pending_confirmation",
                "entities": {},
            }
        )

    # Try Rasa; on failure, fall back to simple heuristics
    rasa_data = None
    try:
        rasa_resp = requests.post(RASA_URL, json={"text": text}, timeout=5)
        rasa_resp.raise_for_status()
        rasa_data = rasa_resp.json()
    except Exception as e:
        logger.warning(f"Rasa unreachable, using heuristic fallback: {e}")
        low = text.lower()
        entities = {}
        if any(k in low for k in ["ลางาน", "หยุด", "day off", "leave"]):
            m = re.search(r"(\d{1,2})", text)
            entities["date"] = m.group(1) if m else None
            if any(k in low for k in ["urgent", "critical", "สำคัญ", "ด่วน"]):
                entities["rank"] = 3
            intent = "add_day_off"
        else:
            if any(k in low for k in ["เช้า", "morning"]):
                entities["shift"] = "morning"
            elif any(k in low for k in ["บ่าย", "afternoon"]):
                entities["shift"] = "afternoon"
            elif any(k in low for k in ["กลางคืน", "ดึก", "night"]):
                entities["shift"] = "night"
            days = []
            for k in [
                "จันทร์",
                "อังคาร",
                "พุธ",
                "พฤหัส",
                "ศุกร์",
                "เสาร์",
                "อาทิตย์",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ]:
                if k in low:
                    days.append(k)
            if days:
                entities["days"] = days
            intent = "add_shift_preference" if "shift" in entities else "update_profile"
        reply_text = process_intent(intent, nurse_id, entities, "Dev User")
        return jsonify(
            {
                "ok": True,
                "intent": intent,
                "entities": entities,
                "reply": reply_text,
                "fallback": True,
            }
        )

    # Parse Rasa entities structure
    raw_ents = rasa_data.get("entities", []) if rasa_data else []
    entities = {}
    for ent in raw_ents:
        name = ent.get("entity")
        val = ent.get("value")
        if not name:
            continue
        if name == "days":
            entities.setdefault(name, []).append(val)
        else:
            entities[name] = val

    intent = rasa_data.get("intent", {}).get("name") if rasa_data else None
    if not intent:
        insert_preference(nurse_id, "unrecognized", {"text": text})
        return jsonify({"ok": True, "reply": "nlu_fallback", "saved": False})

    reply_text = process_intent(intent, nurse_id, entities, "Dev User")
    return jsonify(
        {"ok": True, "intent": intent, "entities": entities, "reply": reply_text}
    )


# Alias dev path for demo UI compatibility
@app.post("/dev/callback_test")
def dev_callback_test():
    return callback_test()


# Real LINE webhook (guard for missing creds)
if handler and line_bot_api:

    @app.post("/callback")
    def callback():
        signature = request.headers.get("X-Line-Signature", "")
        body = request.get_data(as_text=True)
        try:
            handler.handle(body, signature)
        except Exception as e:
            logger.error(f"Error handling event: {e}")
            return str(e), 500
        return "OK"

    @handler.add(MessageEvent, message=TextMessage)
    def handle_message(event):
        user_id = event.source.user_id
        user_message = event.message.text
        try:
            profile = line_bot_api.get_profile(user_id)
            line_name = profile.display_name
        except Exception:
            line_name = "Unknown"
        nurse_id = get_or_create_nurse(user_id, line_name)

        # 1) If we have a pending Yes/No confirmation, handle it here
        pending_reply = handle_pending_message(nurse_id, user_message, line_name)
        if pending_reply:
            safe_reply(event, pending_reply)
            return

        # 2) Otherwise, call Rasa as usual
        try:
            rasa_resp = requests.post(
                RASA_URL, json={"text": user_message}, timeout=5
            )
            rasa_resp.raise_for_status()
            rasa_data = rasa_resp.json()
        except Exception as e:
            logger.error(f"Error contacting Rasa: {e}")
            safe_reply(event, "ขอโทษค่ะ ระบบไม่สามารถตอบกลับได้ในตอนนี้")
            return

        intent = rasa_data.get("intent", {}).get("name")
        confidence = rasa_data.get("intent", {}).get("confidence", 0)
        entities = {
            e.get("entity"): e.get("value")
            for e in rasa_data.get("entities", [])
            if "entity" in e
        }

        if not intent or confidence < 0.5 or intent == "nlu_fallback":
            insert_preference(nurse_id, "unrecognized", {"text": user_message})
            safe_reply(
                event,
                f"ขอโทษค่ะ {line_name} ฉันไม่เข้าใจข้อความของคุณ กรุณาลองใหม่อีกครั้งนะคะ",
            )
            return

        reply = process_intent(intent, nurse_id, entities, line_name)
        safe_reply(event, reply)


def safe_reply(event, text):
    if not (handler and line_bot_api):
        return
    try:
        text = text or "ขอโทษค่ะ ระบบไม่สามารถตอบกลับได้ในตอนนี้"
        line_bot_api.reply_message(
            event.reply_token, TextSendMessage(text=text)
        )
    except Exception as e:
        logger.error(f"LINE reply failed: {e}")


# ------------------------------------
# Intent Handling
# ------------------------------------
def process_intent(intent, nurse_id, entities, line_name):
    if intent == "update_profile":
        level = entities.get("level")
        employment_type = entities.get("employment_type")
        unit = entities.get("unit")

        level_value = None
        if level:
            m = re.search(r"\d+", str(level))
            if m:
                level_value = int(m.group())

        update_nurse_details(
            nurse_id,
            level=level_value,
            employment_type=employment_type,
            unit=unit,
        )
        return (
            f"Got it, {line_name}! Profile updated "
            f"(Level {level_value or 1}, {employment_type or 'full_time'}, Unit: {unit or 'ER'})."
        )

    elif intent == "add_shift_preference":
        # User messages like: "I prefer morning shifts next month."
        shift = SHIFT_MAP.get(str(entities.get("shift", "")).lower(), "M")
        days = normalize_day_list(entities.get("days", []))
        priority = entities.get("priority", "medium")

        # Do not save yet – ask for confirmation like the mockup
        set_pending(
            nurse_id,
            "confirm_shift_pref",
            {"shift": shift, "days": days, "priority": priority},
        )

        pretty_shift = {"M": "morning", "A": "afternoon", "N": "night"}.get(
            shift, "that"
        )

        return (
            f"Preference updated:\n✴ {pretty_shift.capitalize()} shifts preferred\n\n"
            "Please reply Yes or No to confirm."
        )

    elif intent == "add_day_off":
        raw_day = entities.get("date")
        rank = int(entities.get("rank", 2))

        if not raw_day:
            # First turn: ask which date
            return "Of course. Which date would you like to take off?"

        date_iso, pretty_date = parse_dayoff_date(raw_day)
        if not date_iso:
            return (
                "Sorry, I couldn't understand the date. "
                "Please reply with a date like '17 November' or 'this Sunday'."
            )

        set_pending(
            nurse_id,
            "confirm_day_off",
            {"date_iso": date_iso, "pretty_date": pretty_date, "rank": rank},
        )

        return (
            f"I detected: {pretty_date}\n"
            "Do you want to submit this day-off request?\n\nYes / No"
        )

    # default: record unrecognized
    insert_preference(nurse_id, "unrecognized", {"note": "intent not handled"})
    return "Saved."


# ------------------------------------
# Export All
# ------------------------------------
@app.get("/export_all")
def export_all():
    """
    Export all nurses and preferences in optimizer-ready JSON format.
    """
    with db_connection() as conn:
        nurses = conn.execute(
            "SELECT id, name, level, employment_type, unit FROM nurses"
        ).fetchall()
        prefs = conn.execute(
            "SELECT nurse_id, preference_type, data FROM preferences"
        ).fetchall()

    nurse_dict = {}
    for n in nurses:
        nurse_id = n[0]
        nurse_dict[nurse_id] = OrderedDict(
            [
                ("id", f"N{nurse_id:03}"),
                ("name", n[1] or f"Nurse {nurse_id}"),
                ("level", n[2] or 1),
                ("employment_type", n[3] or "full_time"),
                ("unit", n[4] or "ER"),
                (
                    "preferences",
                    {
                        "preferred_shifts": [],
                        "preferred_days_off": [],
                    },
                ),
            ]
        )

    for nurse_id, pref_type, data in prefs:
        try:
            parsed = json.loads(data)
            if (
                nurse_id in nurse_dict
                and pref_type in nurse_dict[nurse_id]["preferences"]
            ):
                nurse_dict[nurse_id]["preferences"][pref_type].append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse preference for {nurse_id}: {e}")

    sorted_nurses = [nurse_dict[k] for k in sorted(nurse_dict.keys())]
    return app.response_class(
        response=json.dumps({"nurses": sorted_nurses}, ensure_ascii=False, indent=2),
        mimetype="application/json",
    )


# ------------------------------------
# Normalized payload for solver
# ------------------------------------
@app.get("/normalize_for_solver")
def normalize_for_solver():
    """
    Build a JSON payload that is ready to POST to the FastAPI /solve endpoint.

    Optional query params:
      - start_date=YYYY-MM-DD (default: today)
      - horizon_days=int (default: 30)
      - morning_demand=int (default: 6)
      - evening_demand=int (default: 6)
      - night_demand=int (default: 2)
    """

    def _safe_int(name: str, default: int) -> int:
        """Convert query param to int, robust against None / '' / junk."""
        raw = request.args.get(name, None)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return int(raw)
        except ValueError:
            # If it's invalid, treat as bad request
            raise ValueError(f"Invalid value for {name}: {raw!r}")

    try:
        start_date = request.args.get("start_date") or None
        horizon_days = _safe_int("horizon_days", 30)
        morning_demand = _safe_int("morning_demand", 6)
        evening_demand = _safe_int("evening_demand", 6)
        night_demand = _safe_int("night_demand", 2)
    except ValueError as e:
        return jsonify({"ok": False, "error": "invalid query param", "detail": str(e)}), 400

    with db_connection() as conn:
        nurses_rows = conn.execute(
            "SELECT id, name, level, employment_type, unit FROM nurses"
        ).fetchall()
        prefs_rows = conn.execute(
            "SELECT nurse_id, preference_type, data FROM preferences"
        ).fetchall()

    payload = build_solver_payload_from_db_rows(
        nurses_rows=nurses_rows,
        prefs_rows=prefs_rows,
        start_date=start_date,
        horizon_days=horizon_days,
        morning_demand=morning_demand,
        evening_demand=evening_demand,
        night_demand=night_demand,
    )

    return app.response_class(
        response=json.dumps(payload, ensure_ascii=False, indent=2),
        mimetype="application/json",
    )


# Simple alias to fix old /dev/solve_preview calls
@app.get("/dev/solve_preview")
def dev_solve_preview():
    """
    Preview the normalized payload (same as /normalize_for_solver).
    """
    return normalize_for_solver()


# ------------------------------------
# Call FastAPI /solve
# ------------------------------------
@app.post("/dev/solve")
def dev_solve():
    """
    Build payload from SQLite, call the FastAPI /solve endpoint,
    and return both the request body and solver response.

    Optional query params (same as /normalize_for_solver):
      - start_date=YYYY-MM-DD (default: today)
      - horizon_days=int (default: 7)
      - morning_demand=int (default: 4)
      - evening_demand=int (default: 3)
      - night_demand=int (default: 2)
    """
    try:
        start_date = request.args.get("start_date") or None
        horizon_days = int(request.args.get("horizon_days", 30))
        morning_demand = int(request.args.get("morning_demand", 6))
        evening_demand = int(request.args.get("evening_demand", 6))
        night_demand = int(request.args.get("night_demand", 2))
    except ValueError:
        return jsonify({"ok": False, "error": "invalid query param"}), 400

    with db_connection() as conn:
        nurses_rows = conn.execute(
            "SELECT id, name, level, employment_type, unit FROM nurses"
        ).fetchall()
        prefs_rows = conn.execute(
            "SELECT nurse_id, preference_type, data FROM preferences"
        ).fetchall()

    normalized = build_solver_payload_from_db_rows(
        nurses_rows=nurses_rows,
        prefs_rows=prefs_rows,
        start_date=start_date,
        horizon_days=horizon_days,
        morning_demand=morning_demand,
        evening_demand=evening_demand,
        night_demand=night_demand,
    )

    if isinstance(normalized, dict) and {"nurses", "days", "shifts", "demand"}.issubset(
        normalized.keys()
    ):
        meta = None
        solve_body = normalized
    else:
        if not isinstance(normalized, dict):
            return jsonify(
                {"ok": False, "error": "normalizer_unexpected_type"}
            ), 500

        meta = normalized.get("meta")
        solve_body = normalized.get("payload") or {}

    if not isinstance(solve_body, dict) or not {
        "nurses",
        "days",
        "shifts",
        "demand",
    }.issubset(solve_body.keys()):
        return jsonify(
            {"ok": False, "error": "normalizer_shape_invalid", "payload": normalized}
        ), 500

    try:
        resp = requests.post(SOLVER_URL, json=solve_body, timeout=60)
        resp.raise_for_status()
        solver_json = resp.json()
    except Exception as e:
        logger.error(f"Error calling solver at {SOLVER_URL}: {e}")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "solver_call_failed",
                    "detail": str(e),
                    "solver_url": SOLVER_URL,
                }
            ),
            502,
        )

    return jsonify(
        {
            "ok": True,
            "solver_url": SOLVER_URL,
            "meta": meta,
            "solve_request": solve_body,
            "solve_response": solver_json,
        }
    )


# ------------------------------------
# Dev/Health Endpoints
# ------------------------------------
@app.get("/health")
def health():
    ok, err = True, ""
    try:
        with db_connection() as conn:
            conn.execute("SELECT 1")
    except Exception as e:
        ok, err = False, str(e)
    return jsonify({"ok": ok, "db_path": DB_PATH, "error": err})


@app.post("/dev/initdb")
def dev_initdb():
    try:
        init_db()
        return jsonify({"ok": True, "db_path": DB_PATH})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/dev/dbinfo")
def dev_dbinfo():
    try:
        with db_connection() as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
            ]
            counts = {}
            for t in tables:
                try:
                    counts[t] = conn.execute(
                        f"SELECT COUNT(*) FROM {t}"
                    ).fetchone()[0]
                except Exception:
                    counts[t] = "n/a"
        return jsonify(
            {"ok": True, "db_path": DB_PATH, "tables": tables, "counts": counts}
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/dev/resetdb")
def dev_resetdb():
    try:
        drop_db()
        init_db()
        seeded = seed_placeholders()
        with db_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM nurses").fetchone()[0]
        return jsonify({"ok": True, "seeded": seeded, "total_nurses": total})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/dev/seed")
def dev_seed():
    try:
        count = int(request.args.get("count", PLACEHOLDER_NURSES))
        with db_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM nurses").fetchone()[0]
        seeded = 0
        if total == 0:
            seeded = seed_placeholders(count)
        return jsonify(
            {"ok": True, "seeded": seeded, "existing_total": total}
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ------------------------------------
# Run App
# ------------------------------------
if __name__ == "__main__":
    logger.info(
        f"Starting NurseBot Flask app on port 8070 | DB: {DB_PATH} | SOLVER_URL: {SOLVER_URL}"
    )
    app.run(host="0.0.0.0", port=8070, debug=True)