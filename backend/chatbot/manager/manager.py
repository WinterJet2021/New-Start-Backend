# chatbot/manager/manager.py
# Build optimizer-ready JSON from chatbot DB or webhook, with strong normalization

from __future__ import annotations

import os, re, json, sqlite3, requests
from datetime import datetime, timedelta
from pathlib import Path
from collections import OrderedDict
from typing import Dict, Any, List

TARGET_MIN_NURSES = int(os.getenv("MANAGER_MIN_NURSES", "16"))

# ----------------------------
# Normalization helpers
# ----------------------------
DAY_MAP = {
    "mon": "Mon", "monday": "Mon", "จันทร์": "Mon",
    "tue": "Tue", "tuesday": "Tue", "อังคาร": "Tue",
    "wed": "Wed", "wednesday": "Wed", "พุธ": "Wed",
    "thu": "Thu", "thursday": "Thu", "พฤหัส": "Thu", "พฤหัสบดี": "Thu",
    "fri": "Fri", "friday": "Fri", "ศุกร์": "Fri",
    "sat": "Sat", "saturday": "Sat", "เสาร์": "Sat",
    "sun": "Sun", "sunday": "Sun", "อาทิตย์": "Sun",
}
SHIFT_MAP = {
    "morning": "M", "เช้า": "M", "m": "M",
    "afternoon": "A", "บ่าย": "A", "a": "A", "evening": "A", "eve": "A",
    "night": "N", "กลางคืน": "N", "ดึก": "N", "n": "N"
}


def _norm_days(raw: Any) -> List[str]:
    if not raw:
        return []
    items = raw if isinstance(raw, list) else re.split(r"[,/\s]+", str(raw))
    out = []
    for t in items:
        k = str(t).strip().lower()
        out.append(DAY_MAP.get(k, k[:3].title()))
    seen, result = set(), []
    for d in out:
        if d and d not in seen:
            seen.add(d)
            result.append(d)
    return result


def _norm_shift(raw: Any) -> str:
    if not raw:
        return "M"
    k = str(raw).strip().lower()
    if k in SHIFT_MAP:
        return SHIFT_MAP[k]
    c = k[0].upper()
    return c if c in ("M", "A", "N") else "M"


def _norm_date(raw: Any) -> str | None:
    if not raw and raw != 0:
        return None
    s = str(raw)
    # Try YYYY-MM-DD first
    try:
        return datetime.strptime(s, "%Y-%m-%d").date().isoformat()
    except Exception:
        pass
    # Try DD or D of current month
    m = re.search(r"(\d{1,2})", s)
    if not m:
        return None
    day = int(m.group(1))
    now = datetime.now()
    try:
        return datetime(now.year, now.month, day).date().isoformat()
    except Exception:
        return None


# ----------------------------
# Fetchers
# ----------------------------

def fetch_webhook_json(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=10, headers={"Cache-Control": "no-cache"})
    r.raise_for_status()
    return r.json()


def fetch_sqlite_json(db_path: Path) -> Dict[str, Any]:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        nurses = conn.execute(
            "SELECT id, name, level, employment_type, unit FROM nurses"
        ).fetchall()
        prefs  = conn.execute(
            "SELECT nurse_id, preference_type, data FROM preferences"
        ).fetchall()

    nd: Dict[int, Dict[str, Any]] = {}
    for n in nurses:
        nid_int = int(n["id"])
        nd[nid_int] = OrderedDict([
            ("id", f"N{nid_int:03}"),
            ("name", n["name"] or f"Nurse {nid_int}"),
            ("level", int(n["level"]) if n["level"] is not None else 1),
            ("employment_type", n["employment_type"] or "full_time"),
            ("unit", n["unit"] or "ER"),
            ("preferences", {"preferred_shifts": [], "preferred_days_off": []})
        ])

    for row in prefs:
        nid_int = int(row["nurse_id"])
        if nid_int not in nd:  # skip orphans
            continue
        ptype = row["preference_type"]
        try:
            payload = json.loads(row["data"]) if row["data"] else {}
        except Exception:
            payload = {}

        if ptype == "preferred_shifts":
            payload = {
                "shift": _norm_shift(payload.get("shift")),
                "days": _norm_days(payload.get("days")),
                "priority": str(payload.get("priority", "medium")).lower(),
            }
        elif ptype == "preferred_days_off":
            payload = {
                "date": _norm_date(payload.get("date")),
                "rank": int(payload.get("rank", 2))
            }

        if ptype in nd[nid_int]["preferences"]:
            nd[nid_int]["preferences"][ptype].append(payload)

    return {"nurses": [nd[k] for k in sorted(nd.keys())]}


# ----------------------------
# Enrichment (padding with realistic prefs/levels)
# ----------------------------

def ensure_min_nurses(cfg: Dict[str, Any], min_nurses: int = TARGET_MIN_NURSES) -> None:
    nurses: List[Dict[str, Any]] = list(cfg.get("nurses") or [])

    # normalize existing
    for n in nurses:
        prefs = n.get("preferences") or {}
        n["preferences"] = {
            "preferred_shifts": [
                {
                    "shift": _norm_shift(p.get("shift")),
                    "days": _norm_days(p.get("days")),
                    "priority": str(p.get("priority", "medium")).lower(),
                }
                for p in (prefs.get("preferred_shifts") or [])
            ],
            "preferred_days_off": [
                {"date": _norm_date(p.get("date")), "rank": int(p.get("rank", 2))}
                for p in (prefs.get("preferred_days_off") or [])
            ],
        }
        n["level"] = int(n.get("level", 1))
        n["employment_type"] = n.get("employment_type") or "full_time"
        n["unit"] = n.get("unit") or "ER"

    existing_ids = {n.get("id") for n in nurses if n.get("id")}
    # find next idx
    max_idx = 0
    for nid in existing_ids:
        try:
            if isinstance(nid, str) and nid.startswith("N"):
                max_idx = max(max_idx, int(nid[1:]))
        except Exception:
            pass

    import random
    day_options = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    while len(nurses) < min_nurses:
        max_idx += 1
        nid = f"N{max_idx:03}"
        level = 2 if (len(nurses) % 5 in (0, 1)) else 1
        shift = random.choice(["M","A","N"])
        days = random.sample(day_options, 3)
        priority = random.choice(["low","medium","high"])
        nurses.append({
            "id": nid,
            "name": f"Nurse {nid}",
            "level": level,
            "employment_type": "full_time",
            "unit": "ER",
            "preferences": {
                "preferred_shifts": [{"shift": shift, "days": days, "priority": priority}],
                "preferred_days_off": [{"date": _norm_date(15), "rank": 2}],
            }
        })
        existing_ids.add(nid)

    cfg["nurses"] = nurses


# ----------------------------
# Reference + coverage synth
# ----------------------------

def _default_reference() -> Dict[str, Any]:
    return {
        "shift_types": [{"code": "M"}, {"code": "A"}, {"code": "N"}],
        "policy_parameters": {
            "no_consecutive_nights": True,
            "min_rest_hours_between_shifts": 11,
            "weights": {
                "preferred_shift_satisfaction": 1.0,
                "preferred_dayoff_satisfaction": 1.2,
                "shortfall_penalty": 1000.0
            },
        },
    }


def _synth_coverage(start_date: datetime, days: int, m: int, a: int, n: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(days):
        d = (start_date + timedelta(days=i)).date().isoformat()
        out += [
            {"date": d, "shift": "M", "req_total": int(m)},
            {"date": d, "shift": "A", "req_total": int(a)},
            {"date": d, "shift": "N", "req_total": int(n)},
        ]
    return out


# ----------------------------
# Builders
# ----------------------------

def build_from_webhook(
    webhook_url: str,
    out_path: Path,
    coverage_days: int = 14,
    m: int = 4, a: int = 4, n: int = 2,
    reference: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    base = fetch_webhook_json(webhook_url)
    cfg = {"nurses": base.get("nurses", [])}
    ensure_min_nurses(cfg, TARGET_MIN_NURSES)
    ref = reference or _default_reference()

    today = datetime.now().date()
    cfg.update({
        "shift_types": ref["shift_types"],
        "coverage_requirements": _synth_coverage(datetime.combine(today, datetime.min.time()), coverage_days, m, a, n),
        "date_horizon": {"start": today.isoformat(), "end": (today + timedelta(days=coverage_days-1)).isoformat()},
        "policy_parameters": ref["policy_parameters"],
    })

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[✅] Manager output saved -> {out_path}")
    return cfg


def build_from_sqlite(
    db_path: Path,
    out_path: Path,
    coverage_days: int = 14,
    m: int = 4, a: int = 4, n: int = 2,
    reference: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    base = fetch_sqlite_json(db_path)
    cfg = {"nurses": base.get("nurses", [])}
    ensure_min_nurses(cfg, TARGET_MIN_NURSES)
    ref = reference or _default_reference()

    today = datetime.now().date()
    cfg.update({
        "shift_types": ref["shift_types"],
        "coverage_requirements": _synth_coverage(datetime.combine(today, datetime.min.time()), coverage_days, m, a, n),
        "date_horizon": {"start": today.isoformat(), "end": (today + timedelta(days=coverage_days-1)).isoformat()},
        "policy_parameters": ref["policy_parameters"],
    })

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[✅] Manager output (sqlite) saved -> {out_path}")
    return cfg


if __name__ == "__main__":
    src = os.getenv("MANAGER_SOURCE", "webhook").lower()
    out = Path(os.getenv("MANAGER_OUTPUT", f"manager_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"))
    if src == "sqlite":
        build_from_sqlite(Path(os.getenv("MANAGER_DB", "nurse_prefs.db")), out)
    else:
        build_from_webhook(os.getenv("MANAGER_WEBHOOK", "http://localhost:8080/export_all"), out)