# chatbot/normalizer.py

from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

WEEKDAY_ABBR = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def dayoff_rank_to_penalty(rank: Any) -> int:
    """
    Map day-off rank (1,2,3) to a soft preference penalty for the solver.
    Higher rank = stronger 'please don't schedule me' signal.
    """
    try:
        r = int(rank)
    except Exception:
        r = 2
    return {1: 10, 2: 25, 3: 40}.get(r, 25)


def level_to_skills(level: Any) -> List[str]:
    """
    Map nurse level to skills list.
    For now: Level >= 2 => Senior.
    """
    try:
        lvl = int(level)
    except Exception:
        lvl = 1
    if lvl >= 2:
        return ["Senior"]
    return []


def build_horizon_days(start_date: Optional[str] = None, horizon_days: int = 7) -> List[str]:
    """
    Build a list of ISO date strings for the scheduling horizon.
    """
    if start_date:
        base = datetime.fromisoformat(start_date).date()
    else:
        base = datetime.now().date()
    return [(base + timedelta(days=i)).isoformat() for i in range(horizon_days)]


def build_week_index_map(days: List[str]) -> Dict[str, int]:
    """
    Same logic as solver.get_week_index_map when days are ISO dates:
    group by ISO week number into 0..k buckets.
    """
    iso_weeks = [datetime.fromisoformat(d).isocalendar()[1] for d in days]
    uniq_sorted = {w: i for i, w in enumerate(dict.fromkeys(iso_weeks))}
    return {
        d: uniq_sorted[datetime.fromisoformat(d).isocalendar()[1]]
        for d in days
    }


def build_solver_payload_from_db_rows(
    nurses_rows: List[Tuple[Any, Any, Any, Any, Any]],
    prefs_rows: List[Tuple[Any, Any, Any]],
    start_date: Optional[str] = None,
    horizon_days: int = 7,
    morning_demand: int = 4,
    evening_demand: int = 3,
    night_demand: int = 2,
) -> Dict[str, Any]:
    """
    Convert raw DB rows (nurses + preferences) into a JSON payload
    that matches the FastAPI /solve (SolveRequest) schema.

    nurses_rows: (id, name, level, employment_type, unit)
    prefs_rows:  (nurse_id, preference_type, data_json_str)
    """
    days = build_horizon_days(start_date, horizon_days)
    shifts = ["Morning", "Evening", "Night"]

    # 1) Map DB ids -> nurse codes
    nurse_codes: Dict[int, str] = {}
    nurse_meta: Dict[str, Dict[str, Any]] = {}
    for row in nurses_rows:
        nid, name, level, employment_type, unit = row
        code = f"N{nid:03}"
        nurse_codes[nid] = code
        nurse_meta[code] = {
            "name": name or f"Nurse {nid}",
            "level": level or 1,
            "employment_type": employment_type or "full_time",
            "unit": unit or "ER",
        }

    nurses_list = sorted(nurse_meta.keys())

    # 2) Build preferences: nurse -> day -> shift -> penalty
    preferences: Dict[str, Dict[str, Dict[str, int]]] = {}
    import json

    for nurse_id, pref_type, data in prefs_rows:
        code = nurse_codes.get(nurse_id)
        if not code:
            continue
        try:
            parsed = json.loads(data)
        except Exception:
            continue

        if pref_type == "preferred_days_off":
            # Example: {"date": "YYYY-MM-DD", "rank": 1..3}
            date_str = parsed.get("date")
            rank = parsed.get("rank", 2)
            if not date_str or date_str not in days:
                continue
            penalty = dayoff_rank_to_penalty(rank)
            for shift_name in shifts:
                preferences.setdefault(code, {}).setdefault(date_str, {})[shift_name] = penalty

        # NOTE: preferred_shifts (likes) are currently ignored for the solver.
        # You can extend this later to treat "non-liked" combos as soft dislikes.

    # 3) Min/max shifts per nurse based on employment type
    min_total: Dict[str, int] = {}
    max_total: Dict[str, int] = {}

    for code, meta in nurse_meta.items():
        emp = (meta["employment_type"] or "").strip().lower()
        if emp in ("part_time", "part-time", "pt"):
            min_total[code] = max(2, horizon_days // 4)
            max_total[code] = max(4, horizon_days // 2)
        elif emp in ("contract", "temp"):
            min_total[code] = max(1, horizon_days // 5)
            max_total[code] = max(3, horizon_days // 2)
        else:  # full_time / default
            min_total[code] = max(4, horizon_days // 2)
            max_total[code] = horizon_days  # max one per day anyway

    # 4) Nurse skills
    nurse_skills: Dict[str, List[str]] = {}
    for code, meta in nurse_meta.items():
        nurse_skills[code] = level_to_skills(meta["level"])

    # 5) Required skills: each Night needs 1 Senior (demo assumption)
    required_skills: Dict[str, Dict[str, Dict[str, int]]] = {}
    for d in days:
        required_skills.setdefault(d, {})["Night"] = {"Senior": 1}

    # 6) Demand per day/shift (constant for now)
    demand: Dict[str, Dict[str, int]] = {}
    for d in days:
        demand[d] = {
            "Morning": morning_demand,
            "Evening": evening_demand,
            "Night": night_demand,
        }

    # 7) Week index map
    week_index_by_day = build_week_index_map(days)

    # 8) Final payload, shaped exactly like SolveRequest expects
    payload: Dict[str, Any] = {
        "nurses": nurses_list,
        "days": days,
        "shifts": shifts,
        "demand": demand,
        "min_total_shifts_per_nurse": min_total,
        "max_total_shifts_per_nurse": max_total,
        "availability": None,
        "preferences": preferences,
        "nurse_skills": nurse_skills,
        "required_skills": required_skills,
        "week_index_by_day": week_index_by_day,
        "weights": {
            "understaff_penalty": 50,
            "overtime_penalty": 10,
            "preference_penalty_multiplier": 1,
            "weekly_night_over_penalty": 80,
            "weekly_overwork_penalty": 60,
            "workload_balance_weight": 0,
            "postfill_same_day_penalty": 12,
            "postfill_weekly_night_over_penalty": 5,
        },
        "time_limit_sec": 15.0,
        "relaxed_time_limit_sec": 10.0,
        "num_search_workers": 8,
        "random_seed": 42,
        "enable_cp_sat_log": False,
    }

    return payload
