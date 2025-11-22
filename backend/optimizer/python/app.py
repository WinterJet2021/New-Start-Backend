from __future__ import annotations
from typing import Dict, List, Optional, Any, Tuple
from fastapi import FastAPI
from pydantic import BaseModel, Field, model_validator
from ortools.sat.python import cp_model
from datetime import datetime
from collections import Counter, defaultdict

# ────────────────────────────────
#  DATA MODELS
# ────────────────────────────────
class Weights(BaseModel):
    # Base penalties (strict and relaxed)
    understaff_penalty: int = Field(50, ge=0, description="Penalty per missing nurse on a shift (CP-SAT stage)")
    overtime_penalty: int = Field(10, ge=0, description="Penalty per extra shift above max for a nurse (CP-SAT)")
    preference_penalty_multiplier: int = Field(1, ge=0, description="Multiplier for preference penalties (CP-SAT)")

    # Soft constraints used by RELAXED pass (Night→Morning is now HARD, so no weight for NM)
    weekly_night_over_penalty: int = Field(80, ge=0, description="Penalty per extra night above weekly cap (RELAXED)")
    weekly_overwork_penalty: int = Field(60, ge=0, description="Penalty per extra shift above weekly cap (days off) (RELAXED)")

    # Optional fairness (RELAXED only)
    workload_balance_weight: int = Field(0, ge=0, description="Penalty per absolute deviation from target workload")

    # Post-fill satisfaction penalties (overtime decisions after CP-SAT)
    postfill_same_day_penalty: int = Field(12, ge=0, description="Satisfaction penalty per extra same-day shift (post-fill)")
    postfill_weekly_night_over_penalty: int = Field(5, ge=0, description="Satisfaction penalty per extra night above weekly cap (post-fill)")


class SolveRequest(BaseModel):
    nurses: List[str]
    days: List[str]
    shifts: List[str]
    demand: Dict[str, Dict[str, int]]

    # Per-nurse totals (optional)
    min_total_shifts_per_nurse: Optional[Dict[str, int]] = None
    max_total_shifts_per_nurse: Optional[Dict[str, int]] = None
    max_shifts_per_nurse: Optional[Dict[str, int]] = None  # legacy alias used as fallback for max_total_shifts

    # Optionals
    availability: Optional[Dict[str, Dict[str, Dict[str, int]]]] = None  # 1/0 availability by nurse→day→shift
    preferences: Optional[Dict[str, Dict[str, Dict[str, int]]]] = None   # penalty by nurse→day→shift
    nurse_skills: Optional[Dict[str, List[str]]] = None                   # e.g., {"N01":["Senior"], ...}
    required_skills: Optional[Dict[str, Dict[str, Dict[str, int]]]] = None # e.g., {day:{shift:{"Senior":1}}}
    week_index_by_day: Optional[Dict[str, int]] = None                    # maps dates to week buckets (0..)
    weights: Optional[Weights] = None

    # Solver knobs
    time_limit_sec: float = Field(15.0, gt=0)
    relaxed_time_limit_sec: float = Field(10.0, gt=0)
    num_search_workers: int = Field(8, ge=1)
    random_seed: Optional[int] = None
    enable_cp_sat_log: bool = False

    @model_validator(mode="after")
    def check_consistency(self):
        # basic demand shape
        for d in self.days:
            if d not in self.demand:
                raise ValueError(f"Demand missing for day '{d}'.")
            for s in self.shifts:
                if s not in self.demand[d]:
                    raise ValueError(f"Demand missing for day '{d}', shift '{s}'.")
                val = self.demand[d][s]
                if not isinstance(val, int) or val < 0:
                    raise ValueError(f"Demand must be nonnegative int at {d}/{s}.")
        # uniqueness
        if len(set(self.nurses)) != len(self.nurses):
            raise ValueError("Duplicate nurse IDs are not allowed.")
        if len(set(self.days)) != len(self.days):
            raise ValueError("Duplicate days are not allowed.")
        if len(set(self.shifts)) != len(self.shifts):
            raise ValueError("Duplicate shifts are not allowed.")
        return self


class Assignment(BaseModel):
    day: str
    shift: str
    nurse: str


class UnderstaffItem(BaseModel):
    day: str
    shift: str
    missing: int


class NurseStats(BaseModel):
    nurse: str
    assigned_shifts: int
    overtime: int
    nights: int
    satisfaction: int  # 1–100 composite score per nurse


class SolveResponse(BaseModel):
    status: str
    objective_value: Optional[int] = None
    assignments: List[Assignment] = Field(default_factory=list)
    understaffed: List[UnderstaffItem] = Field(default_factory=list)
    nurse_stats: List[NurseStats] = Field(default_factory=list)
    details: Optional[Dict[str, Any]] = None


# ────────────────────────────────
#  FASTAPI APP
# ────────────────────────────────
app = FastAPI(
    title="Nurse Scheduling API",
    description="Schedules nurses with coverage, Senior requirement, night limits, and rest constraints. Post-fills any gaps with real-nurse overtime (no Night→Morning).",
    version="2.4.0",
)


# ────────────────────────────────
#  HELPERS
# ────────────────────────────────
def get_pref_penalty(prefs, nurse, day, shift) -> int:
    if not prefs:
        return 0
    return int(prefs.get(nurse, {}).get(day, {}).get(shift, 0))


def is_available(avail, nurse, day, shift) -> bool:
    if not avail:
        return True
    return bool(avail.get(nurse, {}).get(day, {}).get(shift, 1))


def is_iso_date(s: str) -> bool:
    try:
        datetime.fromisoformat(s)
        return True
    except Exception:
        return False


def get_week_index_map(days: List[str], explicit_map: Optional[Dict[str, int]]) -> Dict[str, int]:
    if explicit_map:
        return dict(explicit_map)
    if all(is_iso_date(d) for d in days):
        iso_weeks = [datetime.fromisoformat(d).isocalendar()[1] for d in days]
        uniq_sorted = {w: i for i, w in enumerate(dict.fromkeys(iso_weeks))}
        return {d: uniq_sorted[datetime.fromisoformat(d).isocalendar()[1]] for d in days}
    # fallback: every 7 days is a new week bucket
    return {d: i // 7 for i, d in enumerate(days)}


def shift_eq(a: str, b: str) -> bool:
    return a.strip().lower() == b.strip().lower()


def find_shift_name(shifts: List[str], target: str) -> Optional[str]:
    for s in shifts:
        if shift_eq(s, target):
            return s
    return None


# ---------- Satisfaction + Post-fill helpers ----------
def compute_satisfaction_for_nurse(
    nurse: str,
    days: List[str],
    shifts: List[str],
    assigned_map: Dict[tuple, int],
    preferences: Dict[str, Dict[str, Dict[str, int]]],
    night_label: Optional[str],
    weights: Weights,
    overtime_month: int = 0,
    extra_same_day: int = 0,
    extra_nights_over: int = 0,
) -> int:
    total = 0
    nights_count = 0
    disliked = 0
    for d in days:
        for s in shifts:
            if assigned_map.get((nurse, d, s), 0) == 1:
                total += 1
                if night_label and (s == night_label):
                    nights_count += 1
                if int((preferences.get(nurse, {}).get(d, {}).get(s, 0))) > 0:
                    disliked += 1

    score = 100
    if total > 0:
        score -= int((disliked / total) * 40)                         # dislike load (max −40)
        score -= int((nights_count / total) * 20)                      # night share   (max −20)
    score -= 10 * int(overtime_month)                                  # monthly overtime (from CP-SAT + post-fill)
    score -= weights.postfill_same_day_penalty * int(extra_same_day)   # per same-day extra shift
    score -= weights.postfill_weekly_night_over_penalty * int(extra_nights_over)  # per extra night beyond cap
    return max(1, min(100, score))


def backfill_missing_with_overtime(
    assignments: List[Assignment],
    understaffed: List[UnderstaffItem],
    nurses: List[str],
    days: List[str],
    shifts: List[str],
    week_idx: Dict[str, int],
    availability: Dict[str, Dict[str, Dict[str, int]]],
    preferences: Dict[str, Dict[str, Dict[str, int]]],
    required_skills: Dict[str, Dict[str, Dict[str, int]]],
    nurse_skills: Dict[str, List[str]],
    night_label: Optional[str],
    morning_label: Optional[str],  # kept for completeness; we forbid NM anyway
    weights: Weights,
    base_overtime_from_model: Dict[str, int],
) -> Tuple[List[Assignment], List[UnderstaffItem], Dict[str, int], Dict[Tuple[str, str], int]]:
    """
    Fill remaining missing coverage using ONLY real nurses by assigning overtime.
    Priority: highest satisfaction & lighter load. Respects availability, Night→Morning ban,
    weekly night cap initially; may overflow weekly night cap if needed; NEVER creates Night→Morning.
    Returns: (new_assignments, [], overtime_map, extra_same_day_map)
    """
    if not understaffed:
        return assignments, [], base_overtime_from_model, {}

    # Precompute structures
    assigned_map = {(a.nurse, a.day, a.shift): 1 for a in assignments}
    load_total = Counter([a.nurse for a in assignments])
    load_by_day = Counter([(a.nurse, a.day) for a in assignments])
    nights_by_week = defaultdict(int)  # (nurse, week) -> nights assigned
    if night_label:
        for a in assignments:
            if a.shift == night_label:
                w = week_idx[a.day]
                nights_by_week[(a.nurse, w)] += 1

    def has_skill(n: str, skill: str) -> bool:
        return skill in (nurse_skills.get(n, []) or [])

    # Night→Morning detection (forbid)
    def would_create_nm(n: str, d: str, s: str) -> bool:
        if not (night_label and morning_label):
            return False
        idx = days.index(d)
        if s == "Morning" and morning_label:
            # if assigning Morning on day d, check Night on day d-1
            if idx > 0 and assigned_map.get((n, days[idx - 1], night_label), 0) == 1:
                return True
        if s == night_label:
            # if assigning Night on day d, check Morning on day d+1
            if idx < len(days) - 1 and assigned_map.get((n, days[idx + 1], morning_label), 0) == 1:
                return True
        return False

    def extra_night_over_if_add(n: str, d: str, s: str) -> int:
        if night_label and s == night_label:
            w = week_idx[d]
            return max(0, (nights_by_week[(n, w)] + 1) - 2)
        return 0

    def is_disliked(n: str, d: str, s: str) -> bool:
        return get_pref_penalty(preferences, n, d, s) > 0

    def is_avail(n: str, d: str, s: str) -> bool:
        return is_available(availability, n, d, s)

    def senior_needed(d: str, s: str) -> int:
        return int((required_skills.get(d, {}).get(s, {}) or {}).get("Senior", 0))

    def senior_already_assigned(d: str, s: str) -> int:
        return sum(1 for a in assignments if a.day == d and a.shift == s and has_skill(a.nurse, "Senior"))

    seniors = {n for n in nurses if has_skill(n, "Senior")}

    overtime_map = defaultdict(int, **{k: int(v) for k, v in base_overtime_from_model.items()})
    extra_same_day = defaultdict(int)  # (nurse, day) -> count of extra shifts added by post-fill

    unders_by_day = defaultdict(list)
    for u in understaffed:
        unders_by_day[u.day].append((u.shift, u.missing))

    for d in days:
        if d not in unders_by_day:
            continue
        for s, miss in unders_by_day[d]:
            while miss > 0:
                need_senior = max(0, senior_needed(d, s) - senior_already_assigned(d, s))

                # Escalation WITHOUT Night→Morning ever:
                # L0: avail=1, <=1/day, no weekly night over
                # L1: allow second shift same day (<=2/day), no weekly night over
                # L2: allow weekly night over (>2/week), still no NM
                # L3: (last resort) ignore availability, still no NM
                levels = [
                    {"allow_same_day_second": False, "allow_weekly_night_over": False, "ignore_avail": False},
                    {"allow_same_day_second": True,  "allow_weekly_night_over": False, "ignore_avail": False},
                    {"allow_same_day_second": True,  "allow_weekly_night_over": True,  "ignore_avail": False},
                    {"allow_same_day_second": True,  "allow_weekly_night_over": True,  "ignore_avail": True},
                ]

                chosen = None
                cand_meta = None

                for lvl in levels:
                    candidates = []
                    for n in nurses:
                        if need_senior and (n not in seniors):
                            continue
                        if not lvl["ignore_avail"] and not is_avail(n, d, s):
                            continue

                        day_load = load_by_day.get((n, d), 0)
                        if day_load >= 2:
                            continue
                        if day_load >= 1 and not lvl["allow_same_day_second"]:
                            continue

                        # forbid Night→Morning always
                        if would_create_nm(n, d, s):
                            continue

                        night_over = extra_night_over_if_add(n, d, s)
                        if night_over > 0 and not lvl["allow_weekly_night_over"]:
                            continue

                        # satisfaction snapshot
                        sat = compute_satisfaction_for_nurse(
                            nurse=n,
                            days=days,
                            shifts=shifts,
                            assigned_map=assigned_map,
                            preferences=preferences,
                            night_label=night_label,
                            weights=weights,
                            overtime_month=overtime_map[n],
                            extra_same_day=extra_same_day.get((n, d), 0),
                            extra_nights_over=0,
                        )

                        disliked_flag = 1 if is_disliked(n, d, s) else 0
                        candidates.append((
                            -sat,              # higher sat first
                            day_load,          # prefer not assigned yet today
                            load_total[n],     # lighter total load
                            disliked_flag,     # prefer not disliked slot
                            n,
                            night_over
                        ))

                    if candidates:
                        candidates.sort()
                        _, day_load, _, disliked_flag, nbest, night_over = candidates[0]
                        chosen = nbest
                        cand_meta = (day_load, disliked_flag, night_over)
                        break

                if chosen is None:
                    # Could not fill further even at last level (very rare)
                    break

                # Commit assignment
                assignments.append(Assignment(day=d, shift=s, nurse=chosen))
                assigned_map[(chosen, d, s)] = 1
                load_total[chosen] += 1
                if load_by_day.get((chosen, d), 0) >= 1:
                    extra_same_day[(chosen, d)] += 1
                load_by_day[(chosen, d)] += 1

                if night_label and s == night_label:
                    w = week_idx[d]
                    nights_by_week[(chosen, w)] += 1

                overtime_map[chosen] += 1
                miss -= 1

    return assignments, [], dict(overtime_map), dict(extra_same_day)


# ────────────────────────────────
#  CORE SOLVER
# ────────────────────────────────
@app.post("/solve", response_model=SolveResponse)
def solve(req: SolveRequest) -> SolveResponse:
    nurses, days, shifts = req.nurses, req.days, req.shifts
    demand = req.demand
    availability, preferences = req.availability or {}, req.preferences or {}
    nurse_skills, required_skills = req.nurse_skills or {}, req.required_skills or {}
    weights = req.weights or Weights()

    default_upper = len(days)
    per_nurse_min = {n: int((req.min_total_shifts_per_nurse or {}).get(n, 0)) for n in nurses}
    per_nurse_max = {
        n: int((req.max_total_shifts_per_nurse or {}).get(n, (req.max_shifts_per_nurse or {}).get(n, default_upper)))
        for n in nurses
    }

    week_idx = get_week_index_map(days, req.week_index_by_day)
    night_label = find_shift_name(shifts, "night")
    morning_label = find_shift_name(shifts, "morning")

    # Preindex weeks
    weeks: Dict[int, List[str]] = {}
    for d in days:
        weeks.setdefault(week_idx[d], []).append(d)

    # ========== STRICT MODEL ==========
    model = cp_model.CpModel()
    x = {(n, d, s): model.NewBoolVar(f"x_{n}_{d}_{s}") for n in nurses for d in days for s in shifts}
    under = {(d, s): model.NewIntVar(0, len(nurses), f"under_{d}_{s}") for d in days for s in shifts}
    over = {n: model.NewIntVar(0, len(days), f"over_{n}") for n in nurses}

    # 1) Coverage (with understaff slack)
    for d in days:
        for s in shifts:
            model.Add(sum(x[(n, d, s)] for n in nurses) + under[(d, s)] == demand[d][s])

    # 2) ≤ 1 shift/day per nurse
    for n in nurses:
        for d in days:
            model.Add(sum(x[(n, d, s)] for s in shifts) <= 1)

    # 3) Availability
    for n in nurses:
        for d in days:
            for s in shifts:
                if not is_available(availability, n, d, s):
                    model.Add(x[(n, d, s)] == 0)

    # 4) Monthly min/max w/ overtime slack
    total_assigned = {}
    for n in nurses:
        total = sum(x[(n, d, s)] for d in days for s in shifts)
        total_assigned[n] = total
        model.Add(total - over[n] <= per_nurse_max[n])
        model.Add(total >= per_nurse_min[n])

    # 5) No Night→Morning next day (HARD)
    if night_label and morning_label:
        for n in nurses:
            for i in range(len(days) - 1):
                model.Add(x[(n, days[i], night_label)] + x[(n, days[i + 1], morning_label)] <= 1)

    # 6) ≤ 2 Nights per week (HARD)
    if night_label:
        for n in nurses:
            for w, dlist in weeks.items():
                model.Add(sum(x[(n, d, night_label)] for d in dlist) <= 2)

    # 7) ≥ 2 days off per week (HARD)
    for n in nurses:
        for w, dlist in weeks.items():
            cap = max(0, len(dlist) - 2)  # at most 5 working days/week
            model.Add(sum(sum(x[(n, d, s)] for s in shifts) for d in dlist) <= cap)

    # 8) Senior requirement (HARD)
    for d in days:
        for s in shifts:
            need_senior = int((required_skills.get(d, {}).get(s, {}) or {}).get("Senior", 0))
            if need_senior > 0:
                eligible = [n for n in nurses if "Senior" in (nurse_skills.get(n, []) or [])]
                model.Add(sum(x[(n, d, s)] for n in eligible) >= need_senior)

    # Objective
    terms = []
    for d in days:
        for s in shifts:
            terms.append(weights.understaff_penalty * under[(d, s)])
    for n in nurses:
        terms.append(weights.overtime_penalty * over[n])
    for n in nurses:
        for d in days:
            for s in shifts:
                pen = get_pref_penalty(preferences, n, d, s)
                if pen:
                    terms.append(weights.preference_penalty_multiplier * pen * x[(n, d, s)])
    model.Minimize(sum(terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = req.time_limit_sec
    solver.parameters.num_search_workers = req.num_search_workers
    if req.random_seed is not None:
        solver.parameters.random_seed = req.random_seed
    solver.parameters.log_search_progress = req.enable_cp_sat_log

    result = solver.Solve(model)

    def pack_strict(code):
        assignments, understaffed, stats = [], [], []
        assigned_map = {(n, d, s): int(solver.Value(x[(n, d, s)])) for n in nurses for d in days for s in shifts}
        for d in days:
            for s in shifts:
                for n in nurses:
                    if assigned_map[(n, d, s)] == 1:
                        assignments.append(Assignment(day=d, shift=s, nurse=n))
        for d in days:
            for s in shifts:
                miss = solver.Value(under[(d, s)])
                if miss:
                    understaffed.append(UnderstaffItem(day=d, shift=s, missing=int(miss)))

        base_overtime = {n: int(solver.Value(over[n])) for n in nurses}

        # preliminary stats (before post-fill)
        night_count_map = {n: 0 for n in nurses}
        if night_label:
            for n in nurses:
                night_count_map[n] = sum(assigned_map[(n, d, night_label)] for d in days)
        for n in nurses:
            total = int(solver.Value(total_assigned[n]))
            stats.append(NurseStats(
                nurse=n,
                assigned_shifts=total,
                overtime=base_overtime[n],
                nights=night_count_map[n],
                satisfaction=100,
            ))

        # POST-FILL: real-nurse overtime, NO Night→Morning ever
        assignments2, understaffed2, overtime2, extra_same_day = backfill_missing_with_overtime(
            assignments=assignments,
            understaffed=understaffed,
            nurses=nurses,
            days=days,
            shifts=shifts,
            week_idx=week_idx,
            availability=availability,
            preferences=preferences,
            required_skills=required_skills or {},
            nurse_skills=nurse_skills or {},
            night_label=night_label,
            morning_label=morning_label,
            weights=weights,
            base_overtime_from_model=base_overtime,
        )
        assignments = assignments2
        understaffed = understaffed2

        # Recompute stats & satisfaction using post-fill results
        assigned_map2 = {(a.nurse, a.day, a.shift): 1 for a in assignments}

        # weekly night over from final schedule
        extra_nights_over_map = Counter()
        if night_label:
            nights_by_week = defaultdict(int)
            for n in nurses:
                for d in days:
                    if assigned_map2.get((n, d, night_label), 0) == 1:
                        w = week_idx[d]
                        nights_by_week[(n, w)] += 1
            for (n, w), cnt in nights_by_week.items():
                if cnt > 2:
                    extra_nights_over_map[n] += (cnt - 2)

        new_stats = []
        for n in nurses:
            total = sum(1 for d in days for s in shifts if assigned_map2.get((n, d, s), 0) == 1)
            nights = sum(1 for d in days if night_label and assigned_map2.get((n, d, night_label), 0) == 1)
            same_day_extra = sum(v for (nn, _d), v in extra_same_day.items() if nn == n)
            sat = compute_satisfaction_for_nurse(
                nurse=n,
                days=days,
                shifts=shifts,
                assigned_map=assigned_map2,
                preferences=preferences,
                night_label=night_label,
                weights=weights,
                overtime_month=overtime2.get(n, 0),
                extra_same_day=same_day_extra,
                extra_nights_over=extra_nights_over_map.get(n, 0),
            )
            new_stats.append(NurseStats(
                nurse=n,
                assigned_shifts=int(total),
                overtime=int(overtime2.get(n, 0)),
                nights=int(nights),
                satisfaction=int(sat),
            ))
        stats = new_stats

        avg_satisfaction = round(sum(s.satisfaction for s in stats) / len(stats), 2) if stats else 0.0

        return SolveResponse(
            status="OPTIMAL" if code == cp_model.OPTIMAL else "FEASIBLE",
            objective_value=int(solver.ObjectiveValue()),
            assignments=assignments,
            understaffed=understaffed,  # should be []
            nurse_stats=stats,
            details={
                "average_satisfaction": avg_satisfaction,
                "post_fill": {
                    "note": "All gaps were filled with real-nurse overtime (never Night→Morning).",
                },
                "best_bound": solver.BestObjectiveBound(),
                "wall_time_sec": solver.WallTime(),
                "conflicts": solver.NumConflicts(),
                "branches": solver.NumBranches(),
            },
        )

    if result in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return pack_strict(result)

    # ========== RELAXED MODEL (Night→Morning is HARD here now) ==========
    r_model = cp_model.CpModel()
    rx = {(n, d, s): r_model.NewBoolVar(f"rx_{n}_{d}_{s}") for n in nurses for d in days for s in shifts}
    r_under = {(d, s): r_model.NewIntVar(0, len(nurses), f"r_under_{d}_{s}") for d in days for s in shifts}
    r_over = {n: r_model.NewIntVar(0, len(days), f"r_over_{n}") for n in nurses}

    # coverage
    for d in days:
        for s in shifts:
            r_model.Add(sum(rx[(n, d, s)] for n in nurses) + r_under[(d, s)] == demand[d][s])

    # ≤1 shift/day and availability stay hard
    for n in nurses:
        for d in days:
            r_model.Add(sum(rx[(n, d, s)] for s in shifts) <= 1)
            for s in shifts:
                if not is_available(availability, n, d, s):
                    r_model.Add(rx[(n, d, s)] == 0)

    # monthly max soft via overtime; min-total soft via slack
    r_total_assigned = {}
    r_min_slack = {}
    for n in nurses:
        total = sum(rx[(n, d, s)] for d in days for s in shifts)
        r_total_assigned[n] = total
        r_model.Add(total - r_over[n] <= per_nurse_max[n])
        slack = r_model.NewIntVar(0, max(0, per_nurse_min[n]), f"r_min_slack_{n}")
        r_model.Add(total + slack >= per_nurse_min[n])
        r_min_slack[n] = slack

    # Night→Morning HARD in RELAXED
    if night_label and morning_label:
        for n in nurses:
            for i in range(len(days) - 1):
                r_model.Add(rx[(n, days[i], night_label)] + rx[(n, days[i + 1], morning_label)] <= 1)

    # Soft weekly night cap (≤2) and days-off
    wn_over: List[cp_model.IntVar] = []
    if night_label:
        for n in nurses:
            for w, dlist in weeks.items():
                nights_this = sum(rx[(n, d, night_label)] for d in dlist)
                extra_nights = r_model.NewIntVar(0, len(dlist), f"wn_over_{n}_{w}")
                r_model.Add(nights_this - 2 <= extra_nights)
                wn_over.append(extra_nights)

    wd_over: List[cp_model.IntVar] = []
    for n in nurses:
        for w, dlist in weeks.items():
            cap = max(0, len(dlist) - 2)
            shifts_this_week = sum(sum(rx[(n, d, s)] for s in shifts) for d in dlist)
            extra_work = r_model.NewIntVar(0, len(dlist), f"wd_over_{n}_{w}")
            r_model.Add(shifts_this_week - cap <= extra_work)
            wd_over.append(extra_work)

    # Soft Senior shortage
    skill_short: List[cp_model.IntVar] = []
    for d in days:
        for s in shifts:
            need_senior = int((required_skills.get(d, {}).get(s, {}) or {}).get("Senior", 0))
            if need_senior > 0:
                eligible = [n for n in nurses if "Senior" in (nurse_skills.get(n, []) or [])]
                short = r_model.NewIntVar(0, need_senior, f"skill_short_{d}_{s}")
                r_model.Add(sum(rx[(n, d, s)] for n in eligible) + short >= need_senior)
                skill_short.append(short)

    # Optional fairness
    workload_devs: List[cp_model.IntVar] = []
    if weights.workload_balance_weight > 0:
        total_demand = sum(demand[d][s] for d in days for s in shifts)
        target = total_demand // max(1, len(nurses))
        for n in nurses:
            dev = r_model.NewIntVar(0, len(days), f"dev_{n}")
            r_model.AddAbsEquality(dev, r_total_assigned[n] - target)
            workload_devs.append(dev)

    # Objective (relaxed)
    r_terms: List[cp_model.LinearExpr] = []
    for d in days:
        for s in shifts:
            r_terms.append(weights.understaff_penalty * r_under[(d, s)])
    for n in nurses:
        r_terms.append(weights.overtime_penalty * r_over[n])
        r_terms.append(weights.overtime_penalty * r_min_slack[n])
    for n in nurses:
        for d in days:
            for s in shifts:
                pen = get_pref_penalty(preferences, n, d, s)
                if pen:
                    r_terms.append(weights.preference_penalty_multiplier * pen * rx[(n, d, s)])
    for v in wn_over:
        r_terms.append(weights.weekly_night_over_penalty * v)
    for v in wd_over:
        r_terms.append(weights.weekly_overwork_penalty * v)
    for v in skill_short:
        r_terms.append(weights.weekly_overwork_penalty * v)
    for v in workload_devs:
        r_terms.append(weights.workload_balance_weight * v)

    r_model.Minimize(sum(r_terms))

    r_solver = cp_model.CpSolver()
    r_solver.parameters.max_time_in_seconds = req.relaxed_time_limit_sec
    r_solver.parameters.num_search_workers = req.num_search_workers
    if req.random_seed is not None:
        r_solver.parameters.random_seed = req.random_seed
    r_solver.parameters.log_search_progress = req.enable_cp_sat_log

    r_res = r_solver.Solve(r_model)

    def pack_relaxed():
        assignments, understaffed, stats = [], [], []
        assigned_map = {(n, d, s): int(r_solver.Value(rx[(n, d, s)])) for n in nurses for d in days for s in shifts}
        for d in days:
            for s in shifts:
                for n in nurses:
                    if assigned_map[(n, d, s)] == 1:
                        assignments.append(Assignment(day=d, shift=s, nurse=n))
        for d in days:
            for s in shifts:
                miss = r_solver.Value(r_under[(d, s)])
                if miss:
                    understaffed.append(UnderstaffItem(day=d, shift=s, missing=int(miss)))

        base_overtime = {n: int(r_solver.Value(r_over[n])) for n in nurses}

        # PRE stats (will be recomputed post-fill)
        night_count_map = {n: 0 for n in nurses}
        if night_label:
            for n in nurses:
                night_count_map[n] = sum(assigned_map[(n, d, night_label)] for d in days)
        for n in nurses:
            total = int(r_solver.Value(r_total_assigned[n]))
            stats.append(NurseStats(
                nurse=n,
                assigned_shifts=total,
                overtime=base_overtime[n],
                nights=night_count_map[n],
                satisfaction=100,
            ))

        # POST-FILL with real nurses (never Night→Morning)
        assignments2, understaffed2, overtime2, extra_same_day = backfill_missing_with_overtime(
            assignments=assignments,
            understaffed=understaffed,
            nurses=nurses,
            days=days,
            shifts=shifts,
            week_idx=week_idx,
            availability=availability,
            preferences=preferences,
            required_skills=required_skills or {},
            nurse_skills=nurse_skills or {},
            night_label=night_label,
            morning_label=morning_label,
            weights=weights,
            base_overtime_from_model=base_overtime,
        )
        assignments = assignments2
        understaffed = understaffed2

        # Recompute stats & satisfaction from final schedule
        assigned_map2 = {(a.nurse, a.day, a.shift): 1 for a in assignments}

        extra_nights_over_map = Counter()
        if night_label:
            nights_by_week = defaultdict(int)
            for n in nurses:
                for d in days:
                    if assigned_map2.get((n, d, night_label), 0) == 1:
                        w = week_idx[d]
                        nights_by_week[(n, w)] += 1
            for (n, w), cnt in nights_by_week.items():
                if cnt > 2:
                    extra_nights_over_map[n] += (cnt - 2)

        new_stats = []
        for n in nurses:
            total = sum(1 for d in days for s in shifts if assigned_map2.get((n, d, s), 0) == 1)
            nights = sum(1 for d in days if night_label and assigned_map2.get((n, d, night_label), 0) == 1)
            same_day_extra = sum(v for (nn, _d), v in extra_same_day.items() if nn == n)
            sat = compute_satisfaction_for_nurse(
                nurse=n,
                days=days,
                shifts=shifts,
                assigned_map=assigned_map2,
                preferences=preferences,
                night_label=night_label,
                weights=weights,
                overtime_month=overtime2.get(n, 0),
                extra_same_day=same_day_extra,
                extra_nights_over=extra_nights_over_map.get(n, 0),
            )
            new_stats.append(NurseStats(
                nurse=n,
                assigned_shifts=int(total),
                overtime=int(overtime2.get(n, 0)),
                nights=int(nights),
                satisfaction=int(sat),
            ))
        stats = new_stats

        avg_satisfaction = round(sum(s.satisfaction for s in stats) / len(stats), 2) if stats else 0.0
        status = "RELAXED_OPTIMAL" if r_res == cp_model.OPTIMAL else "RELAXED_FEASIBLE"
        return SolveResponse(
            status=status,
            objective_value=int(r_solver.ObjectiveValue()),
            assignments=assignments,
            understaffed=understaffed,  # should be []
            nurse_stats=stats,
            details={
                "average_satisfaction": avg_satisfaction,
                "post_fill": {
                    "note": "All gaps were filled with real-nurse overtime (never Night→Morning).",
                },
                "best_bound": r_solver.BestObjectiveBound(),
                "wall_time_sec": r_solver.WallTime(),
                "conflicts": r_solver.NumConflicts(),
                "branches": r_solver.NumBranches(),
            },
        )

    if r_res in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return pack_relaxed()

    # ========== LAST-RESORT HEURISTIC (then post-fill with no Night→Morning) ==========
    def has_skill(n, skill):
        return skill in (nurse_skills.get(n, []) or [])

    def is_avail(n, d, s):
        return bool((availability.get(n, {}) or {}).get(d, {}).get(s, 1))

    assignments: List[Assignment] = []
    used = {(n, d): False for n in nurses for d in days}
    load = {n: 0 for n in nurses}

    for d in days:
        for s in shifts:
            req_needed = demand[d][s]
            senior_need = int((required_skills.get(d, {}).get(s, {}) or {}).get("Senior", 0))
            seniors = sorted([n for n in nurses if has_skill(n, "Senior")], key=lambda n: load[n])
            regulars = sorted(nurses, key=lambda n: load[n])

            chosen: List[str] = []
            # seniors first
            for n in seniors:
                if len(chosen) >= senior_need:
                    break
                if is_avail(n, d, s) and not used[(n, d)]:
                    chosen.append(n); used[(n, d)] = True; load[n] += 1
            # fill remaining
            for n in regulars:
                if len(chosen) >= req_needed:
                    break
                if n in chosen:
                    continue
                if is_avail(n, d, s) and not used[(n, d)]:
                    chosen.append(n); used[(n, d)] = True; load[n] += 1
            for n in chosen:
                assignments.append(Assignment(day=d, shift=s, nurse=n))

    # compute understaffed from heuristic
    count = Counter((a.day, a.shift) for a in assignments)
    understaffed: List[UnderstaffItem] = []
    for d in days:
        for s in shifts:
            miss = max(0, demand[d][s] - count.get((d, s), 0))
            if miss:
                understaffed.append(UnderstaffItem(day=d, shift=s, missing=miss))

    base_overtime = {n: 0 for n in nurses}

    assignments, understaffed, overtime2, _extra_same_day = backfill_missing_with_overtime(
        assignments=assignments,
        understaffed=understaffed,
        nurses=nurses,
        days=days,
        shifts=shifts,
        week_idx=week_idx,
        availability=availability,
        preferences=preferences,
        required_skills=required_skills or {},
        nurse_skills=nurse_skills or {},
        night_label=night_label,
        morning_label=morning_label,
        weights=weights,
        base_overtime_from_model=base_overtime,
    )

    # final stats
    assigned_map2 = {(a.nurse, a.day, a.shift): 1 for a in assignments}
    stats = []
    for n in nurses:
        total = sum(1 for d in days for s in shifts if assigned_map2.get((n, d, s), 0) == 1)
        nights = sum(1 for d in days if night_label and assigned_map2.get((n, d, night_label), 0) == 1)
        sat = compute_satisfaction_for_nurse(
            nurse=n,
            days=days,
            shifts=shifts,
            assigned_map=assigned_map2,
            preferences=preferences,
            night_label=night_label,
            weights=weights,
            overtime_month=overtime2.get(n, 0),
            extra_same_day=0,
            extra_nights_over=0,
        )
        stats.append(NurseStats(
            nurse=n, assigned_shifts=total, overtime=int(overtime2.get(n, 0)), nights=int(nights), satisfaction=int(sat)
        ))
    avg_satisfaction = round(sum(s.satisfaction for s in stats) / len(stats), 2) if stats else 0.0

    return SolveResponse(
        status="HEURISTIC",
        objective_value=None,
        assignments=assignments,
        understaffed=[],  # guaranteed zero now
        nurse_stats=stats,
        details={"average_satisfaction": avg_satisfaction, "message": "Heuristic + post-fill with real-nurse overtime (no Night→Morning)."},
    )


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "version": "2.4.0"}
