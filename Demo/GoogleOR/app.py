# app.py  (Pydantic v2 compatible)
from typing import Dict, List, Optional, Any
from fastapi import FastAPI
from pydantic import BaseModel, Field, model_validator
from ortools.sat.python import cp_model
from datetime import datetime

# SCHEMA DEFINITIONS
class Weights(BaseModel):
    understaff_penalty: int = Field(50, description="Penalty per missing nurse on a shift")
    overtime_penalty: int = Field(10, description="Penalty per extra shift above max for a nurse")
    preference_penalty_multiplier: int = Field(1, description="Multiplier for preference penalties")


class SolveRequest(BaseModel):
    nurses: List[str]
    days: List[str]
    shifts: List[str]
    demand: Dict[str, Dict[str, int]]

    min_total_shifts_per_nurse: Optional[Dict[str, int]] = None
    max_total_shifts_per_nurse: Optional[Dict[str, int]] = None
    max_shifts_per_nurse: Optional[Dict[str, int]] = None

    availability: Optional[Dict[str, Dict[str, Dict[str, int]]]] = None
    preferences: Optional[Dict[str, Dict[str, Dict[str, int]]]] = None
    nurse_skills: Optional[Dict[str, List[str]]] = None
    required_skills: Optional[Dict[str, Dict[str, Dict[str, int]]]] = None
    week_index_by_day: Optional[Dict[str, int]] = None
    weights: Optional[Weights] = None

    # NEW: Pydantic v2 validator
    @model_validator(mode="after")
    def check_demand(self): 
        for d in self.days:
            if d not in self.demand:
                raise ValueError(f"Demand missing for day '{d}'.")
            for s in self.shifts:
                if s not in self.demand[d]:
                    raise ValueError(f"Demand missing for day '{d}', shift '{s}'.")
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


class SolveResponse(BaseModel):
    status: str
    objective_value: Optional[int] = None
    assignments: List[Assignment] = []
    understaffed: List[UnderstaffItem] = []
    nurse_stats: List[NurseStats] = []
    details: Optional[Dict[str, Any]] = None



#  FASTAPI APP
app = FastAPI(
    title="Nurse Scheduling API (Pydantic v2 compatible)",
    description="Schedules nurses with coverage, skill rules, night limits, and rest constraints.",
    version="1.3.0"
)



#  HELPER FUNCTIONS
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
    """Return mapping day->week_index."""
    if explicit_map:
        return dict(explicit_map)
    if all(is_iso_date(d) for d in days):
        iso_weeks = [datetime.fromisoformat(d).isocalendar()[1] for d in days]
        uniq_sorted = {w: i for i, w in enumerate(dict.fromkeys(iso_weeks))}
        return {d: uniq_sorted[datetime.fromisoformat(d).isocalendar()[1]] for d in days}
    return {d: i // 7 for i, d in enumerate(days)}


def shift_eq(a: str, b: str) -> bool:
    return a.strip().lower() == b.strip().lower()


#  CORE SOLVER ENDPOINT
@app.post("/solve", response_model=SolveResponse)
def solve(req: SolveRequest) -> SolveResponse:
    nurses = req.nurses
    days = req.days
    shifts = req.shifts
    demand = req.demand
    availability = req.availability or {}
    preferences = req.preferences or {}
    nurse_skills = req.nurse_skills or {}
    required_skills = req.required_skills or {}
    weights = req.weights or Weights()

    default_upper = len(days)
    per_nurse_min = {n: int((req.min_total_shifts_per_nurse or {}).get(n, 0)) for n in nurses}
    per_nurse_max = {
        n: int((req.max_total_shifts_per_nurse or {}).get(n,
               (req.max_shifts_per_nurse or {}).get(n, default_upper)))
        for n in nurses
    }

    week_idx = get_week_index_map(days, req.week_index_by_day)
    model = cp_model.CpModel()

    x = {(n, d, s): model.NewBoolVar(f"x_{n}_{d}_{s}") for n in nurses for d in days for s in shifts}
    under = {(d, s): model.NewIntVar(0, len(nurses), f"under_{d}_{s}") for d in days for s in shifts}
    over = {n: model.NewIntVar(0, len(days) * len(shifts), f"over_{n}") for n in nurses}

    # 1) coverage
    for d in days:
        for s in shifts:
            model.Add(sum(x[(n, d, s)] for n in nurses) + under[(d, s)] == demand[d][s])

    # 2) ≤1 shift/day per nurse
    for n in nurses:
        for d in days:
            model.Add(sum(x[(n, d, s)] for s in shifts) <= 1)

    # 3) availability
    for n in nurses:
        for d in days:
            for s in shifts:
                if not is_available(availability, n, d, s):
                    model.Add(x[(n, d, s)] == 0)

    # 4) monthly min/max + overtime
    for n in nurses:
        total = sum(x[(n, d, s)] for d in days for s in shifts)
        model.Add(total - over[n] <= per_nurse_max[n])
        model.Add(total >= per_nurse_min[n])

    # 5) no Night→Morning next day
    if any(shift_eq(s, "night") for s in shifts) and any(shift_eq(s, "morning") for s in shifts):
        night_name = next(s for s in shifts if shift_eq(s, "night"))
        morning_name = next(s for s in shifts if shift_eq(s, "morning"))
        for n in nurses:
            for i in range(len(days) - 1):
                model.Add(x[(n, days[i], night_name)] + x[(n, days[i + 1], morning_name)] <= 1)

    # 6) ≤2 Nights per week
    if any(shift_eq(s, "night") for s in shifts):
        night_name = next(s for s in shifts if shift_eq(s, "night"))
        weeks = {}
        for d in days:
            weeks.setdefault(week_idx[d], []).append(d)
        for n in nurses:
            for w, dlist in weeks.items():
                model.Add(sum(x[(n, d, night_name)] for d in dlist) <= 2)

    # 7) ≥2 days off per week
    weeks = {}
    for d in days:
        weeks.setdefault(week_idx[d], []).append(d)
    for n in nurses:
        for w, dlist in weeks.items():
            cap = max(0, len(dlist) - 2)
            model.Add(sum(sum(x[(n, d, s)] for s in shifts) for d in dlist) <= cap)

    # 8) skill requirements
    for d in days:
        for s in shifts:
            for skill, need in (required_skills.get(d, {}).get(s, {}) or {}).items():
                if need <= 0:
                    continue
                eligible = [n for n in nurses if skill in (nurse_skills.get(n, []) or [])]
                model.Add(sum(x[(n, d, s)] for n in eligible) >= need)

    # 9) objective
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
    solver.parameters.max_time_in_seconds = 15.0
    solver.parameters.num_search_workers = 8
    result = solver.Solve(model)

    if result in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        assignments, understaffed, stats = [], [], []
        for d in days:
            for s in shifts:
                for n in nurses:
                    if solver.Value(x[(n, d, s)]) == 1:
                        assignments.append(Assignment(day=d, shift=s, nurse=n))
        for d in days:
            for s in shifts:
                miss = solver.Value(under[(d, s)])
                if miss:
                    understaffed.append(UnderstaffItem(day=d, shift=s, missing=int(miss)))

        night_label = next((s for s in shifts if shift_eq(s, "night")), None)
        for n in nurses:
            total = sum(solver.Value(x[(n, d, s)]) for d in days for s in shifts)
            nights = sum(solver.Value(x[(n, d, night_label)]) for d in days) if night_label else 0
            stats.append(NurseStats(
                nurse=n, assigned_shifts=int(total),
                overtime=int(solver.Value(over[n])), nights=int(nights)
            ))

        return SolveResponse(
            status="OPTIMAL" if result == cp_model.OPTIMAL else "FEASIBLE",
            objective_value=int(solver.ObjectiveValue()),
            assignments=assignments,
            understaffed=understaffed,
            nurse_stats=stats,
            details={
                "best_bound": solver.BestObjectiveBound(),
                "wall_time_sec": solver.WallTime(),
                "conflicts": solver.NumConflicts(),
                "branches": solver.NumBranches(),
            },
        )

    return SolveResponse(status="INFEASIBLE", details={"message": "No valid schedule found."})
