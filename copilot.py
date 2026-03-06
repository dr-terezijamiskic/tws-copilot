#!/usr/bin/env python3
"""
TWS Copilot - Reports + Alerts, NO TRADING

Two independent modes:
1) REPORT MODE (one-shot, exits): Daily history report with FIFO realized PnL
2) MONITOR MODE (loop, alerts): Live TP/SL alerts on positions

Read-only. Never places orders.
"""

import argparse
import csv
import hashlib
import os
import sqlite3
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# =============================================================================
# SAFETY GUARDRAIL - No trading code allowed
# =============================================================================

_FORBIDDEN_PATTERNS = [
    "ib.place" + "Order",  # split to avoid self-match
    "ib.cancel" + "Order",
    "Limit" + "Order(",
    "Market" + "Order(",
]

def _safety_check():
    """Verify this file contains no order-placing code."""
    with open(__file__, "r") as f:
        source = f.read()
    for pattern in _FORBIDDEN_PATTERNS:
        if pattern in source:
            raise RuntimeError(f"SAFETY VIOLATION: order code found. This tool is READ-ONLY.")

_safety_check()

# Optional IB imports (only needed in live mode) - NO ORDER CLASSES
try:
    from ib_insync import IB, Contract
except ImportError:
    IB = None
    Contract = None


# =============================================================================
# UTILITIES
# =============================================================================

def ts_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ts_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")


def today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def fmt(x, nd=2) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return str(x)


def is_rth_open() -> bool:
    """Check if regular trading hours are open (09:30-16:00 ET)."""
    now = datetime.now(ET).time()
    return (now.hour > 9 or (now.hour == 9 and now.minute >= 30)) and (now.hour < 16)


def to_et(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ET)


def classify_session(dt_et: datetime) -> str:
    t = dt_et.time()
    if (t.hour > 9 or (t.hour == 9 and t.minute >= 30)) and (t.hour < 16):
        return "RTH"
    if 16 <= t.hour < 20:
        return "AH"
    return "OTHER"


def format_contract(row: dict) -> str:
    """Format contract info for display."""
    sym = row.get("symbol") or "UNK"
    expiry = row.get("expiry") or ""
    strike = row.get("strike")
    right = row.get("right") or ""

    if expiry and strike:
        return f"{sym} {expiry} {strike}{right}"
    return sym


# =============================================================================
# NOTIFICATIONS (Mac)
# =============================================================================

def mac_notify(title: str, message: str) -> None:
    """
    Reliable notification system:
    - Always logs to ACTION_INBOX.md
    - Always prints to terminal
    - Tries macOS notification (best effort)
    - Plays a sound
    """
    # 1) Append to inbox file
    try:
        with open("ACTION_INBOX.md", "a") as f:
            f.write(f"- [{ts_utc()}] **{title}** — {message}\n")
    except Exception:
        pass

    # 2) Print to terminal
    print(f"[ACTION] {title}: {message}", flush=True)

    # 3) Try macOS notification
    safe_title = title.replace('"', "'")
    safe_msg = message.replace('"', "'")
    os.system(f'''osascript -e 'display notification "{safe_msg}" with title "{safe_title}"' ''')

    # 4) Play sound
    os.system("afplay /System/Library/Sounds/Ping.aiff >/dev/null 2>&1 &")


# =============================================================================
# DATABASE (SQLite)
# =============================================================================

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exec_id TEXT NOT NULL UNIQUE,
    ts_utc TEXT NOT NULL,
    ts_et TEXT NOT NULL,
    session_tag TEXT NOT NULL,
    account TEXT,
    con_id INTEGER,
    symbol TEXT,
    sec_type TEXT,
    expiry TEXT,
    strike REAL,
    right TEXT,
    multiplier REAL,
    side TEXT,
    qty REAL,
    price REAL,
    commission REAL,
    realized_csv REAL,
    basis_csv REAL,
    proceeds_csv REAL,
    code TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    event_day TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    threshold REAL NOT NULL,
    pnl_pct REAL NOT NULL,
    avg_cost REAL NOT NULL,
    mark REAL NOT NULL,
    UNIQUE(symbol, event_type, event_day)
);

CREATE TABLE IF NOT EXISTS swing_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    pnl_pct REAL NOT NULL,
    avg_cost REAL NOT NULL,
    mark REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS swing_state (
    symbol TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_cost REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS swing_marks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_et TEXT NOT NULL,
    session_tag TEXT NOT NULL,
    symbol TEXT NOT NULL,
    mark REAL NOT NULL,
    avg_cost REAL NOT NULL,
    qty REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS swing_crossings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    cross_type TEXT NOT NULL,
    first_ts_et TEXT NOT NULL,
    pnl_pct_at_cross REAL NOT NULL,
    avg_cost REAL NOT NULL,
    mark_at_cross REAL NOT NULL,
    UNIQUE(date, symbol, cross_type)
);

CREATE INDEX IF NOT EXISTS idx_swing_marks_ts ON swing_marks(ts_et);
CREATE INDEX IF NOT EXISTS idx_swing_marks_symbol ON swing_marks(symbol);

CREATE INDEX IF NOT EXISTS idx_executions_ts_et ON executions(ts_et);
CREATE INDEX IF NOT EXISTS idx_executions_con_id ON executions(con_id);
"""


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA)
    # Migrate: add nullable CSV columns if missing
    existing = {row[1] for row in conn.execute("PRAGMA table_info(executions)").fetchall()}
    for col in ("realized_csv", "basis_csv", "proceeds_csv"):
        if col not in existing:
            conn.execute(f"ALTER TABLE executions ADD COLUMN {col} REAL")
    if "code" not in existing:
        conn.execute("ALTER TABLE executions ADD COLUMN code TEXT")
    conn.commit()
    return conn


def db_upsert_executions(conn: sqlite3.Connection, rows: List[dict]) -> int:
    """Insert executions, ignoring duplicates. Returns count of new inserts."""
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            cur.execute(
                """
                INSERT INTO executions (
                    exec_id, ts_utc, ts_et, session_tag, account, con_id, symbol, sec_type,
                    expiry, strike, right, multiplier, side, qty, price, commission,
                    realized_csv, basis_csv, proceeds_csv, code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["exec_id"], r["ts_utc"], r["ts_et"], r["session_tag"], r.get("account"),
                    r.get("con_id"), r.get("symbol"), r.get("sec_type"),
                    r.get("expiry"), r.get("strike"), r.get("right"),
                    r.get("multiplier"), r.get("side"), r.get("qty"),
                    r.get("price"), r.get("commission"),
                    r.get("realized_csv"), r.get("basis_csv"), r.get("proceeds_csv"),
                    r.get("code"),
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return inserted


def db_get_executions_on_date(conn: sqlite3.Connection, date_str: str, sec_type: str = "OPT") -> List[dict]:
    """Get all executions on a specific date (ET)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM executions
        WHERE ts_et LIKE ? AND sec_type = ?
        ORDER BY ts_et
        """,
        (f"{date_str}%", sec_type)
    )
    return [dict(row) for row in cur.fetchall()]


def db_get_executions_up_to_date(conn: sqlite3.Connection, date_str: str, sec_type: str = "OPT") -> List[dict]:
    """Get all executions up to end of date (ET) for FIFO calculation."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM executions
        WHERE ts_et <= ? AND sec_type = ?
        ORDER BY ts_et
        """,
        (f"{date_str} 23:59:59", sec_type)
    )
    return [dict(row) for row in cur.fetchall()]


def db_try_insert_event(conn: sqlite3.Connection, symbol: str, event_type: str,
                        threshold: float, pnl_pct: float, avg_cost: float, mark: float) -> bool:
    """Insert event if not already triggered today. Returns True if inserted."""
    cur = conn.cursor()
    event_day = today_et()
    try:
        cur.execute(
            """
            INSERT INTO events (ts, event_day, symbol, event_type, threshold, pnl_pct, avg_cost, mark)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_utc(), event_day, symbol, event_type, threshold, pnl_pct, avg_cost, mark)
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


# =============================================================================
# IB CLIENT (Live Data)
# =============================================================================

def ib_connect(host: str, port: int, client_id: int) -> "IB":
    """Connect to TWS/IB Gateway (read-only)."""
    if IB is None:
        raise RuntimeError("ib_insync not available. Install: pip install ib_insync")

    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id, readonly=True)
    except TypeError:
        ib.connect(host, port, clientId=client_id)
    return ib


def _extract_commission(cr) -> Optional[float]:
    """Extract commission from a commissionReport, returning None if unavailable."""
    if cr is None:
        return None
    try:
        val = float(cr.commission)
        # IB returns 1e10 as sentinel for "not yet available"
        if val >= 1e9:
            return None
        return val
    except Exception:
        return None


def pull_executions_live(host: str, port: int, client_id: int, scope: str,
                         exec_filter: Any = None) -> List[dict]:
    """Pull executions from TWS and normalize them.

    Args:
        exec_filter: optional ib_insync ExecutionFilter for date-range pulls.
    """
    ib = ib_connect(host, port, client_id)
    try:
        if exec_filter is not None:
            fills = ib.reqExecutions(exec_filter)
        else:
            fills = ib.reqExecutions()

        # Wait for commission reports to arrive (they can lag)
        ib.sleep(1.5)

        # Re-read fills to pick up late commission reports
        fills = ib.fills()

        missing_comm = 0
        out = []
        for f in fills:
            c = f.contract
            e = f.execution
            cr = getattr(f, "commissionReport", None)

            sec_type = getattr(c, "secType", None) or ""
            if scope == "options" and sec_type != "OPT":
                continue
            if scope == "stocks" and sec_type != "STK":
                continue
            if scope == "both" and sec_type not in ("OPT", "STK"):
                continue

            dt_et = to_et(e.time)
            session_tag = classify_session(dt_et)

            mult = getattr(c, "multiplier", None)
            try:
                mult_f = float(mult) if mult else (100.0 if sec_type == "OPT" else 1.0)
            except Exception:
                mult_f = 100.0 if sec_type == "OPT" else 1.0

            commission = _extract_commission(cr)
            if commission is None:
                missing_comm += 1

            out.append({
                "exec_id": e.execId,
                "ts_utc": e.time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "ts_et": dt_et.strftime("%Y-%m-%d %H:%M:%S"),
                "session_tag": session_tag,
                "account": getattr(e, "acctNumber", None),
                "con_id": getattr(c, "conId", None),
                "symbol": getattr(c, "symbol", None),
                "sec_type": sec_type,
                "expiry": getattr(c, "lastTradeDateOrContractMonth", None),
                "strike": float(getattr(c, "strike", None)) if getattr(c, "strike", None) else None,
                "right": getattr(c, "right", None),
                "multiplier": mult_f,
                "side": e.side,
                "qty": float(e.shares),
                "price": float(e.price),
                "commission": commission,
            })

        if missing_comm > 0:
            print(f"  WARNING: commission missing for {missing_comm} execution(s)", flush=True)

        return out
    finally:
        ib.disconnect()


def get_account_id(host: str, port: int, client_id: int) -> str:
    """Get the account ID from TWS."""
    ib = ib_connect(host, port, client_id)
    try:
        ib.sleep(0.3)
        accounts = ib.managedAccounts()
        return accounts[0] if accounts else "UNKNOWN"
    finally:
        ib.disconnect()


def pull_positions_live(host: str, port: int, client_id: int, scope: str) -> List[dict]:
    """Pull portfolio positions from TWS."""
    ib = ib_connect(host, port, client_id)
    try:
        ib.sleep(0.5)
        out = []
        for item in ib.portfolio():
            c = item.contract
            sec_type = getattr(c, "secType", None) or ""

            if scope == "options" and sec_type != "OPT":
                continue
            if scope == "stocks" and sec_type != "STK":
                continue

            qty = float(item.position or 0.0)
            if qty == 0:
                continue

            avg_cost = float(item.averageCost or 0.0)
            mark = float(item.marketPrice or 0.0)

            out.append({
                "con_id": getattr(c, "conId", None),
                "symbol": getattr(c, "symbol", None),
                "sec_type": sec_type,
                "expiry": getattr(c, "lastTradeDateOrContractMonth", None),
                "strike": getattr(c, "strike", None),
                "right": getattr(c, "right", None),
                "qty": qty,
                "avg_cost": avg_cost,
                "mark": mark,
            })
        return out
    finally:
        ib.disconnect()


# =============================================================================
# FIFO ENGINE
# =============================================================================

@dataclass
class IntradayResult:
    """Result of intraday-only FIFO (same-day buys matched with same-day sells)."""
    realized_gross: float
    commissions: float
    realized_net: float
    by_symbol: Dict[str, float]
    reduced_prior_flags: List[str]  # sells that exceeded today's buys


@dataclass
class FifoResult:
    """Result of FIFO PnL calculation across history for a report date."""
    realized_gross: float
    commissions_on_date: float
    realized_net: float
    by_symbol: Dict[str, float]
    sells_count: int
    buys_count: int
    missing_history_flags: List[str]
    missing_on_date_flags: List[str]
    unmatched_sells_on_date: int = 0


@dataclass
class PriorDayClose:
    """A single prior-day position closed today."""
    con_id: int
    symbol: str
    expiry: str
    strike: float
    right: str
    qty_sold: float
    fifo_pnl: float


@dataclass
class ExpirationInfo:
    """An option that expired on the report date with remaining long position."""
    con_id: int
    symbol: str
    expiry: str
    strike: float
    right: str
    qty_expired: float
    cost_basis: float  # total cost of the expired lots (loss)


@dataclass
class WeeklyDayRow:
    """One day's FIFO results within a weekly report."""
    date: str
    gross: float
    commissions: float
    net: float
    execs_count: int
    sells_count: int
    matched_sells: int
    missing_count: int
    missing_flags: List[str]


def compute_intraday_realized(execs_today: List[dict]) -> IntradayResult:
    """
    Compute intraday realized PnL: only matches today's buys with today's sells.

    This is the RELIABLE metric - doesn't depend on historical data.
    Sells that exceed today's buys are flagged as "reduced prior" (not counted in intraday realized).
    """
    # Group by con_id
    by_conid = defaultdict(list)
    for r in execs_today:
        cid = r.get("con_id") or 0
        if cid:
            by_conid[cid].append(r)

    realized_gross = 0.0
    by_symbol = defaultdict(float)
    reduced_prior_flags = []
    commissions = 0.0

    for cid, rows in by_conid.items():
        rows = sorted(rows, key=lambda x: x["ts_et"])
        sym = rows[-1].get("symbol") or "UNK"
        mult = float(rows[-1].get("multiplier") or 100.0)

        # Track commissions
        for r in rows:
            if r.get("commission") is not None:
                commissions += float(r["commission"])

        # FIFO lots from today's buys only
        lots = deque()  # [qty_remaining, price]

        for r in rows:
            qty = float(r["qty"])
            price = float(r["price"])

            if r["side"] == "BOT":
                lots.append([qty, price])
            else:  # SLD
                remaining_sell = qty
                realized_this_sell = 0.0

                while remaining_sell > 0 and lots:
                    lot_qty, lot_price = lots[0]
                    match_qty = min(remaining_sell, lot_qty)
                    pnl = (price - lot_price) * match_qty * mult
                    realized_this_sell += pnl

                    lots[0][0] -= match_qty
                    remaining_sell -= match_qty

                    if lots[0][0] <= 1e-9:
                        lots.popleft()

                # If remaining_sell > 0, this sell reduced prior-day holdings (not intraday)
                if remaining_sell > 1e-9:
                    reduced_prior_flags.append(f"{sym} (con_id={cid}): sold {remaining_sell:.0f} from prior days")

                realized_gross += realized_this_sell
                by_symbol[sym] += realized_this_sell

    return IntradayResult(
        realized_gross=realized_gross,
        commissions=commissions,
        realized_net=realized_gross - commissions,
        by_symbol=dict(by_symbol),
        reduced_prior_flags=reduced_prior_flags,
    )


def compute_fifo_realized_for_date(all_execs: List[dict], report_date: str) -> FifoResult:
    """
    Compute realized PnL for sells occurring on report_date using FIFO across all history.

    - Processes all executions up to end of report_date
    - Attributes realized PnL to the SELL date
    - Flags missing history if sell has no prior buy lots
    """
    # Sort by timestamp
    execs_sorted = sorted(all_execs, key=lambda r: r["ts_et"])

    # FIFO lots per con_id: deque of [qty_remaining, entry_price, multiplier, symbol, ts_et]
    lots = defaultdict(deque)

    # Results
    realized_gross = 0.0
    by_symbol = defaultdict(float)
    sells_count = 0
    buys_count = 0
    missing_history_flags = []
    missing_on_date_flags = []
    unmatched_sells_on_date = 0
    commissions_on_date = 0.0

    for r in execs_sorted:
        ts_et_str = r["ts_et"]
        is_on_report_date = ts_et_str.startswith(report_date)

        con_id = r.get("con_id") or 0
        sym = r.get("symbol") or "UNK"
        side = r["side"]
        qty = float(r["qty"])
        price = float(r["price"])
        mult = float(r.get("multiplier") or 100.0)

        # Track commissions on report date
        if is_on_report_date and r.get("commission") is not None:
            commissions_on_date += float(r["commission"])

        if side == "BOT":
            # Add to lots
            lots[con_id].append([qty, price, mult, sym, ts_et_str])
            if is_on_report_date:
                buys_count += 1
        else:  # SLD
            if is_on_report_date:
                sells_count += 1

            remaining_sell = qty
            realized_this_sell = 0.0

            while remaining_sell > 0 and lots[con_id]:
                lot = lots[con_id][0]
                lot_qty, lot_price, lot_mult, lot_sym, lot_ts = lot

                match_qty = min(remaining_sell, lot_qty)
                pnl = (price - lot_price) * match_qty * lot_mult
                realized_this_sell += pnl

                lot[0] -= match_qty
                remaining_sell -= match_qty

                if lot[0] <= 1e-9:
                    lots[con_id].popleft()

            # If we still have remaining_sell > 0, missing history
            if remaining_sell > 1e-9:
                flag = f"{sym} (con_id={con_id}): sold {qty}, missing {remaining_sell:.0f} lots in history"
                missing_history_flags.append(flag)
                if is_on_report_date:
                    missing_on_date_flags.append(flag)
                    unmatched_sells_on_date += 1

            # Only attribute realized PnL if this sell is on the report date
            if is_on_report_date:
                realized_gross += realized_this_sell
                by_symbol[sym] += realized_this_sell

    return FifoResult(
        realized_gross=realized_gross,
        commissions_on_date=commissions_on_date,
        realized_net=realized_gross - commissions_on_date,
        by_symbol=dict(by_symbol),
        sells_count=sells_count,
        buys_count=buys_count,
        missing_history_flags=missing_history_flags,
        missing_on_date_flags=missing_on_date_flags,
        unmatched_sells_on_date=unmatched_sells_on_date,
    )


def compute_opened_today_cost(execs_today: List[dict]) -> List[dict]:
    """
    Compute contracts opened today & still open (net qty > 0).
    Returns list of {symbol, expiry, strike, right, qty, cost_today}.
    Cost = qty * avg_price * multiplier (pure accounting, no marks).
    """
    by_conid = defaultdict(list)
    for r in execs_today:
        cid = r.get("con_id") or 0
        if cid:
            by_conid[cid].append(r)

    result = []
    for cid, rows in by_conid.items():
        rows = sorted(rows, key=lambda x: x["ts_et"])
        lots = deque()  # [qty, price]

        for r in rows:
            qty = float(r["qty"])
            price = float(r["price"])
            if r["side"] == "BOT":
                lots.append([qty, price])
            else:
                remaining = qty
                while remaining > 0 and lots:
                    if lots[0][0] <= remaining:
                        remaining -= lots[0][0]
                        lots.popleft()
                    else:
                        lots[0][0] -= remaining
                        remaining = 0

        total_qty = sum(q for q, p in lots)
        if total_qty > 0:
            # Cost = qty * price * multiplier for remaining lots
            last_row = rows[-1]
            mult = float(last_row.get("multiplier") or 100.0)
            total_cost = sum(q * p * mult for q, p in lots)

            result.append({
                "con_id": cid,
                "symbol": last_row.get("symbol"),
                "expiry": last_row.get("expiry"),
                "strike": last_row.get("strike"),
                "right": last_row.get("right"),
                "qty": total_qty,
                "cost_today": total_cost,
            })

    return sorted(result, key=lambda x: -x["cost_today"])


def compute_prior_day_closes(all_execs: List[dict], report_date: str) -> List[PriorDayClose]:
    """
    Identify sells on report_date that closed prior-day positions.

    Logic:
    - For each SLD on report_date, check if there were enough BOTs on report_date before it
    - Any portion of the sell that exceeds same-day buys is a prior-day close
    - Use FIFO across history to compute realized P&L for those closes

    Returns list of PriorDayClose objects grouped by con_id.
    """
    execs_sorted = sorted(all_execs, key=lambda r: r["ts_et"])

    # FIFO lots per con_id: deque of [qty_remaining, entry_price, multiplier, ts_et]
    lots = defaultdict(deque)

    # Track same-day buys per con_id (for determining what's prior-day)
    same_day_buy_qty = defaultdict(float)

    # Results: con_id -> {info, sells_closing_prior}
    prior_day_by_conid = defaultdict(lambda: {"qty_sold": 0.0, "fifo_pnl": 0.0, "info": None})

    for r in execs_sorted:
        ts_et_str = r["ts_et"]
        is_on_report_date = ts_et_str.startswith(report_date)

        con_id = r.get("con_id") or 0
        sym = r.get("symbol") or "UNK"
        side = r["side"]
        qty = float(r["qty"])
        price = float(r["price"])
        mult = float(r.get("multiplier") or 100.0)

        if side == "BOT":
            # Add to lots
            lots[con_id].append([qty, price, mult, ts_et_str])
            # Track same-day buys
            if is_on_report_date:
                same_day_buy_qty[con_id] += qty
        else:  # SLD
            # Process FIFO regardless of date
            remaining_sell = qty
            realized_this_sell = 0.0
            qty_from_prior_days = 0.0

            while remaining_sell > 0 and lots[con_id]:
                lot = lots[con_id][0]
                lot_qty, lot_price, lot_mult, lot_ts = lot

                match_qty = min(remaining_sell, lot_qty)
                pnl = (price - lot_price) * match_qty * lot_mult

                # Check if lot was from prior day
                lot_is_prior_day = not lot_ts.startswith(report_date)
                if is_on_report_date and lot_is_prior_day:
                    qty_from_prior_days += match_qty
                    realized_this_sell += pnl

                lot[0] -= match_qty
                remaining_sell -= match_qty

                if lot[0] <= 1e-9:
                    lots[con_id].popleft()

            # If sell is on report date and closed prior-day lots
            if is_on_report_date and qty_from_prior_days > 1e-9:
                prior_day_by_conid[con_id]["qty_sold"] += qty_from_prior_days
                prior_day_by_conid[con_id]["fifo_pnl"] += realized_this_sell
                if prior_day_by_conid[con_id]["info"] is None:
                    prior_day_by_conid[con_id]["info"] = {
                        "symbol": sym,
                        "expiry": r.get("expiry"),
                        "strike": r.get("strike"),
                        "right": r.get("right"),
                    }

    # Convert to list of PriorDayClose
    result = []
    for con_id, data in prior_day_by_conid.items():
        if data["qty_sold"] > 0:
            info = data["info"]
            result.append(PriorDayClose(
                con_id=con_id,
                symbol=info["symbol"],
                expiry=info.get("expiry") or "",
                strike=info.get("strike") or 0.0,
                right=info.get("right") or "",
                qty_sold=data["qty_sold"],
                fifo_pnl=data["fifo_pnl"],
            ))

    return sorted(result, key=lambda x: -x.fifo_pnl)


def compute_expiration_synthetics(all_execs: List[dict], report_date: str) -> tuple:
    """
    Find long option positions expiring on report_date with remaining lots.

    Runs FIFO across all history to find remaining lots, then for each con_id
    where expiry == report_date and remaining qty > 0, generates a synthetic
    SLD execution at price $0.00 (expired worthless).

    Returns:
        (synthetic_execs, expiration_infos)
        - synthetic_execs: list of dict (fake execution rows to feed into FIFO)
        - expiration_infos: list of ExpirationInfo for display
    """
    execs_sorted = sorted(all_execs, key=lambda r: r["ts_et"])

    # FIFO lots per con_id: deque of [qty_remaining, entry_price, multiplier, symbol, ts_et]
    lots = defaultdict(deque)
    # Track metadata per con_id for building synthetic rows
    meta = {}  # con_id -> {symbol, expiry, strike, right, multiplier, sec_type, account}

    for r in execs_sorted:
        con_id = r.get("con_id") or 0
        side = r["side"]
        qty = float(r["qty"])
        price = float(r["price"])
        mult = float(r.get("multiplier") or 100.0)

        # Store metadata
        if con_id not in meta:
            meta[con_id] = {
                "symbol": r.get("symbol"),
                "expiry": r.get("expiry"),
                "strike": r.get("strike"),
                "right": r.get("right"),
                "multiplier": mult,
                "sec_type": r.get("sec_type"),
                "account": r.get("account"),
            }

        if side == "BOT":
            lots[con_id].append([qty, price, mult, r.get("symbol") or "UNK", r["ts_et"]])
        else:  # SLD
            remaining_sell = qty
            while remaining_sell > 0 and lots[con_id]:
                lot = lots[con_id][0]
                match_qty = min(remaining_sell, lot[0])
                lot[0] -= match_qty
                remaining_sell -= match_qty
                if lot[0] <= 1e-9:
                    lots[con_id].popleft()

    # Convert report_date "2026-01-23" → expiry format "20260123"
    expiry_fmt = report_date.replace("-", "")

    synthetic_execs = []
    expiration_infos = []

    for con_id, lot_deque in lots.items():
        if not lot_deque:
            continue
        m = meta.get(con_id)
        if not m:
            continue
        # Only OPT contracts
        if m.get("sec_type") != "OPT":
            continue
        # Only contracts expiring on report_date
        if m.get("expiry") != expiry_fmt:
            continue

        total_remaining = sum(l[0] for l in lot_deque)
        if total_remaining <= 1e-9:
            continue

        # Cost basis = sum of (qty * price * multiplier) for remaining lots
        cost_basis = sum(l[0] * l[1] * l[2] for l in lot_deque)

        # Generate synthetic SLD at $0.00
        synthetic_execs.append({
            "exec_id": f"SYNTH_EXP_{con_id}_{report_date}",
            "ts_utc": f"{report_date} 21:00:00",  # ~16:00 ET
            "ts_et": f"{report_date} 16:00:00",
            "session_tag": "RTH",
            "account": m.get("account"),
            "con_id": con_id,
            "symbol": m["symbol"],
            "sec_type": "OPT",
            "expiry": m["expiry"],
            "strike": m["strike"],
            "right": m["right"],
            "multiplier": m["multiplier"],
            "side": "SLD",
            "qty": total_remaining,
            "price": 0.0,
            "commission": None,
            "synthetic": "EXPIRATION",
        })

        expiration_infos.append(ExpirationInfo(
            con_id=con_id,
            symbol=m["symbol"],
            expiry=m["expiry"],
            strike=m["strike"] or 0.0,
            right=m["right"] or "",
            qty_expired=total_remaining,
            cost_basis=cost_basis,
        ))

    expiration_infos.sort(key=lambda x: -x.cost_basis)
    return synthetic_execs, expiration_infos


# =============================================================================
# REPORT MODE
# =============================================================================

def _write_explain_md(f, explain_sells: List[dict], explain_symbol: str) -> None:
    """Write explain drill-down section to markdown file handle."""
    if not explain_sells:
        f.write(f"No sells found for {explain_symbol.upper()}.\n\n")
        return

    total_pnl = 0.0
    for s in explain_sells:
        ts_str = s["ts_et"].split(" ")[1] if " " in s["ts_et"] else s["ts_et"]
        synth_tag = " **[EXPIRED]**" if s.get("synthetic") else ""
        f.write(f"**SLD {s['date']} {ts_str}** | {s['contract']} | qty={s['qty']:.0f} | price=${s['price']:.2f}{synth_tag}\n\n")
        if s["matches"]:
            f.write("| Buy Date | Buy Time | Qty | Buy Price | PnL |\n")
            f.write("|----------|----------|----:|----------:|----:|\n")
            for m in s["matches"]:
                buy_ts = m["buy_ts"].split(" ")[1] if " " in m["buy_ts"] else m["buy_ts"]
                buy_date = m["buy_ts"][:10]
                sign = "+" if m["pnl"] >= 0 else ""
                f.write(f"| {buy_date} | {buy_ts} | {m['qty']:.0f} | ${m['buy_price']:.2f} | {sign}${m['pnl']:.2f} |\n")
            f.write("\n")
        if s["unmatched_qty"] > 0:
            f.write(f"MISSING: {s['unmatched_qty']:.0f} lots unmatched\n\n")
        sign = "+" if s["total_pnl"] >= 0 else ""
        f.write(f"Sell total: **{sign}${s['total_pnl']:.2f}**\n\n")
        total_pnl += s["total_pnl"]

    sign = "+" if total_pnl >= 0 else ""
    f.write(f"**{explain_symbol.upper()} total: {sign}${total_pnl:.2f}**\n\n")


def write_report_md(path: str, date_str: str, scope: str, session: str,
                    execs_today: List[dict], fifo: FifoResult,
                    opened_today: List[dict],
                    expirations: Optional[List[ExpirationInfo]] = None,
                    top_n: int = 5,
                    explain_sells: Optional[List[dict]] = None,
                    explain_symbol: Optional[str] = None) -> None:
    """Write markdown report file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    real_execs = [r for r in execs_today if not r.get("synthetic")]
    num_synth = len(execs_today) - len(real_execs)
    real_buys = fifo.buys_count
    real_sells = fifo.sells_count - num_synth
    comm_present = sum(1 for r in real_execs if r.get("commission") is not None)

    with open(path, "w") as f:
        f.write(f"# Daily Report — {date_str}\n\n")
        f.write(f"- Scope: **{scope}**\n")
        f.write(f"- Sessions: **{session}**\n")
        f.write(f"- Executions: **{len(real_execs)}** ({real_buys} buys, {real_sells} sells)\n")
        f.write(f"- Commission data: **{comm_present}/{len(real_execs)}**\n")
        f.write(f"- Generated: {ts_et()} ET\n\n")

        # ── Headline: Daily Realized P&L ──
        f.write("## Daily Realized P&L\n\n")
        sign_net = "+" if fifo.realized_net >= 0 else ""
        f.write(f"**Net: {sign_net}${fifo.realized_net:.2f}**\n\n")
        sign_gross = "+" if fifo.realized_gross >= 0 else ""
        f.write(f"| | |\n|---|---:|\n")
        f.write(f"| Gross | {sign_gross}${fifo.realized_gross:.2f} |\n")
        f.write(f"| Fees | ${fifo.commissions_on_date:.2f} |\n")
        f.write(f"| **Net** | **{sign_net}${fifo.realized_net:.2f}** |\n\n")

        # ── Top Winners / Losers ──
        if fifo.by_symbol:
            sorted_syms = sorted(fifo.by_symbol.items(), key=lambda x: x[1], reverse=True)
            winners = [(s, p) for s, p in sorted_syms if p > 0]
            losers = [(s, p) for s, p in sorted_syms if p < 0]
            flat = [(s, p) for s, p in sorted_syms if p == 0]

            if winners:
                f.write(f"### Top {min(len(winners), top_n)} Winners\n\n")
                for sym, pnl in winners[:top_n]:
                    f.write(f"- {sym}: +${pnl:.2f}\n")
                f.write("\n")

            if losers:
                f.write(f"### Top {min(len(losers), top_n)} Losers\n\n")
                for sym, pnl in losers[:top_n]:
                    f.write(f"- {sym}: ${pnl:.2f}\n")
                f.write("\n")

            if flat:
                f.write("### Flat\n\n")
                for sym, pnl in flat:
                    f.write(f"- {sym}: $0.00\n")
                f.write("\n")

        # ── Expired Worthless ──
        if expirations:
            total_loss = sum(e.cost_basis for e in expirations)
            f.write("## Expired Worthless\n\n")
            f.write(f"Total loss: **-${total_loss:.2f}**\n\n")
            f.write("| Contract | Qty | Cost Basis | Realized |\n")
            f.write("|----------|----:|-----------:|---------:|\n")
            for e in expirations:
                contract = f"{e.symbol} {e.expiry} {e.strike} {e.right}".strip()
                f.write(f"| {contract} | {int(e.qty_expired)} | ${e.cost_basis:.2f} | -${e.cost_basis:.2f} |\n")
            f.write("\n")

        # ── Opened Today & Still Open ──
        if opened_today:
            total_cost = sum(o["cost_today"] for o in opened_today)
            f.write("## Opened Today & Still Open\n\n")
            f.write(f"Total cost: **${total_cost:.2f}**\n\n")
            f.write("| Contract | Qty | Cost |\n")
            f.write("|----------|----:|-----:|\n")
            for o in opened_today:
                contract = f"{o['symbol']} {o.get('expiry', '')} {o.get('strike', '')} {o.get('right', '')}".strip()
                f.write(f"| {contract} | {int(o['qty'])} | ${o['cost_today']:.2f} |\n")
            f.write("\n")

        # ── Executions Timeline ──
        f.write("## Executions Timeline (ET)\n\n")
        f.write("| Time | Contract | Side | Qty | Price | Session | Comm |\n")
        f.write("|------|----------|------|----:|------:|---------|-----:|\n")

        for r in sorted(execs_today, key=lambda x: x["ts_et"]):
            time_str = r["ts_et"].split(" ")[1] if " " in r["ts_et"] else r["ts_et"]
            contract = format_contract(r)
            comm = fmt(r.get("commission"), 2) if r.get("commission") else "—"
            synthetic_tag = " *EXP*" if r.get("synthetic") else ""
            f.write(f"| {time_str} | {contract} | {r['side']} | {r['qty']:.0f} | {r['price']:.2f} | {r['session_tag']}{synthetic_tag} | {comm} |\n")
        f.write("\n")

        # ── Explain ──
        if explain_sells is not None and explain_symbol:
            f.write(f"## Explain: {explain_symbol.upper()}\n\n")
            _write_explain_md(f, explain_sells, explain_symbol)

        # ── Data Quality ──
        if fifo.missing_history_flags:
            f.write("## Data Quality\n\n")
            f.write(f"{len(fifo.missing_history_flags)} missing history flag(s) — P&L may be incomplete.\n\n")
            for flag in fifo.missing_history_flags:
                f.write(f"- {flag}\n")
            f.write("\n")


def cmd_report(args) -> int:
    """Execute report command."""
    conn = db_connect(args.db)
    row_count = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
    date_str = args.date or today_et()

    # Startup trust output
    print(f"\n{'='*50}", flush=True)
    print(f"REPORT MODE", flush=True)
    print(f"{'='*50}", flush=True)
    print(f"  DB:     {args.db} ({row_count} rows)", flush=True)

    # Step 1: Pull and upsert executions if live
    if args.source == "live":
        print(f"  TWS:    {args.host}:{args.port}", flush=True)
        try:
            account_id = get_account_id(args.host, args.port, args.client_id)
            print(f"  Account: {account_id}", flush=True)
        except Exception as e:
            print(f"  Account: CONNECTION FAILED - {e}", flush=True)

        print(f"{'='*50}\n", flush=True)
        print(f"[{ts_et()}] Pulling executions from TWS...", flush=True)
        try:
            live_execs = pull_executions_live(args.host, args.port, args.client_id + 1, args.scope)
            inserted = db_upsert_executions(conn, live_execs)
            print(f"[{ts_et()}] Pulled {len(live_execs)} executions, {inserted} new", flush=True)
        except Exception as e:
            print(f"[{ts_et()}] ERROR pulling: {e}", flush=True)
            print(f"[{ts_et()}] Continuing with existing DB data...", flush=True)
    else:
        print(f"  Source: DEMO (offline)", flush=True)
        print(f"{'='*50}\n", flush=True)

    # Step 2: Query from SQLite (source of truth)
    sec_type = "OPT" if args.scope == "options" else ("STK" if args.scope == "stocks" else None)

    if sec_type:
        execs_today = db_get_executions_on_date(conn, date_str, sec_type)
        all_execs = db_get_executions_up_to_date(conn, date_str, sec_type)
    else:
        # Both: combine
        execs_today = db_get_executions_on_date(conn, date_str, "OPT") + db_get_executions_on_date(conn, date_str, "STK")
        all_execs = db_get_executions_up_to_date(conn, date_str, "OPT") + db_get_executions_up_to_date(conn, date_str, "STK")

    # Filter by session if specified
    if args.session == "rth":
        execs_today = [r for r in execs_today if r["session_tag"] == "RTH"]
    elif args.session == "ah":
        execs_today = [r for r in execs_today if r["session_tag"] == "AH"]
    # "both" = no filter

    print(f"[{ts_et()}] Date: {date_str} | Executions today: {len(execs_today)} | Total history: {len(all_execs)}", flush=True)

    if not execs_today:
        print(f"[{ts_et()}] No executions found for {date_str}.", flush=True)
        conn.close()
        return 0

    # Step 3: Compute expirations (synthetic closes at $0 for expired options)
    synthetic_execs, expirations = compute_expiration_synthetics(all_execs, date_str)
    if synthetic_execs:
        print(f"[{ts_et()}] {len(synthetic_execs)} expired option(s) — adding synthetic close(s) at $0.00", flush=True)
        all_execs_with_synth = all_execs + synthetic_execs
        execs_today = execs_today + synthetic_execs
    else:
        all_execs_with_synth = all_execs

    # Step 4: Compute Daily Realized PnL (FIFO across full history, including synthetics)
    fifo = compute_fifo_realized_for_date(all_execs_with_synth, date_str)

    # Step 5: Compute opened today & still open (include synthetics so expired positions are closed out)
    real_execs_today = [r for r in execs_today if not r.get("synthetic")]
    opened_today = compute_opened_today_cost(execs_today)

    # Commission coverage for today's real executions (exclude synthetics)
    comm_present = sum(1 for r in real_execs_today if r.get("commission") is not None)
    comm_total = len(real_execs_today)

    # Step 6: Print terminal summary
    print(f"\n{'='*60}")
    print(f"DAILY REPORT — {date_str} — {args.scope}")
    print(f"{'='*60}")
    real_buys = fifo.buys_count - len([s for s in synthetic_execs if s["side"] == "BOT"])
    real_sells = fifo.sells_count - len(synthetic_execs)
    print(f"Executions: {len(real_execs_today)} ({real_buys} buys, {real_sells} sells)")
    print(f"Commission data: {comm_present}/{comm_total} executions")

    # Headline: Daily Realized P&L
    print(f"\nDAILY REALIZED P&L:")
    if fifo.missing_history_flags:
        print(f"  ** INCOMPLETE — {len(fifo.missing_history_flags)} missing history flag(s) **")
    sign_gross = "+" if fifo.realized_gross >= 0 else ""
    sign_net = "+" if fifo.realized_net >= 0 else ""
    print(f"  Gross: {sign_gross}${fifo.realized_gross:.2f}")
    print(f"  Fees:  ${fifo.commissions_on_date:.2f}")
    print(f"  Net:   {sign_net}${fifo.realized_net:.2f}")

    if fifo.missing_history_flags:
        print(f"\n  Missing history:")
        for flag in fifo.missing_history_flags:
            print(f"    - {flag}")

    # P&L by Symbol
    if fifo.by_symbol:
        print(f"\nP&L by Symbol (gross):")
        for sym, pnl in sorted(fifo.by_symbol.items(), key=lambda x: x[1], reverse=True):
            sign = "+" if pnl >= 0 else ""
            print(f"  {sym}: {sign}${pnl:.2f}")

    # Opened Today & Still Open (cost basis only)
    if opened_today:
        total_cost = sum(o["cost_today"] for o in opened_today)
        print(f"\nOPENED TODAY & STILL OPEN (cost basis only):")
        print(f"  Total cost: ${total_cost:.2f}")
        for o in opened_today:
            contract = f"{o['symbol']} {o.get('expiry', '')} {o.get('strike', '')} {o.get('right', '')}".strip()
            print(f"  - {contract} | qty={int(o['qty'])} | cost=${o['cost_today']:.2f}")

    # Expired Worthless
    if expirations:
        total_loss = sum(e.cost_basis for e in expirations)
        print(f"\nEXPIRED WORTHLESS:")
        print(f"  Total loss: -${total_loss:.2f}")
        for e in expirations:
            contract = f"{e.symbol} {e.expiry} {e.strike} {e.right}".strip()
            print(f"  - {contract} | qty={int(e.qty_expired)} | cost basis=${e.cost_basis:.2f} | realized=-${e.cost_basis:.2f}")

    print(f"{'='*60}\n")

    # Step 7: --explain SYMBOL drill-down
    explain_sym = getattr(args, "explain", None)
    explain_sells = []
    if explain_sym:
        print(f"{'═'*60}")
        print(f"EXPLAIN: {explain_sym.upper()}")
        print(f"{'═'*60}")

        sells = explain_symbol_fifo(all_execs_with_synth, [date_str], explain_sym)
        explain_sells = sells

        if not sells:
            print(f"  No sells found for {explain_sym.upper()} on {date_str}.")
        else:
            total_explain_pnl = 0.0
            for s in sells:
                ts_str = s["ts_et"].split(" ")[1] if " " in s["ts_et"] else s["ts_et"]
                synth_tag = " [EXPIRED]" if s.get("synthetic") else ""
                print(f"\n  SLD {s['date']} {ts_str} | {s['contract']} | qty={s['qty']:.0f} | price=${s['price']:.2f}{synth_tag}")
                for m in s["matches"]:
                    buy_ts = m["buy_ts"].split(" ")[1] if " " in m["buy_ts"] else m["buy_ts"]
                    buy_date = m["buy_ts"][:10]
                    sign = "+" if m["pnl"] >= 0 else ""
                    print(f"    -> matched BOT {buy_date} {buy_ts} | qty={m['qty']:.0f} | price=${m['buy_price']:.2f} | PnL={sign}${m['pnl']:.2f}")
                if s["unmatched_qty"] > 0:
                    print(f"    -> MISSING: {s['unmatched_qty']:.0f} lots unmatched")
                sign = "+" if s["total_pnl"] >= 0 else ""
                print(f"    sell total: {sign}${s['total_pnl']:.2f}")
                total_explain_pnl += s["total_pnl"]

            sign = "+" if total_explain_pnl >= 0 else ""
            print(f"\n  {explain_sym.upper()} total: {sign}${total_explain_pnl:.2f}")

        print(f"{'='*60}\n")

    # Step 8: Write markdown report
    report_path = f"reports/daily/{date_str}_{args.scope}.md"
    write_report_md(report_path, date_str, args.scope, args.session, execs_today, fifo,
                    opened_today, expirations, explain_sells=explain_sells,
                    explain_symbol=explain_sym)
    print(f"[{ts_et()}] Report saved: {report_path}", flush=True)

    conn.close()
    return 0


# =============================================================================
# PULL (backfill execution history)
# =============================================================================

def cmd_pull(args) -> int:
    """Pull and backfill execution history from TWS."""
    # Resolve date range
    from_date_str = getattr(args, "from_date", None)
    to_date_str = getattr(args, "to_date", None)
    if from_date_str:
        try:
            start_dt = datetime.strptime(from_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"ERROR: --from must be YYYY-MM-DD, got '{from_date_str}'", flush=True)
            return 1
        end_date_str = to_date_str or date.today().isoformat()
        try:
            datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            print(f"ERROR: --to must be YYYY-MM-DD, got '{end_date_str}'", flush=True)
            return 1
        range_label = f"{from_date_str} -> {end_date_str}"
    else:
        if to_date_str:
            print("WARNING: --to is ignored without --from, using --days instead.", flush=True)
        start_dt = datetime.now(timezone.utc) - timedelta(days=args.days)
        end_date_str = None
        range_label = f"last {args.days} days"

    conn = db_connect(args.db)
    row_count_before = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

    # Determine sec_type for DB queries
    scope_sec_types = {"options": ["OPT"], "stocks": ["STK"], "both": ["OPT", "STK"]}
    sec_types = scope_sec_types[args.scope]

    # Pre-pull DB coverage
    for st in sec_types:
        min_d, max_d = db_get_date_range(conn, st)
        if min_d:
            count = conn.execute("SELECT COUNT(*) FROM executions WHERE sec_type = ?", (st,)).fetchone()[0]
            print(f"  DB coverage ({st}): {min_d} to {max_d} ({count} rows)", flush=True)
        else:
            print(f"  DB coverage ({st}): empty", flush=True)

    print(f"\n{'='*50}", flush=True)
    print(f"PULL (backfill history)", flush=True)
    print(f"{'='*50}", flush=True)
    print(f"  DB:     {args.db} ({row_count_before} rows)", flush=True)
    print(f"  TWS:    {args.host}:{args.port}", flush=True)
    print(f"  Scope:  {args.scope}", flush=True)
    print(f"  Range:  {range_label}", flush=True)

    try:
        account_id = get_account_id(args.host, args.port, args.client_id)
        print(f"  Account: {account_id}", flush=True)
    except Exception as e:
        print(f"  Account: CONNECTION FAILED - {e}", flush=True)
        conn.close()
        return 1

    print(f"{'='*50}\n", flush=True)

    # IB execution filter supports afterTime in "yyyyMMdd HH:mm:ss" UTC format
    try:
        from ib_insync import ExecutionFilter
    except ImportError:
        print("ERROR: ib_insync not available.", flush=True)
        conn.close()
        return 1

    after_time = start_dt.strftime("%Y%m%d-%H:%M:%S")

    ef = ExecutionFilter()
    ef.time = after_time

    print(f"[{ts_et()}] Requesting executions since {after_time} UTC...", flush=True)

    try:
        live_execs = pull_executions_live(args.host, args.port, args.client_id + 1, args.scope, exec_filter=ef)
    except Exception as e:
        print(f"[{ts_et()}] ERROR pulling: {e}", flush=True)
        conn.close()
        return 1

    inserted = db_upsert_executions(conn, live_execs)
    row_count_after = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

    # Commission coverage
    with_comm = sum(1 for r in live_execs if r.get("commission") is not None)
    total = len(live_execs)

    print(f"[{ts_et()}] Pulled {total} executions, {inserted} new", flush=True)
    print(f"[{ts_et()}] Commission coverage: {with_comm}/{total}", flush=True)
    print(f"[{ts_et()}] DB: {row_count_before} -> {row_count_after} rows", flush=True)

    # Post-pull DB coverage
    print(f"\n  Updated DB coverage:", flush=True)
    for st in sec_types:
        min_d, max_d = db_get_date_range(conn, st)
        if min_d:
            count = conn.execute("SELECT COUNT(*) FROM executions WHERE sec_type = ?", (st,)).fetchone()[0]
            print(f"    {st}: {min_d} to {max_d} ({count} rows)", flush=True)
        else:
            print(f"    {st}: empty", flush=True)

    # Missing-day warnings (only when --from/--to specified)
    if from_date_str:
        print(f"\n  Checking coverage for {from_date_str} -> {end_date_str}...", flush=True)
        missing_days = []
        d = date.fromisoformat(from_date_str)
        end_d = date.fromisoformat(end_date_str)
        while d <= end_d:
            # Skip weekends (Mon=0, Sun=6)
            if d.weekday() < 5:
                day_str = d.isoformat()
                has_data = False
                for st in sec_types:
                    rows = db_get_executions_on_date(conn, day_str, st)
                    if rows:
                        has_data = True
                        break
                if not has_data:
                    missing_days.append(day_str)
            d += timedelta(days=1)

        if missing_days:
            print(f"\n  WARNING: No executions found for {len(missing_days)} business day(s):", flush=True)
            for md in missing_days:
                print(f"    - {md}", flush=True)
            print(f"  This could mean: (a) no trades that day, or (b) TWS didn't return data that far back.", flush=True)

        # TWS limitation warning
        days_ago = (datetime.now(timezone.utc) - start_dt).days
        if total == 0 and days_ago > 7:
            print(f"\n  WARNING: TWS typically only provides ~7 days of execution history.", flush=True)
            print(f"  For older data, use: python copilot.py import --csv <activity_statement.csv>", flush=True)
            print(f"  Download your Activity Statement from IBKR Account Management > Reports.", flush=True)
        elif missing_days and days_ago > 7:
            print(f"\n  HINT: TWS typically only provides ~7 days of execution history.", flush=True)
            print(f"  For older data, use: python copilot.py import --csv <activity_statement.csv>", flush=True)

    conn.close()
    return 0


# =============================================================================
# MONITOR MODE (stocks only — intraday % vs prev close)
# =============================================================================

@dataclass
class StockSnapshot:
    """A stock position with current mark and yesterday's close."""
    symbol: str
    qty: float
    mark: float
    prev_close: float
    con_id: Optional[int] = None


def get_demo_snapshots() -> List[StockSnapshot]:
    """Demo stock snapshots for testing."""
    return [
        StockSnapshot(symbol="AAPL", qty=10, mark=194.68, prev_close=190.12),
        StockSnapshot(symbol="PLTR", qty=50, mark=17.8, prev_close=18.2),
        StockSnapshot(symbol="NVDA", qty=5, mark=585.0, prev_close=572.0),
    ]


def get_stock_snapshots_live(host: str, port: int, client_id: int) -> List[StockSnapshot]:
    """
    Get STOCK positions with current mark and yesterday's close from TWS.
    Options are excluded. Symbols missing prev_close are skipped.
    """
    ib = ib_connect(host, port, client_id)
    try:
        ib.sleep(0.5)
        portfolio = ib.portfolio()

        # Filter to stocks with qty
        stock_items = []
        for item in portfolio:
            c = item.contract
            if (getattr(c, "secType", "") != "STK"):
                continue
            qty = float(item.position or 0.0)
            if qty == 0:
                continue
            stock_items.append(item)

        if not stock_items:
            return []

        # Request market data for each stock to get prev close
        tickers = []
        for item in stock_items:
            c = item.contract
            # Qualify the contract so IB can resolve it
            ib.qualifyContracts(c)
            ticker = ib.reqMktData(c, "", False, False)
            tickers.append((item, ticker))

        # Wait for data to arrive
        ib.sleep(2.0)

        snapshots = []
        for item, ticker in tickers:
            c = item.contract
            qty = float(item.position or 0.0)
            mark = float(item.marketPrice or 0.0)

            # Get yesterday's close: prefer ticker.close, fallback to ticker.lastClose
            prev_close = None
            if ticker.close is not None and ticker.close > 0:
                prev_close = float(ticker.close)
            elif hasattr(ticker, "lastClose") and ticker.lastClose is not None and ticker.lastClose > 0:
                prev_close = float(ticker.lastClose)

            # Skip if no prev close available
            if prev_close is None or prev_close <= 0:
                print(f"  [{getattr(c, 'symbol', '?')}] no prev close — skipped", flush=True)
                continue

            if mark <= 0:
                continue

            snapshots.append(StockSnapshot(
                symbol=getattr(c, "symbol", "UNK"),
                qty=qty,
                mark=mark,
                prev_close=prev_close,
                con_id=getattr(c, "conId", None),
            ))

        # Cancel market data subscriptions
        for _, ticker in tickers:
            ib.cancelMktData(ticker.contract)

        return snapshots
    finally:
        ib.disconnect()


def evaluate_day_moves(conn: sqlite3.Connection, snapshots: List[StockSnapshot],
                       day_up: float, day_down: float, notify: bool) -> None:
    """Evaluate intraday % change vs prev close and trigger alerts."""
    for s in snapshots:
        if s.prev_close <= 0:
            continue

        day_pct = (s.mark - s.prev_close) / s.prev_close

        # Day up alert
        if day_pct >= day_up:
            inserted = db_try_insert_event(
                conn, s.symbol, "DAY_UP", day_up, day_pct, s.prev_close, s.mark
            )
            if inserted and notify:
                mac_notify(
                    "DAY UP",
                    f"{s.symbol} +{day_pct*100:.1f}% today\nprevClose {s.prev_close:.2f} -> now {s.mark:.2f}"
                )

        # Day down alert
        if day_pct <= day_down:
            inserted = db_try_insert_event(
                conn, s.symbol, "DAY_DOWN", day_down, day_pct, s.prev_close, s.mark
            )
            if inserted and notify:
                mac_notify(
                    "DAY DOWN",
                    f"{s.symbol} {day_pct*100:.1f}% today\nprevClose {s.prev_close:.2f} -> now {s.mark:.2f}"
                )


def print_monitor_summary(snapshots: List[StockSnapshot], day_up: float, day_down: float) -> None:
    """Print monitor summary to terminal."""
    hits_up = []
    hits_down = []

    for s in snapshots:
        if s.prev_close <= 0:
            continue
        day_pct = (s.mark - s.prev_close) / s.prev_close
        if day_pct >= day_up:
            hits_up.append((s.symbol, day_pct, s.prev_close, s.mark))
        if day_pct <= day_down:
            hits_down.append((s.symbol, day_pct, s.prev_close, s.mark))

    print(f"\n[{ts_et()}] Monitor — {len(snapshots)} stocks", flush=True)

    if hits_up:
        print("  DAY UP:", flush=True)
        for sym, pct, pc, mk in sorted(hits_up, key=lambda x: -x[1]):
            print(f"    {sym} +{pct*100:.1f}%  prevClose {pc:.2f} -> now {mk:.2f}", flush=True)
    if hits_down:
        print("  DAY DOWN:", flush=True)
        for sym, pct, pc, mk in sorted(hits_down, key=lambda x: x[1]):
            print(f"    {sym} {pct*100:.1f}%  prevClose {pc:.2f} -> now {mk:.2f}", flush=True)
    if not hits_up and not hits_down:
        print("  No triggers.", flush=True)


def cmd_monitor(args) -> int:
    """Execute monitor command (stocks only — intraday % vs prev close)."""
    # Startup trust output
    print(f"\n{'='*50}", flush=True)
    print(f"MONITOR MODE (INTRADAY MOVE)", flush=True)
    print(f"{'='*50}", flush=True)
    print(f"  Scope:  stocks only", flush=True)
    print(f"  Metric: % vs yesterday close", flush=True)
    print(f"  Up:     +{args.day_up*100:.1f}% | Down: {args.day_down*100:.1f}%", flush=True)
    print(f"  DB:     {args.db} (exists={os.path.exists(args.db)})", flush=True)

    if args.source == "live":
        print(f"  TWS:    {args.host}:{args.port}", flush=True)
        try:
            account_id = get_account_id(args.host, args.port, args.client_id)
            print(f"  Account: {account_id}", flush=True)
        except Exception as e:
            print(f"  Account: CONNECTION FAILED - {e}", flush=True)
            return 1
    else:
        print(f"  Source: DEMO (offline)", flush=True)

    print(f"{'='*50}\n", flush=True)

    conn = db_connect(args.db)

    def run_once():
        if args.source == "demo":
            snapshots = get_demo_snapshots()
        else:
            try:
                snapshots = get_stock_snapshots_live(args.host, args.port, args.client_id + 1)
            except Exception as e:
                print(f"[{ts_et()}] ERROR: {e}", flush=True)
                return

        evaluate_day_moves(conn, snapshots, args.day_up, args.day_down, args.notify)
        print_monitor_summary(snapshots, args.day_up, args.day_down)

    # Run once
    run_once()

    if args.once:
        conn.close()
        return 0

    # Loop
    print(f"\n[{ts_et()}] Monitoring every {args.interval}s. Ctrl+C to stop.", flush=True)
    try:
        while True:
            time.sleep(args.interval)
            run_once()
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
    finally:
        conn.close()

    return 0


# =============================================================================
# SWING MODE (stocks only — entry-based TP/SL with crossing logic)
# =============================================================================

@dataclass
class SwingPosition:
    """A stock position for swing monitoring."""
    symbol: str
    qty: float
    avg_cost: float
    mark: float


@dataclass
class SwingState:
    """Per-symbol state for crossing-based alerts."""
    status: str  # "NEUTRAL", "TP", "SL"
    qty: float
    avg_cost: float


def get_demo_swing_positions() -> List[SwingPosition]:
    """Demo stock positions for swing testing."""
    return [
        SwingPosition(symbol="AAPL", qty=10, avg_cost=180.0, mark=225.5),
        SwingPosition(symbol="PLTR", qty=50, avg_cost=20.0, mark=17.8),
        SwingPosition(symbol="NVDA", qty=5, avg_cost=500.0, mark=585.0),
    ]


def get_swing_positions_live(host: str, port: int, client_id: int) -> List[SwingPosition]:
    """Get STOCK positions from TWS for swing monitoring (options excluded)."""
    raw = pull_positions_live(host, port, client_id, scope="stocks")
    return [
        SwingPosition(
            symbol=r["symbol"],
            qty=r["qty"],
            avg_cost=r["avg_cost"],
            mark=r["mark"],
        )
        for r in raw
        if r["avg_cost"] > 0 and r["mark"] > 0
    ]


def db_log_swing_alert(conn: sqlite3.Connection, symbol: str, event_type: str,
                       pnl_pct: float, avg_cost: float, mark: float) -> None:
    """Log a swing alert to the DB (no dedup — state machine handles that)."""
    conn.execute(
        """
        INSERT INTO swing_alerts (ts, symbol, event_type, pnl_pct, avg_cost, mark)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ts_utc(), symbol, event_type, pnl_pct, avg_cost, mark)
    )
    conn.commit()


def db_load_swing_state(conn: sqlite3.Connection) -> Dict[str, SwingState]:
    """Load persisted swing state from DB."""
    cur = conn.execute("SELECT symbol, status, qty, avg_cost FROM swing_state")
    states = {}
    for row in cur.fetchall():
        states[row["symbol"]] = SwingState(
            status=row["status"],
            qty=float(row["qty"]),
            avg_cost=float(row["avg_cost"]),
        )
    return states


def db_save_swing_state(conn: sqlite3.Connection, states: Dict[str, SwingState]) -> None:
    """Persist swing state to DB (full replace)."""
    conn.execute("DELETE FROM swing_state")
    for sym, st in states.items():
        conn.execute(
            "INSERT INTO swing_state (symbol, status, qty, avg_cost, updated_at) VALUES (?, ?, ?, ?, ?)",
            (sym, st.status, st.qty, st.avg_cost, ts_utc())
        )
    conn.commit()


def db_store_swing_marks(conn: sqlite3.Connection, positions: List["SwingPosition"]) -> None:
    """Store a mark snapshot for each position (called each loop iteration)."""
    now_et = datetime.now(ET)
    ts = now_et.strftime("%Y-%m-%d %H:%M:%S")
    session = classify_session(now_et)
    for p in positions:
        conn.execute(
            "INSERT INTO swing_marks (ts_et, session_tag, symbol, mark, avg_cost, qty) VALUES (?, ?, ?, ?, ?, ?)",
            (ts, session, p.symbol, p.mark, p.avg_cost, p.qty)
        )
    conn.commit()


def db_get_swing_marks_for_date(conn: sqlite3.Connection, date_str: str,
                                session: str = "both") -> List[dict]:
    """Get all mark snapshots for a date, optionally filtered by session."""
    if session == "both":
        cur = conn.execute(
            "SELECT * FROM swing_marks WHERE ts_et LIKE ? ORDER BY ts_et, symbol",
            (f"{date_str}%",)
        )
    elif session == "rth":
        cur = conn.execute(
            "SELECT * FROM swing_marks WHERE ts_et LIKE ? AND session_tag = 'RTH' ORDER BY ts_et, symbol",
            (f"{date_str}%",)
        )
    elif session == "ah":
        cur = conn.execute(
            "SELECT * FROM swing_marks WHERE ts_et LIKE ? AND session_tag = 'AH' ORDER BY ts_et, symbol",
            (f"{date_str}%",)
        )
    else:
        cur = conn.execute(
            "SELECT * FROM swing_marks WHERE ts_et LIKE ? ORDER BY ts_et, symbol",
            (f"{date_str}%",)
        )
    return [dict(row) for row in cur.fetchall()]


def db_upsert_swing_crossing(conn: sqlite3.Connection, date_str: str, symbol: str,
                              cross_type: str, first_ts_et: str, pnl_pct: float,
                              avg_cost: float, mark: float) -> None:
    """Record a crossing (first per symbol/date/type wins — ignore if exists)."""
    try:
        conn.execute(
            """INSERT INTO swing_crossings (date, symbol, cross_type, first_ts_et, pnl_pct_at_cross, avg_cost, mark_at_cross)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (date_str, symbol, cross_type, first_ts_et, pnl_pct, avg_cost, mark)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # already recorded


def _compute_swing_status(pnl_pct: float, tp: float, sl: float) -> str:
    """Determine what status a position should be based on current pnl_pct."""
    if pnl_pct >= tp:
        return "TP"
    if pnl_pct <= sl:
        return "SL"
    return "NEUTRAL"


def evaluate_swing(conn: sqlite3.Connection, positions: List[SwingPosition],
                   tp: float, sl: float, tp_reset: float, sl_reset: float,
                   notify: bool, states: Dict[str, SwingState],
                   alert_existing: bool = False, min_value: float = 0.0) -> int:
    """
    Evaluate TP/SL with crossing logic, hysteresis deadband, and baseline suppression.

    State machine per symbol:
      NEUTRAL -> TP   (when pnl_pct >= tp)        → alert
      NEUTRAL -> SL   (when pnl_pct <= sl)         → alert
      TP -> NEUTRAL   (when pnl_pct < tp_reset)    → silent
      SL -> NEUTRAL   (when pnl_pct > sl_reset)    → silent

    Baseline: If a symbol has no prior state, its initial status is set silently
    (no alert) unless alert_existing is True.

    Reset to NEUTRAL if qty or avg_cost changed, or position reappeared.
    Returns the number of new alerts fired.
    """
    current_symbols = set()
    alerts_fired = 0

    for p in positions:
        current_symbols.add(p.symbol)

        if p.avg_cost <= 0:
            continue

        # Min-value filter
        if min_value > 0 and abs(p.qty * p.mark) < min_value:
            continue

        pnl_pct = (p.mark - p.avg_cost) / p.avg_cost

        prev = states.get(p.symbol)
        is_new = prev is None

        if prev is not None:
            if abs(prev.qty - p.qty) > 1e-9 or abs(prev.avg_cost - p.avg_cost) > 1e-4:
                # Position changed — reset to NEUTRAL (can alert on next crossing)
                states[p.symbol] = SwingState(status="NEUTRAL", qty=p.qty, avg_cost=p.avg_cost)
                prev = states[p.symbol]
                is_new = False  # reset is not a baseline — it can alert
        else:
            # No prior state — baseline: set correct status silently
            initial_status = _compute_swing_status(pnl_pct, tp, sl)
            states[p.symbol] = SwingState(status=initial_status, qty=p.qty, avg_cost=p.avg_cost)
            prev = states[p.symbol]

            if not alert_existing:
                # Baselined silently — skip alert logic for this symbol
                continue
            # With --alert-existing, fall through to normal logic below
            # but force status to NEUTRAL so the crossing check fires
            states[p.symbol].status = "NEUTRAL"
            prev = states[p.symbol]

        status = prev.status

        if status == "NEUTRAL":
            if pnl_pct >= tp:
                states[p.symbol].status = "TP"
                msg = f"{p.symbol} +{pnl_pct*100:.1f}% (avg {p.avg_cost:.2f} -> {p.mark:.2f})"
                print(f"[{ts_et()}] [TP] {msg}", flush=True)
                db_log_swing_alert(conn, p.symbol, "TP", pnl_pct, p.avg_cost, p.mark)
                if notify:
                    mac_notify("TP HIT", msg)
                alerts_fired += 1
            elif pnl_pct <= sl:
                states[p.symbol].status = "SL"
                msg = f"{p.symbol} {pnl_pct*100:.1f}% (avg {p.avg_cost:.2f} -> {p.mark:.2f})"
                print(f"[{ts_et()}] [SL] {msg}", flush=True)
                db_log_swing_alert(conn, p.symbol, "SL", pnl_pct, p.avg_cost, p.mark)
                if notify:
                    mac_notify("SL HIT", msg)
                alerts_fired += 1
        elif status == "TP":
            if pnl_pct < tp_reset:
                states[p.symbol].status = "NEUTRAL"
        elif status == "SL":
            if pnl_pct > sl_reset:
                states[p.symbol].status = "NEUTRAL"

        # Update qty/avg_cost snapshot
        states[p.symbol].qty = p.qty
        states[p.symbol].avg_cost = p.avg_cost

    # Remove state for symbols that disappeared (so they reset on return)
    gone = [sym for sym in states if sym not in current_symbols]
    for sym in gone:
        del states[sym]

    return alerts_fired


def cmd_swing(args) -> int:
    """Execute swing monitor (stocks only — entry-based TP/SL with crossing logic)."""
    is_arm = getattr(args, "arm", False)
    alert_existing = getattr(args, "alert_existing", False)
    min_value = getattr(args, "min_value", 0.0)

    # Startup banner
    print(f"\n{'='*50}", flush=True)
    print(f"MONITOR SWING{' (ARM)' if is_arm else ''}", flush=True)
    print(f"{'='*50}", flush=True)
    print(f"  Scope:  stocks only", flush=True)
    print(f"  Metric: (mark - avg_cost) / avg_cost", flush=True)
    print(f"  TP:     +{args.tp*100:.0f}% (reset < +{args.tp_reset*100:.0f}%)", flush=True)
    print(f"  SL:     {args.sl*100:.0f}% (reset > {args.sl_reset*100:.0f}%)", flush=True)
    if min_value > 0:
        print(f"  Filter: min position value ${min_value:.0f}", flush=True)
    print(f"  DB:     {args.db} (exists={os.path.exists(args.db)})", flush=True)

    if args.source == "live":
        print(f"  TWS:    {args.host}:{args.port}", flush=True)
        try:
            account_id = get_account_id(args.host, args.port, args.client_id)
            print(f"  Account: {account_id}", flush=True)
        except Exception as e:
            print(f"  Account: CONNECTION FAILED - {e}", flush=True)
            return 1
    else:
        print(f"  Source: DEMO (offline)", flush=True)

    print(f"{'='*50}\n", flush=True)

    conn = db_connect(args.db)

    # Load persisted state from DB
    states = db_load_swing_state(conn)
    if states:
        print(f"[{ts_et()}] Loaded state for {len(states)} symbols from DB.", flush=True)
    else:
        print(f"[{ts_et()}] No prior state in DB — baselining positions.", flush=True)

    heartbeat_interval = args.heartbeat * 60
    last_heartbeat = time.monotonic()

    def get_positions() -> List[SwingPosition]:
        if args.source == "demo":
            return get_demo_swing_positions()
        return get_swing_positions_live(args.host, args.port, args.client_id + 1)

    # --arm mode: baseline all positions and exit (no alerts)
    if is_arm:
        try:
            positions = get_positions()
        except Exception as e:
            print(f"[{ts_et()}] ERROR: {e}", flush=True)
            conn.close()
            return 1

        for p in positions:
            if p.avg_cost <= 0:
                continue
            if min_value > 0 and abs(p.qty * p.mark) < min_value:
                continue
            pnl_pct = (p.mark - p.avg_cost) / p.avg_cost
            status = _compute_swing_status(pnl_pct, args.tp, args.sl)
            states[p.symbol] = SwingState(status=status, qty=p.qty, avg_cost=p.avg_cost)
            print(f"  {p.symbol:8s}  {status:7s}  pnl={pnl_pct*100:+.1f}%  avg={p.avg_cost:.2f}  mark={p.mark:.2f}", flush=True)

        db_save_swing_state(conn, states)
        print(f"\n[{ts_et()}] Armed {len(states)} positions. State saved to DB.", flush=True)
        conn.close()
        return 0

    def run_once(count_positions: list):
        try:
            positions = get_positions()
        except Exception as e:
            print(f"[{ts_et()}] ERROR: {e}", flush=True)
            return 0

        # Store mark snapshots for day-summary replay
        db_store_swing_marks(conn, positions)

        alerts = evaluate_swing(
            conn, positions, args.tp, args.sl, args.tp_reset, args.sl_reset,
            args.notify, states, alert_existing=alert_existing, min_value=min_value
        )
        db_save_swing_state(conn, states)
        count_positions.clear()
        count_positions.append(len(positions))
        return alerts

    # Run once
    pos_count: list = []
    run_once(pos_count)

    if args.once:
        conn.close()
        return 0

    # Loop
    print(f"[{ts_et()}] Swing monitoring every {args.interval}s (heartbeat every {args.heartbeat}m). Ctrl+C to stop.", flush=True)
    try:
        while True:
            time.sleep(args.interval)
            alerts = run_once(pos_count)

            # Heartbeat
            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_interval:
                n = pos_count[0] if pos_count else 0
                if not alerts:
                    print(f"[{ts_et()}] Checked {n} stock positions. No new crossings.", flush=True)
                last_heartbeat = now
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
    finally:
        db_save_swing_state(conn, states)
        conn.close()

    return 0


# =============================================================================
# SWING-DAY (day summary from stored marks)
# =============================================================================

def replay_crossings(marks: List[dict], tp: float, sl: float,
                     tp_reset: float, sl_reset: float,
                     min_value: float = 0.0) -> List[dict]:
    """
    Replay mark snapshots through the hysteresis state machine.
    Returns list of crossing events: {symbol, cross_type, first_ts_et, pnl_pct, avg_cost, mark}.
    """
    # Group marks by symbol, sorted by time
    by_symbol: Dict[str, List[dict]] = defaultdict(list)
    for m in marks:
        by_symbol[m["symbol"]].append(m)

    crossings = []

    for sym, rows in by_symbol.items():
        rows = sorted(rows, key=lambda r: r["ts_et"])
        status = "NEUTRAL"

        for r in rows:
            avg_cost = float(r["avg_cost"])
            mark = float(r["mark"])
            qty = float(r["qty"])

            if avg_cost <= 0:
                continue
            if min_value > 0 and abs(qty * mark) < min_value:
                continue

            pnl_pct = (mark - avg_cost) / avg_cost

            if status == "NEUTRAL":
                if pnl_pct >= tp:
                    status = "TP"
                    crossings.append({
                        "symbol": sym,
                        "cross_type": "TP",
                        "first_ts_et": r["ts_et"],
                        "pnl_pct": pnl_pct,
                        "avg_cost": avg_cost,
                        "mark": mark,
                    })
                elif pnl_pct <= sl:
                    status = "SL"
                    crossings.append({
                        "symbol": sym,
                        "cross_type": "SL",
                        "first_ts_et": r["ts_et"],
                        "pnl_pct": pnl_pct,
                        "avg_cost": avg_cost,
                        "mark": mark,
                    })
            elif status == "TP":
                if pnl_pct < tp_reset:
                    status = "NEUTRAL"
            elif status == "SL":
                if pnl_pct > sl_reset:
                    status = "NEUTRAL"

    return sorted(crossings, key=lambda c: c["first_ts_et"])


def cmd_swing_day(args) -> int:
    """Replay stored marks and show crossing summary for a date."""
    date_str = args.date or today_et()
    session = args.session
    min_value = getattr(args, "min_value", 0.0)

    print(f"\n{'='*50}", flush=True)
    print(f"SWING DAY SUMMARY", flush=True)
    print(f"{'='*50}", flush=True)
    print(f"  Date:    {date_str}", flush=True)
    print(f"  Session: {session}", flush=True)
    print(f"  TP:      +{args.tp*100:.0f}% (reset < +{args.tp_reset*100:.0f}%)", flush=True)
    print(f"  SL:      {args.sl*100:.0f}% (reset > {args.sl_reset*100:.0f}%)", flush=True)
    if min_value > 0:
        print(f"  Filter:  min value ${min_value:.0f}", flush=True)
    print(f"  DB:      {args.db}", flush=True)
    print(f"{'='*50}\n", flush=True)

    conn = db_connect(args.db)

    # Load marks for date
    marks = db_get_swing_marks_for_date(conn, date_str, session)
    if not marks:
        print(f"No mark snapshots found for {date_str} (session={session}).", flush=True)
        print(f"Marks are recorded when 'swing' runs. Start swing first to collect data.", flush=True)
        conn.close()
        return 0

    # Count unique symbols and snapshots
    symbols = set(m["symbol"] for m in marks)
    first_ts = marks[0]["ts_et"].split(" ")[1] if marks else ""
    last_ts = marks[-1]["ts_et"].split(" ")[1] if marks else ""
    print(f"Loaded {len(marks)} snapshots for {len(symbols)} symbols ({first_ts} - {last_ts})", flush=True)

    # Replay crossings
    crossings = replay_crossings(marks, args.tp, args.sl, args.tp_reset, args.sl_reset, min_value)

    if not crossings:
        print(f"\nNo crossings detected on {date_str}.", flush=True)
        conn.close()
        return 0

    # Store crossings to DB
    for c in crossings:
        db_upsert_swing_crossing(
            conn, date_str, c["symbol"], c["cross_type"],
            c["first_ts_et"], c["pnl_pct"], c["avg_cost"], c["mark"]
        )

    # Print summary
    tp_crossings = [c for c in crossings if c["cross_type"] == "TP"]
    sl_crossings = [c for c in crossings if c["cross_type"] == "SL"]

    print(f"\nCrossings: {len(crossings)} total ({len(tp_crossings)} TP, {len(sl_crossings)} SL)\n", flush=True)

    if tp_crossings:
        print("TP CROSSINGS:", flush=True)
        for c in tp_crossings:
            ts = c["first_ts_et"].split(" ")[1] if " " in c["first_ts_et"] else c["first_ts_et"]
            print(f"  {ts}  {c['symbol']:8s}  +{c['pnl_pct']*100:.1f}%  avg={c['avg_cost']:.2f}  mark={c['mark']:.2f}", flush=True)
        print(flush=True)

    if sl_crossings:
        print("SL CROSSINGS:", flush=True)
        for c in sl_crossings:
            ts = c["first_ts_et"].split(" ")[1] if " " in c["first_ts_et"] else c["first_ts_et"]
            print(f"  {ts}  {c['symbol']:8s}  {c['pnl_pct']*100:.1f}%  avg={c['avg_cost']:.2f}  mark={c['mark']:.2f}", flush=True)
        print(flush=True)

    # Write markdown report
    report_dir = "reports"
    os.makedirs(report_dir, exist_ok=True)
    report_path = f"{report_dir}/{date_str}_swing.md"
    with open(report_path, "w") as f:
        f.write(f"# Swing Day Summary — {date_str}\n\n")
        f.write(f"- Session: **{session}**\n")
        f.write(f"- TP: +{args.tp*100:.0f}% (reset +{args.tp_reset*100:.0f}%)\n")
        f.write(f"- SL: {args.sl*100:.0f}% (reset {args.sl_reset*100:.0f}%)\n")
        f.write(f"- Snapshots: {len(marks)} | Symbols: {len(symbols)}\n")
        f.write(f"- Generated: {ts_et()} ET\n\n")

        f.write(f"## Crossings ({len(crossings)} total)\n\n")
        f.write("| Time (ET) | Symbol | Type | PnL % | Avg Cost | Mark |\n")
        f.write("|-----------|--------|------|------:|---------:|-----:|\n")
        for c in crossings:
            ts = c["first_ts_et"].split(" ")[1] if " " in c["first_ts_et"] else c["first_ts_et"]
            sign = "+" if c["pnl_pct"] >= 0 else ""
            f.write(f"| {ts} | {c['symbol']} | {c['cross_type']} | {sign}{c['pnl_pct']*100:.1f}% | {c['avg_cost']:.2f} | {c['mark']:.2f} |\n")
        f.write("\n")

    print(f"Report saved: {report_path}", flush=True)
    conn.close()
    return 0


# =============================================================================
# WEEKLY REPORT
# =============================================================================

def db_get_date_range(conn: sqlite3.Connection, sec_type: str = None) -> tuple:
    """Return (earliest_date, latest_date) of executions in DB as YYYY-MM-DD strings."""
    if sec_type:
        row = conn.execute(
            "SELECT MIN(ts_et), MAX(ts_et) FROM executions WHERE sec_type = ?",
            (sec_type,)
        ).fetchone()
    else:
        row = conn.execute("SELECT MIN(ts_et), MAX(ts_et) FROM executions").fetchone()
    if row and row[0]:
        return (row[0][:10], row[1][:10])
    return (None, None)


def group_missing_flags(flags: List[str]) -> Dict[str, int]:
    """Group missing history flags by symbol. Returns {symbol: count}."""
    counts: Dict[str, int] = defaultdict(int)
    for flag in flags:
        # Format: "SYM (con_id=N): sold X, missing Y lots in history"
        sym = flag.split(" (")[0] if " (" in flag else flag.split(":")[0]
        counts[sym] += 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def explain_symbol_fifo(all_execs: List[dict], report_dates: List[str],
                        target_symbol: str) -> List[dict]:
    """
    Detailed FIFO replay for a specific symbol across given report dates.

    Returns list of sell events with lot-matching detail:
    [
        {
            "date": "2026-01-27",
            "ts_et": "2026-01-27 10:15:00",
            "con_id": 123,
            "contract": "INTC 20260130 49.0 C",
            "qty": 5,
            "price": 0.48,
            "matches": [
                {"qty": 3, "buy_price": 0.35, "buy_ts": "2026-01-22 09:45:00", "pnl": 390.00},
                {"qty": 2, "buy_price": 0.40, "buy_ts": "2026-01-23 11:00:00", "pnl": 160.00},
            ],
            "unmatched_qty": 0,
            "total_pnl": 550.00,
        },
    ]
    """
    report_date_set = set(report_dates)
    execs_sorted = sorted(all_execs, key=lambda r: r["ts_et"])

    # FIFO lots per con_id: deque of [qty_remaining, entry_price, multiplier, symbol, ts_et]
    lots = defaultdict(deque)
    results = []

    for r in execs_sorted:
        ts_et_str = r["ts_et"]
        sell_date = ts_et_str[:10]
        is_in_range = sell_date in report_date_set

        con_id = r.get("con_id") or 0
        sym = r.get("symbol") or "UNK"
        side = r["side"]
        qty = float(r["qty"])
        price = float(r["price"])
        mult = float(r.get("multiplier") or 100.0)

        if side == "BOT":
            lots[con_id].append([qty, price, mult, sym, ts_et_str])
        else:  # SLD
            matches = []
            remaining_sell = qty
            realized_this_sell = 0.0

            while remaining_sell > 0 and lots[con_id]:
                lot = lots[con_id][0]
                lot_qty, lot_price, lot_mult, lot_sym, lot_ts = lot

                match_qty = min(remaining_sell, lot_qty)
                pnl = (price - lot_price) * match_qty * lot_mult
                realized_this_sell += pnl

                if is_in_range and sym.upper() == target_symbol.upper():
                    matches.append({
                        "qty": match_qty,
                        "buy_price": lot_price,
                        "buy_ts": lot_ts,
                        "pnl": pnl,
                    })

                lot[0] -= match_qty
                remaining_sell -= match_qty

                if lot[0] <= 1e-9:
                    lots[con_id].popleft()

            if is_in_range and sym.upper() == target_symbol.upper():
                contract_parts = [sym]
                if r.get("expiry"):
                    contract_parts.append(str(r["expiry"]))
                if r.get("strike"):
                    contract_parts.append(str(r["strike"]))
                if r.get("right"):
                    contract_parts.append(str(r["right"]))

                results.append({
                    "date": sell_date,
                    "ts_et": ts_et_str,
                    "con_id": con_id,
                    "contract": " ".join(contract_parts),
                    "qty": qty,
                    "price": price,
                    "multiplier": mult,
                    "matches": matches,
                    "unmatched_qty": remaining_sell if remaining_sell > 1e-9 else 0,
                    "total_pnl": realized_this_sell,
                    "synthetic": r.get("synthetic"),
                })

    return results


def week_range(args) -> tuple:
    """
    Compute (start_date, end_date) as YYYY-MM-DD strings for the weekly report.

    Logic:
    - --from/--to: explicit range
    - --week DATE: find the Monday of that date's week, run Mon-Fri (or Mon-today if current week)
    - Default: current week Mon -> today (ET)
    """
    today = date.fromisoformat(today_et())

    from_date = getattr(args, "from_date", None)
    to_date = getattr(args, "to_date", None)
    week_date = getattr(args, "week", None)

    if from_date and to_date:
        return (from_date, to_date)

    if week_date:
        d = date.fromisoformat(week_date)
    else:
        d = today

    # Find Monday of d's week (weekday: Mon=0, Sun=6)
    monday = d - timedelta(days=d.weekday())
    friday = monday + timedelta(days=4)

    # Cap end at today if this is the current or future week
    if friday >= today:
        end = today
    else:
        end = friday

    return (monday.isoformat(), end.isoformat())


def write_weekly_report_md(path: str, start_date: str, end_date: str, scope: str,
                           day_rows: List[WeeklyDayRow],
                           weekly_by_symbol: Dict[str, float],
                           expirations: List[ExpirationInfo],
                           total_missing: int,
                           db_earliest: Optional[str] = None,
                           db_latest: Optional[str] = None,
                           total_execs_in_range: int = 0,
                           total_sells: int = 0,
                           total_matched_sells: int = 0,
                           all_missing_flags: Optional[List[str]] = None,
                           pulled_count: int = 0,
                           pulled_new: int = 0,
                           explain_sells: Optional[List[dict]] = None,
                           explain_symbol: Optional[str] = None,
                           top_n: int = 5) -> None:
    """Write markdown weekly report file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    weekly_gross = sum(d.gross for d in day_rows)
    weekly_comm = sum(d.commissions for d in day_rows)
    weekly_net = sum(d.net for d in day_rows)

    with open(path, "w") as f:
        f.write(f"# Weekly Report — {start_date} to {end_date}\n\n")

        # Weekly Summary
        f.write("## Weekly Summary\n\n")
        sign_net = "+" if weekly_net >= 0 else ""
        f.write(f"**Net: {sign_net}${weekly_net:.2f}**\n\n")
        f.write("| | |\n|---|---:|\n")
        sign_gross = "+" if weekly_gross >= 0 else ""
        f.write(f"| Gross | {sign_gross}${weekly_gross:.2f} |\n")
        f.write(f"| Fees | ${weekly_comm:.2f} |\n")
        f.write(f"| **Net** | **{sign_net}${weekly_net:.2f}** |\n\n")

        if expirations:
            total_exp_loss = sum(e.cost_basis for e in expirations)
            f.write(f"Expired worthless total: **-${total_exp_loss:.2f}**\n\n")

        # Reconciliation
        f.write("## Reconciliation\n\n")
        if db_earliest and db_latest:
            f.write(f"DB history covers: **{db_earliest}** to **{db_latest}**\n\n")
        f.write(f"- Executions in range: **{total_execs_in_range}**\n")
        f.write(f"- Closed trades (matched sells): **{total_matched_sells}** / {total_sells}\n")
        if total_missing > 0:
            f.write(f"- Unmatched sells (missing history): **{total_missing}**\n")
        f.write("\n")

        # Sanity check: sum of daily nets
        net_parts = []
        for d in day_rows:
            if d.net >= 0:
                net_parts.append(f"+{d.net:.2f}")
            else:
                net_parts.append(f"{d.net:.2f}")
        math_line = " + ".join(net_parts)
        check = "OK" if abs(sum(d.net for d in day_rows) - weekly_net) < 0.01 else "MISMATCH"
        f.write(f"Weekly Net = Sum(Daily Net): {math_line} = **{sign_net}${weekly_net:.2f}** {check}\n\n")

        # Daily Breakdown
        f.write("## Daily Breakdown\n\n")
        f.write("| Date | Gross | Fees | Net |\n")
        f.write("|------|------:|-----:|----:|\n")
        for d in day_rows:
            sign = "+" if d.net >= 0 else ""
            sign_g = "+" if d.gross >= 0 else ""
            f.write(f"| {d.date} | {sign_g}${d.gross:.2f} | ${d.commissions:.2f} | {sign}${d.net:.2f} |\n")
        f.write("\n")

        # Top Symbols
        if weekly_by_symbol:
            sorted_syms = sorted(weekly_by_symbol.items(), key=lambda x: x[1], reverse=True)
            winners = [(s, p) for s, p in sorted_syms if p > 0]
            losers = [(s, p) for s, p in sorted_syms if p < 0]

            f.write("## Top Symbols\n\n")

            if winners:
                f.write(f"### Top {min(len(winners), top_n)} Winners\n\n")
                for sym, pnl in winners[:top_n]:
                    f.write(f"- {sym}: +${pnl:.2f}\n")
                f.write("\n")

            if losers:
                f.write(f"### Top {min(len(losers), top_n)} Losers\n\n")
                for sym, pnl in losers[:top_n]:
                    f.write(f"- {sym}: ${pnl:.2f}\n")
                f.write("\n")

        # Expired Worthless
        if expirations:
            total_loss = sum(e.cost_basis for e in expirations)
            f.write("## Expired Worthless\n\n")
            f.write(f"Total loss: **-${total_loss:.2f}**\n\n")
            f.write("| Contract | Qty | Cost Basis | Realized |\n")
            f.write("|----------|----:|-----------:|---------:|\n")
            for e in expirations:
                contract = f"{e.symbol} {e.expiry} {e.strike} {e.right}".strip()
                f.write(f"| {contract} | {int(e.qty_expired)} | ${e.cost_basis:.2f} | -${e.cost_basis:.2f} |\n")
            f.write("\n")

        # Explain
        if explain_sells is not None and explain_symbol:
            f.write(f"## Explain: {explain_symbol.upper()}\n\n")
            _write_explain_md(f, explain_sells, explain_symbol)

        # Data Quality
        if total_missing > 0:
            grouped = group_missing_flags(all_missing_flags or [])
            f.write("## Data Quality\n\n")
            if db_earliest:
                f.write(f"DB history starts at **{db_earliest}**. "
                        f"Sells of positions opened before that date cannot be matched to buy lots.\n\n")
            f.write(f"Realized P&L is **partially complete** — "
                    f"{total_missing} sell(s) could not be matched to prior buys.\n\n")
            f.write(f"- Matched (solid): **{total_matched_sells}** sells\n")
            f.write(f"- Unmatched: **{total_missing}** sells\n\n")
            if grouped:
                f.write("Missing history by symbol:\n\n")
                for sym, count in grouped.items():
                    f.write(f"- {sym}: {count} unmatched\n")
                f.write("\n")

        f.write(f"---\n*Generated: {ts_et()} ET*\n")


def weekly_csv_summary(conn: sqlite3.Connection, start_date: str, end_date: str,
                       sec_type: str = None) -> Optional[dict]:
    """
    Build weekly summary from IBKR CSV realized_csv column.

    Returns None if no realized_csv rows exist in the range.
    Otherwise returns a rich dict for the performance report.
    """
    type_clause = "AND sec_type = ?" if sec_type else ""
    params: list = [start_date, end_date + " 23:59:59"]
    if sec_type:
        params.append(sec_type)

    count = conn.execute(
        f"SELECT COUNT(*) FROM executions WHERE realized_csv IS NOT NULL "
        f"AND ts_et >= ? AND ts_et <= ? {type_clause}",
        params,
    ).fetchone()[0]

    if count == 0:
        return None

    # All rows with realized_csv in range (closes / sells)
    close_rows = conn.execute(
        f"SELECT substr(ts_et, 1, 10) as d, symbol, side, qty, price, "
        f"       commission, realized_csv, basis_csv, proceeds_csv, code "
        f"FROM executions WHERE realized_csv IS NOT NULL "
        f"AND ts_et >= ? AND ts_et <= ? {type_clause} "
        f"ORDER BY ts_et",
        params,
    ).fetchall()

    # Daily aggregates (all rows with realized_csv)
    daily_net: Dict[str, float] = defaultdict(float)
    daily_comm: Dict[str, float] = defaultdict(float)
    daily_trades: Dict[str, int] = defaultdict(int)

    # Close-only stats: side='SLD' identifies actual closes (includes Ep expirations)
    daily_best_close: Dict[str, float] = {}
    daily_worst_close: Dict[str, float] = {}
    daily_close_wins: Dict[str, int] = defaultdict(int)
    daily_close_losses: Dict[str, int] = defaultdict(int)

    wins = 0
    losses = 0
    total_closes = 0
    largest_win = 0.0
    largest_loss = 0.0

    # R-multiple tracking: R = realized / |basis| per close
    all_r_multiples: List[float] = []
    sym_r_multiples: Dict[str, List[float]] = defaultdict(list)

    for r in close_rows:
        d, symbol, side, qty, price, comm, realized, basis, proceeds, code = r
        daily_net[d] += realized
        if comm:
            daily_comm[d] += comm
        daily_trades[d] += 1

        # Close events only (SLD = close or expiration)
        is_close = (side == "SLD")
        if is_close:
            total_closes += 1
            if realized > 0:
                wins += 1
                daily_close_wins[d] += 1
            elif realized < 0:
                losses += 1
                daily_close_losses[d] += 1

            if realized > largest_win:
                largest_win = realized
            if realized < largest_loss:
                largest_loss = realized

            if d not in daily_best_close or realized > daily_best_close[d]:
                daily_best_close[d] = realized
            if d not in daily_worst_close or realized < daily_worst_close[d]:
                daily_worst_close[d] = realized

            # R-multiple
            premium = abs(basis) if basis else 0.0
            if premium > 0:
                r_mult = realized / premium
                all_r_multiples.append(r_mult)
                sym_r_multiples[symbol].append(r_mult)

    avg_r = sum(all_r_multiples) / len(all_r_multiples) if all_r_multiples else 0.0
    best_r = max(all_r_multiples) if all_r_multiples else 0.0
    worst_r = min(all_r_multiples) if all_r_multiples else 0.0
    # Expectancy: avg_r tells you expected R per trade
    sym_avg_r = {sym: sum(rs) / len(rs) for sym, rs in sym_r_multiples.items()}

    # Premium deployed: sum of abs(basis_csv) for BOT rows
    buy_rows = conn.execute(
        f"SELECT basis_csv FROM executions WHERE basis_csv IS NOT NULL AND side = 'BOT' "
        f"AND ts_et >= ? AND ts_et <= ? {type_clause}",
        params,
    ).fetchall()
    total_premium_deployed = sum(abs(r[0]) for r in buy_rows if r[0])

    weekly_net = sum(daily_net.values())
    weekly_comm = sum(daily_comm.values())
    weekly_gross = weekly_net + abs(weekly_comm)  # realized includes comm, so gross = net + |comm|

    # Top symbols
    sym_rows = conn.execute(
        f"SELECT symbol, SUM(realized_csv) as pnl "
        f"FROM executions WHERE realized_csv IS NOT NULL "
        f"AND ts_et >= ? AND ts_et <= ? {type_clause} "
        f"GROUP BY symbol ORDER BY pnl DESC",
        params,
    ).fetchall()
    top_symbols = [(r[0], r[1]) for r in sym_rows]

    # Expirations (code containing 'Ep')
    exp_rows = conn.execute(
        f"SELECT symbol, qty, realized_csv, basis_csv "
        f"FROM executions WHERE code LIKE '%Ep%' "
        f"AND ts_et >= ? AND ts_et <= ? {type_clause} "
        f"ORDER BY realized_csv",
        params,
    ).fetchall()
    expirations = [{"symbol": r[0], "qty": abs(r[1]), "realized_csv": r[2],
                    "basis_csv": abs(r[3]) if r[3] else 0.0} for r in exp_rows]
    total_expired_loss = sum(e["realized_csv"] for e in expirations)

    # Total executions in range
    total_execs = conn.execute(
        f"SELECT COUNT(*) FROM executions "
        f"WHERE ts_et >= ? AND ts_et <= ? {type_clause}",
        params,
    ).fetchone()[0]

    # Total losses (for expiration ratio)
    total_losses = sum(r[6] for r in close_rows if r[6] < 0)

    return {
        "daily_net": dict(daily_net),
        "daily_comm": dict(daily_comm),
        "daily_trades": dict(daily_trades),
        "daily_best_close": daily_best_close,
        "daily_worst_close": daily_worst_close,
        "daily_close_wins": dict(daily_close_wins),
        "daily_close_losses": dict(daily_close_losses),
        "weekly_net": weekly_net,
        "weekly_gross": weekly_gross,
        "weekly_comm": weekly_comm,
        "top_symbols": top_symbols,
        "expirations": expirations,
        "total_expired_loss": total_expired_loss,
        "total_execs": total_execs,
        "total_closes": total_closes,
        "wins": wins,
        "losses": losses,
        "largest_win": largest_win,
        "largest_loss": largest_loss,
        "total_premium_deployed": total_premium_deployed,
        "total_losses": total_losses,
        "avg_r": avg_r,
        "best_r": best_r,
        "worst_r": worst_r,
        "sym_avg_r": sym_avg_r,
    }


def cmd_week(args) -> int:
    """Execute weekly report command."""
    conn = db_connect(args.db)
    row_count = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

    start_date, end_date = week_range(args)
    explain_sym = getattr(args, "explain", None)

    sec_type = "OPT" if args.scope == "options" else ("STK" if args.scope == "stocks" else None)

    # DB coverage
    db_earliest, db_latest = db_get_date_range(conn, sec_type)

    # Startup banner
    print(f"\n{'='*60}", flush=True)
    print(f"WEEKLY REPORT", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  DB:     {args.db} ({row_count} rows)", flush=True)
    if db_earliest:
        print(f"  DB history: {db_earliest} to {db_latest}", flush=True)
    print(f"  Range:  {start_date} to {end_date}", flush=True)
    print(f"  Scope:  {args.scope}", flush=True)

    pulled_count = 0
    pulled_new = 0

    if args.source == "live":
        print(f"  TWS:    {args.host}:{args.port}", flush=True)
        try:
            account_id = get_account_id(args.host, args.port, args.client_id)
            print(f"  Account: {account_id}", flush=True)
        except Exception as e:
            print(f"  Account: CONNECTION FAILED - {e}", flush=True)

        print(f"{'='*60}\n", flush=True)
        print(f"[{ts_et()}] Pulling executions from TWS...", flush=True)
        try:
            live_execs = pull_executions_live(args.host, args.port, args.client_id + 1, args.scope)
            pulled_count = len(live_execs)
            pulled_new = db_upsert_executions(conn, live_execs)
            print(f"[{ts_et()}] Pulled {pulled_count} executions, {pulled_new} new", flush=True)
            # Refresh coverage after pull
            db_earliest, db_latest = db_get_date_range(conn, sec_type)
        except Exception as e:
            print(f"[{ts_et()}] ERROR pulling: {e}", flush=True)
            print(f"[{ts_et()}] Continuing with existing DB data...", flush=True)
    else:
        print(f"  Source: DEMO (offline)", flush=True)
        print(f"{'='*60}\n", flush=True)

    # Try CSV-based summary first (authoritative IBKR statement data)
    csv_summary = weekly_csv_summary(conn, start_date, end_date, sec_type)

    # Collect report_dates for --explain
    report_dates: List[str] = []
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while current <= end:
        if current.weekday() < 5:
            report_dates.append(current.isoformat())
        current += timedelta(days=1)

    if csv_summary:
        # ── CSV path: IBKR Statement realized P/L (authoritative) ──
        s = csv_summary
        weekly_net = s["weekly_net"]
        weekly_gross = s["weekly_gross"]
        weekly_comm = s["weekly_comm"]
        daily_net = s["daily_net"]
        daily_trades = s["daily_trades"]
        total_execs_in_range = s["total_execs"]
        win_rate = (s["wins"] / s["total_closes"] * 100) if s["total_closes"] else 0.0
        ropc = (weekly_net / s["total_premium_deployed"] * 100) if s["total_premium_deployed"] else 0.0
        exp_ratio = (abs(s["total_expired_loss"]) / abs(s["total_losses"]) * 100) if s["total_losses"] else 0.0

        # Find day with most trades, day with biggest loss
        busiest_day = max(report_dates, key=lambda d: daily_trades.get(d, 0)) if report_dates else ""
        worst_day = min(report_dates, key=lambda d: daily_net.get(d, 0.0)) if report_dates else ""

        # Concentration: top symbol % of weekly net
        top_sym_pnl = s["top_symbols"][0][1] if s["top_symbols"] else 0.0
        top_sym_name = s["top_symbols"][0][0] if s["top_symbols"] else ""
        concentration = (abs(top_sym_pnl) / abs(weekly_net) * 100) if weekly_net else 0.0

        # ── Header ──
        print(f"WEEKLY PERFORMANCE REPORT -- {start_date} to {end_date}")
        print(f"Source: IBKR Statement Realized P/L")
        print(f"{'─'*60}")

        # ── Core Numbers ──
        print(f"\n  Core Numbers")
        print(f"  {'Weekly Net PnL:':<30} ${weekly_net:>10,.2f}")
        print(f"  {'Weekly Gross PnL:':<30} ${weekly_gross:>10,.2f}")
        print(f"  {'Total Commissions:':<30} ${abs(weekly_comm):>10,.2f}")
        print(f"  {'Win Rate:':<30} {win_rate:>10.1f}%    ({s['wins']}W / {s['losses']}L)")
        print(f"  {'Total Closed Trades:':<30} {s['total_closes']:>10}")
        print(f"  {'Premium Deployed:':<30} ${s['total_premium_deployed']:>10,.2f}")
        print(f"  {'Return on Premium:':<30} {ropc:>10.1f}%")
        print(f"  {'Avg R-Multiple:':<30} {s['avg_r']:>+10.2f}R")
        print(f"  {'Best R:':<30} {s['best_r']:>+10.2f}R")
        print(f"  {'Worst R:':<30} {s['worst_r']:>+10.2f}R")

        # ── Daily Breakdown ──
        print(f"\n  Daily Breakdown")
        print(f"  {'Date':<12} {'Net':>10} {'W/L':>7} {'Best Close':>12} {'Worst Close':>12}")
        print(f"  {'─'*12} {'─'*10} {'─'*7} {'─'*12} {'─'*12}")
        for d_str in report_dates:
            net = daily_net.get(d_str, 0.0)
            w = s["daily_close_wins"].get(d_str, 0)
            l = s["daily_close_losses"].get(d_str, 0)
            wl = f"{w}W/{l}L"
            best = s["daily_best_close"].get(d_str)
            worst = s["daily_worst_close"].get(d_str)
            best_s = f"${best:>10,.2f}" if best is not None else f"{'--':>11}"
            worst_s = f"${worst:>10,.2f}" if worst is not None else f"{'--':>11}"
            print(f"  {d_str:<12} ${net:>9,.2f} {wl:>7} {best_s:>12} {worst_s:>12}")

        # ── Risk & Discipline ──
        print(f"\n  Risk & Discipline")
        print(f"  {'Largest Single Win:':<30} ${s['largest_win']:>10,.2f}")
        print(f"  {'Largest Single Loss:':<30} ${s['largest_loss']:>10,.2f}")
        print(f"  {'Total Expired Loss:':<30} ${s['total_expired_loss']:>10,.2f}")
        print(f"  {'Expirations as % of Losses:':<30} {exp_ratio:>10.1f}%")
        print(f"  {'Number of Expirations:':<30} {len(s['expirations']):>10}")

        # ── Behavioral Signals ──
        print(f"\n  Behavioral Signals")
        print(f"  {'Busiest Day:':<30} {busiest_day} ({daily_trades.get(busiest_day, 0)} trades)")
        print(f"  {'Worst Day:':<30} {worst_day} (${daily_net.get(worst_day, 0.0):,.2f})")
        print(f"  {'Top Concentration:':<30} {top_sym_name} = {concentration:.0f}% of weekly net")

        # ── Top Symbols ──
        if s["top_symbols"]:
            print(f"\n  Top Symbols (Net Realized)")
            print(f"  {'':>2}{'Symbol':<36} {'Net':>11} {'Avg R':>8}")
            print(f"  {'':>2}{'─'*36} {'─'*11} {'─'*8}")
            for sym, pnl in s["top_symbols"]:
                avg_r_sym = s["sym_avg_r"].get(sym)
                r_str = f"{avg_r_sym:>+7.2f}R" if avg_r_sym is not None else f"{'--':>8}"
                print(f"  {'':>2}{sym:<36} ${pnl:>10,.2f} {r_str}")

        # ── Expired Worthless ──
        if s["expirations"]:
            print(f"\n  Expired Worthless (included in realized above)")
            print(f"  {'Symbol':<36} {'Qty':>5} {'Cost':>10} {'Realized':>10}")
            print(f"  {'─'*36} {'─'*5} {'─'*10} {'─'*10}")
            for e in s["expirations"]:
                print(f"  {e['symbol']:<36} {int(e['qty']):>5} ${e['basis_csv']:>9,.2f} ${e['realized_csv']:>9,.2f}")

        # ── Reconciliation ──
        check_sum = sum(daily_net.get(d, 0.0) for d in report_dates)
        ok = abs(check_sum - weekly_net) < 0.01
        print(f"\n  {'─'*60}")
        net_parts = [f"{daily_net.get(d, 0.0):+.2f}" for d in report_dates]
        print(f"  Reconciled: {'YES' if ok else 'NO'}  ({' + '.join(net_parts)} = ${weekly_net:,.2f})")
        print(f"{'='*60}\n")

        # For md report and --explain, build minimal day_rows
        day_rows = []
        for d_str in report_dates:
            day_rows.append(WeeklyDayRow(
                date=d_str, gross=0.0, commissions=0.0,
                net=daily_net.get(d_str, 0.0), execs_count=daily_trades.get(d_str, 0),
                sells_count=0, matched_sells=0, missing_count=0, missing_flags=[],
            ))
        weekly_by_symbol = dict(s["top_symbols"])
        all_expirations = []
        total_missing = 0
        all_missing_flags = []
        total_sells = 0
        total_matched_sells = 0

    else:
        # ── FIFO path: no CSV data, use analytical FIFO ──
        day_rows: List[WeeklyDayRow] = []
        weekly_by_symbol: Dict[str, float] = defaultdict(float)
        all_expirations: List[ExpirationInfo] = []
        all_missing_flags: List[str] = []
        total_missing = 0
        total_execs_in_range = 0
        total_sells = 0
        total_matched_sells = 0

        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        while current <= end:
            d_str = current.isoformat()

            if current.weekday() >= 5:
                current += timedelta(days=1)
                continue

            if sec_type:
                all_execs = db_get_executions_up_to_date(conn, d_str, sec_type)
                execs_today = db_get_executions_on_date(conn, d_str, sec_type)
            else:
                all_execs = (db_get_executions_up_to_date(conn, d_str, "OPT")
                             + db_get_executions_up_to_date(conn, d_str, "STK"))
                execs_today = (db_get_executions_on_date(conn, d_str, "OPT")
                               + db_get_executions_on_date(conn, d_str, "STK"))

            if args.session == "rth":
                execs_today = [r for r in execs_today if r["session_tag"] == "RTH"]
            elif args.session == "ah":
                execs_today = [r for r in execs_today if r["session_tag"] == "AH"]

            total_execs_in_range += len(execs_today)

            synthetic_execs, exp_infos = compute_expiration_synthetics(all_execs, d_str)
            if synthetic_execs:
                all_execs_with_synth = all_execs + synthetic_execs
            else:
                all_execs_with_synth = all_execs

            fifo = compute_fifo_realized_for_date(all_execs_with_synth, d_str)

            matched = fifo.sells_count - fifo.unmatched_sells_on_date
            total_sells += fifo.sells_count
            total_matched_sells += matched

            day_rows.append(WeeklyDayRow(
                date=d_str,
                gross=fifo.realized_gross,
                commissions=fifo.commissions_on_date,
                net=fifo.realized_net,
                execs_count=len(execs_today),
                sells_count=fifo.sells_count,
                matched_sells=matched,
                missing_count=fifo.unmatched_sells_on_date,
                missing_flags=fifo.missing_on_date_flags,
            ))

            for sym, pnl in fifo.by_symbol.items():
                weekly_by_symbol[sym] += pnl

            all_expirations.extend(exp_infos)
            all_missing_flags.extend(fifo.missing_on_date_flags)
            total_missing += fifo.unmatched_sells_on_date

            current += timedelta(days=1)

        weekly_gross = sum(d.gross for d in day_rows)
        weekly_comm = sum(d.commissions for d in day_rows)
        weekly_net = sum(d.net for d in day_rows)

        sign_net = "+" if weekly_net >= 0 else ""
        print(f"Source: Analytical FIFO (no IBKR CSV data in range)")
        print(f"Weekly Net: {sign_net}${weekly_net:.2f}")
        sign_gross = "+" if weekly_gross >= 0 else ""
        print(f"  Gross: {sign_gross}${weekly_gross:.2f} | Fees: ${weekly_comm:.2f}")

        print(f"\nDaily Breakdown:")
        for d in day_rows:
            sign = "+" if d.net >= 0 else ""
            print(f"  {d.date}: {sign}${d.net:.2f} net")

        print(f"\nReconciliation:")
        if db_earliest:
            print(f"  DB history covers: {db_earliest} to {db_latest}")
        if args.source == "live":
            print(f"  Executions pulled: {pulled_count} ({pulled_new} new)")
        print(f"  Executions in range: {total_execs_in_range}")
        print(f"  Closed trades (matched sells): {total_matched_sells} / {total_sells}")
        net_parts = [f"{d.net:+.2f}" for d in day_rows]
        math_line = " + ".join(net_parts)
        ok = abs(sum(d.net for d in day_rows) - weekly_net) < 0.01
        print(f"  Weekly Net = Sum(Daily Net):")
        print(f"  {math_line} = {sign_net}${weekly_net:.2f} {'OK' if ok else 'MISMATCH'}")

        if weekly_by_symbol:
            sorted_syms = sorted(weekly_by_symbol.items(), key=lambda x: x[1], reverse=True)
            print(f"\nTop Symbols (weekly net):")
            for sym, pnl in sorted_syms[:10]:
                sign = "+" if pnl >= 0 else ""
                print(f"  {sym}: {sign}${pnl:.2f}")

        if all_expirations:
            total_exp_loss = sum(e.cost_basis for e in all_expirations)
            print(f"\nExpired Worthless (weekly):")
            print(f"  Total: -${total_exp_loss:.2f}")
            for e in all_expirations:
                contract = f"{e.symbol} {e.expiry} {e.strike} {e.right}".strip()
                print(f"  {contract} | qty={int(e.qty_expired)} | -${e.cost_basis:.2f}")

        if total_missing > 0:
            print(f"\nData Quality:")
            print(f"  Unmatched sells: {total_missing}")
            grouped = group_missing_flags(all_missing_flags)
            for sym, count in list(grouped.items())[:5]:
                print(f"    {sym}: {count}")
            if len(grouped) > 5:
                print(f"    ...and {len(grouped) - 5} more symbols")
            print(f"  Note: Missing history means sells can't be matched because")
            print(f"        DB does not contain earlier buys (before {db_earliest}).")

        print(f"{'='*60}\n")

    # ── --explain SYMBOL drill-down ──
    explain_sells = []
    if explain_sym:
        print(f"{'═'*60}")
        print(f"EXPLAIN: {explain_sym.upper()}")
        print(f"{'═'*60}")

        # Need all execs up to end_date for full FIFO history
        if sec_type:
            all_execs_full = db_get_executions_up_to_date(conn, end_date, sec_type)
        else:
            all_execs_full = (db_get_executions_up_to_date(conn, end_date, "OPT")
                              + db_get_executions_up_to_date(conn, end_date, "STK"))

        # Add synthetics for each day in range
        for d_str in report_dates:
            if sec_type:
                day_execs = db_get_executions_up_to_date(conn, d_str, sec_type)
            else:
                day_execs = (db_get_executions_up_to_date(conn, d_str, "OPT")
                             + db_get_executions_up_to_date(conn, d_str, "STK"))
            synth, _ = compute_expiration_synthetics(day_execs, d_str)
            all_execs_full.extend(synth)

        explain_sells = explain_symbol_fifo(all_execs_full, report_dates, explain_sym)
        sells = explain_sells

        if not sells:
            print(f"  No sells found for {explain_sym.upper()} in {start_date} to {end_date}.")
        else:
            total_explain_pnl = 0.0
            for s in sells:
                ts = s["ts_et"].split(" ")[1] if " " in s["ts_et"] else s["ts_et"]
                synth_tag = " [EXPIRED]" if s.get("synthetic") else ""
                print(f"\n  SLD {s['date']} {ts} | {s['contract']} | qty={s['qty']:.0f} | price=${s['price']:.2f}{synth_tag}")
                for m in s["matches"]:
                    buy_ts = m["buy_ts"].split(" ")[1] if " " in m["buy_ts"] else m["buy_ts"]
                    buy_date = m["buy_ts"][:10]
                    sign = "+" if m["pnl"] >= 0 else ""
                    print(f"    -> matched BOT {buy_date} {buy_ts} | qty={m['qty']:.0f} | price=${m['buy_price']:.2f} | PnL={sign}${m['pnl']:.2f}")
                if s["unmatched_qty"] > 0:
                    print(f"    -> MISSING: {s['unmatched_qty']:.0f} lots unmatched")
                sign = "+" if s["total_pnl"] >= 0 else ""
                print(f"    sell total: {sign}${s['total_pnl']:.2f}")
                total_explain_pnl += s["total_pnl"]

            sign = "+" if total_explain_pnl >= 0 else ""
            print(f"\n  {explain_sym.upper()} weekly total: {sign}${total_explain_pnl:.2f}")

        print(f"{'='*60}\n")

    # Write markdown report
    report_path = f"reports/weekly/{start_date}_to_{end_date}_{args.scope}.md"
    write_weekly_report_md(
        report_path, start_date, end_date, args.scope,
        day_rows, dict(weekly_by_symbol), all_expirations, total_missing,
        db_earliest=db_earliest, db_latest=db_latest,
        total_execs_in_range=total_execs_in_range,
        total_sells=total_sells, total_matched_sells=total_matched_sells,
        all_missing_flags=all_missing_flags,
        pulled_count=pulled_count, pulled_new=pulled_new,
        explain_sells=explain_sells if explain_sym else None,
        explain_symbol=explain_sym,
    )
    print(f"[{ts_et()}] Report saved: {report_path}", flush=True)

    conn.close()
    return 0


# =============================================================================
# IMPORT (IBKR CSV backfill)
# =============================================================================

def _parse_ibkr_datetime(dt_str: str) -> tuple:
    """
    Parse IBKR Activity Statement date/time string.

    Formats observed:
    - "2026-01-22, 09:30:00"  (Activity Statement)
    - "2026-01-22 09:30:00"
    - "20260122 09:30:00"
    - "20260122;093000" (Flex Query)

    Returns (ts_utc, ts_et, session_tag) — assumes input is ET.
    """
    dt_str = dt_str.strip().replace(";", " ").replace(",", "")

    # Normalize "20260122 093000" → "2026-01-22 09:30:00"
    if len(dt_str) >= 8 and dt_str[:8].isdigit():
        d_part = dt_str[:8]
        rest = dt_str[8:].strip()
        dt_str = f"{d_part[:4]}-{d_part[4:6]}-{d_part[6:8]}"
        if rest:
            # Normalize "093000" → "09:30:00"
            if len(rest) == 6 and rest.isdigit():
                rest = f"{rest[:2]}:{rest[2:4]}:{rest[4:6]}"
            dt_str += f" {rest}"

    # Parse
    for fmt_str in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(dt_str, fmt_str)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Cannot parse IBKR datetime: {dt_str!r}")

    dt_et = dt.replace(tzinfo=ET)
    dt_utc = dt_et.astimezone(timezone.utc)

    ts_et_str = dt_et.strftime("%Y-%m-%d %H:%M:%S")
    ts_utc_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
    session_tag = classify_session(dt_et)

    return ts_utc_str, ts_et_str, session_tag


def _stable_exec_id(row_dict: dict) -> str:
    """Generate a stable exec_id hash from row data when no exec_id is present."""
    key_parts = [
        str(row_dict.get("symbol", "")),
        str(row_dict.get("ts_et", "")),
        str(row_dict.get("side", "")),
        str(row_dict.get("qty", "")),
        str(row_dict.get("price", "")),
        str(row_dict.get("con_id", "")),
    ]
    key = "|".join(key_parts)
    return "CSV_" + hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_ibkr_side(qty_str: str) -> tuple:
    """
    Parse quantity from IBKR CSV — negative = sell, positive = buy.
    Returns (side, abs_qty).
    """
    qty = float(qty_str.replace(",", ""))
    if qty < 0:
        return "SLD", abs(qty)
    return "BOT", abs(qty)


def _parse_ibkr_option_symbol(symbol_str: str) -> dict:
    """
    Parse IBKR Activity Statement option symbol format.

    Format: UNDERLYING DDMMMYY STRIKE C/P
    Example: GOOGL 11FEB26 325 C  →  underlying=GOOGL, expiry=20260211, strike=325.0, right=C

    Returns dict with keys: underlying, expiry, strike, right.
    Raises ValueError if the symbol doesn't match expected format.
    """
    parts = symbol_str.strip().split()
    if len(parts) < 4:
        raise ValueError(f"Cannot parse option symbol: {symbol_str!r}")

    underlying = parts[0]
    ddmmmyy = parts[1]
    strike = float(parts[2])
    right = parts[3].upper()

    # Convert DDMMMYY → YYYYMMDD (e.g. 11FEB26 → 20260211)
    MONTHS = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }
    if len(ddmmmyy) != 7:
        raise ValueError(f"Cannot parse date portion: {ddmmmyy!r}")
    dd = ddmmmyy[:2]
    mmm = ddmmmyy[2:5].upper()
    yy = ddmmmyy[5:7]
    if mmm not in MONTHS:
        raise ValueError(f"Unknown month: {mmm!r} in {ddmmmyy!r}")
    expiry = f"20{yy}{MONTHS[mmm]}{dd}"

    return {"underlying": underlying, "expiry": expiry, "strike": strike, "right": right}


def cmd_import(args) -> int:
    """Import executions from an IBKR Activity Statement CSV."""
    csv_path = args.csv
    if not os.path.isfile(csv_path):
        print(f"ERROR: File not found: {csv_path}", flush=True)
        return 1

    conn = db_connect(args.db)
    row_count_before = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"IMPORT — IBKR CSV", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  DB:     {args.db} ({row_count_before} rows)", flush=True)
    print(f"  CSV:    {csv_path}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # Read CSV — only process Trades,Header and Trades,Data rows
    rows_to_insert = []
    errors = []
    headers = None

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)

        for line_num, row in enumerate(reader, 1):
            if len(row) < 3:
                continue

            section = row[0].strip()
            discriminator = row[1].strip()

            if section != "Trades":
                continue

            if discriminator == "Header":
                headers = [h.strip() for h in row]
                continue

            if discriminator != "Data":
                continue  # skip SubTotal, Total

            if not headers or len(row) < len(headers):
                continue

            h = {headers[i]: row[i].strip() for i in range(len(headers))}

            try:
                asset_cat = h.get("Asset Category", "").upper()
                if asset_cat == "STOCKS":
                    sec_type = "STK"
                elif asset_cat in ("OPTIONS", "EQUITY AND INDEX OPTIONS"):
                    sec_type = "OPT"
                else:
                    sec_type = "STK"

                # Date/Time
                dt_field = h.get("Date/Time", "")
                if not dt_field:
                    continue
                ts_utc, ts_et, session_tag = _parse_ibkr_datetime(dt_field)

                # Quantity & side (qty>0 BOT, qty<0 SLD)
                qty_str = h.get("Quantity", "0")
                side, qty = _parse_ibkr_side(qty_str)

                # Code field — expiration synthetic close (Ep) → force SLD at price 0
                code = h.get("Code", "")
                is_expiry = "Ep" in code

                # Price — T. Price; for expirations it's already 0
                price = float(h.get("T. Price", "0").replace(",", ""))
                if is_expiry:
                    side = "SLD"
                    price = 0.0

                # Commission
                comm_str = h.get("Comm/Fee", "").replace(",", "")
                commission = abs(float(comm_str)) if comm_str else None

                # CSV statement columns (nullable — only set for CSV imports)
                _real = h.get("Realized P/L", "").replace(",", "")
                realized_csv = float(_real) if _real else None
                _basis = h.get("Basis", "").replace(",", "")
                basis_csv = float(_basis) if _basis else None
                _proc = h.get("Proceeds", "").replace(",", "")
                proceeds_csv = float(_proc) if _proc else None

                # Symbol parsing — options have compound symbol, stocks are plain
                raw_symbol = h.get("Symbol", "")
                if not raw_symbol:
                    continue

                if sec_type == "OPT":
                    parsed = _parse_ibkr_option_symbol(raw_symbol)
                    symbol = raw_symbol
                    expiry = parsed["expiry"]
                    strike = parsed["strike"]
                    right = parsed["right"]
                else:
                    symbol = raw_symbol
                    expiry = ""
                    strike = None
                    right = ""

                multiplier = 100.0 if sec_type == "OPT" else 1.0
                account = h.get("Account", "") or h.get("AccountId", "")
                con_id = None

                row_dict = {
                    "ts_utc": ts_utc,
                    "ts_et": ts_et,
                    "session_tag": session_tag,
                    "account": account,
                    "con_id": con_id,
                    "symbol": symbol,
                    "sec_type": sec_type,
                    "expiry": expiry,
                    "strike": strike,
                    "right": right,
                    "multiplier": multiplier,
                    "side": side,
                    "qty": qty,
                    "price": price,
                    "commission": commission,
                    "realized_csv": realized_csv,
                    "basis_csv": basis_csv,
                    "proceeds_csv": proceeds_csv,
                    "code": code,
                }

                row_dict["exec_id"] = _stable_exec_id(row_dict)
                rows_to_insert.append(row_dict)

            except Exception as e:
                errors.append(f"Line {line_num}: {e}")

    if not rows_to_insert:
        print("No trade rows found in CSV.", flush=True)
        if errors:
            print(f"\nParse errors ({len(errors)}):", flush=True)
            for err in errors[:10]:
                print(f"  {err}", flush=True)
        conn.close()
        return 1

    # Insert
    inserted = db_upsert_executions(conn, rows_to_insert)
    row_count_after = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

    opt_rows = [r for r in rows_to_insert if r["sec_type"] == "OPT"]
    stk_rows = [r for r in rows_to_insert if r["sec_type"] == "STK"]

    print(f"Parsed: {len(rows_to_insert)} trade rows ({len(opt_rows)} options, {len(stk_rows)} stocks)", flush=True)
    print(f"Inserted: {inserted} new ({len(rows_to_insert) - inserted} duplicates skipped)", flush=True)
    print(f"DB rows: {row_count_before} -> {row_count_after}", flush=True)

    if errors:
        print(f"\nParse errors ({len(errors)}):", flush=True)
        for err in errors[:10]:
            print(f"  {err}", flush=True)
        if len(errors) > 10:
            print(f"  ...and {len(errors) - 10} more", flush=True)

    # Per-day counts for imported rows
    day_counts: Dict[str, int] = defaultdict(int)
    for r in rows_to_insert:
        day = r["ts_et"][:10]
        day_counts[day] += 1
    print(f"\nPer-day breakdown:", flush=True)
    for day in sorted(day_counts):
        print(f"  {day}: {day_counts[day]} trades", flush=True)

    # Show date range of imported data
    db_earliest, db_latest = db_get_date_range(conn)
    if db_earliest:
        print(f"\nDB history now covers: {db_earliest} to {db_latest}", flush=True)

    print(f"{'='*60}\n", flush=True)
    conn.close()
    return 0


# =============================================================================
# MAIN CLI
# =============================================================================

# Connection defaults: env vars override built-in defaults
_DEFAULT_HOST = os.environ.get("TWS_HOST", "127.0.0.1")
_DEFAULT_PORT = int(os.environ.get("TWS_PORT", "7496"))
_DEFAULT_CLIENT_ID = int(os.environ.get("TWS_CLIENT_ID", "17"))


def _add_tws_args(parser: argparse.ArgumentParser) -> None:
    """Add --host/--port/--client-id with env-var defaults, hidden from normal help."""
    parser.add_argument("--host", default=_DEFAULT_HOST, help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, default=_DEFAULT_PORT, help=argparse.SUPPRESS)
    parser.add_argument("--client-id", type=int, default=_DEFAULT_CLIENT_ID, help=argparse.SUPPRESS)


def main():
    parser = argparse.ArgumentParser(
        prog="copilot",
        description="TWS Copilot - Reports + Alerts (NO TRADING)"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # --- REPORT subcommand (options + stocks, realized PnL + open positions) ---
    report_parser = subparsers.add_parser("report", help="Daily options report: realized PnL + opened-today cost")
    report_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")
    report_parser.add_argument("--source", choices=["live", "demo"], default="live",
                               help="Data source (default: live)")
    report_parser.add_argument("--scope", choices=["options", "stocks", "both"], default="options",
                               help="Instrument scope (default: options)")
    report_parser.add_argument("--date", help="Report date in ET (YYYY-MM-DD), default=today")
    report_parser.add_argument("--session", choices=["rth", "ah", "both"], default="both",
                               help="Session filter (default: both)")
    _add_tws_args(report_parser)
    report_parser.add_argument("--explain", metavar="SYMBOL",
                               help="Show detailed FIFO lot matching for a symbol")

    # --- MONITOR subcommand (stocks only, intraday % vs prev close) ---
    monitor_parser = subparsers.add_parser("monitor", help="Stock day-move alerts (%% vs prev close, options excluded)")
    monitor_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")
    monitor_parser.add_argument("--source", choices=["live", "demo"], default="live",
                                help="Data source (default: live)")
    monitor_parser.add_argument("--day-up", type=float, default=0.02,
                                help="Day up threshold (default: +2%%)")
    monitor_parser.add_argument("--day-down", type=float, default=-0.02,
                                help="Day down threshold (default: -2%%)")
    monitor_parser.add_argument("--interval", type=int, default=60, help="Seconds between checks (default: 60)")
    monitor_parser.add_argument("--notify", action="store_true", help="Enable macOS notifications")
    monitor_parser.add_argument("--once", action="store_true", help="Run once then exit")
    _add_tws_args(monitor_parser)

    # --- SWING subcommand (stocks only, entry-based TP/SL with crossing logic) ---
    swing_parser = subparsers.add_parser("swing", help="Stock TP/SL alerts with crossing logic (options excluded)")
    swing_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")
    swing_parser.add_argument("--source", choices=["live", "demo"], default="live",
                              help="Data source (default: live)")
    swing_parser.add_argument("--tp", type=float, default=0.25,
                              help="Take profit threshold (default: +25%%)")
    swing_parser.add_argument("--tp-reset", type=float, default=0.23,
                              help="TP reset threshold — return to neutral below this (default: +23%%)")
    swing_parser.add_argument("--sl", type=float, default=-0.10,
                              help="Stop loss threshold (default: -10%%)")
    swing_parser.add_argument("--sl-reset", type=float, default=-0.08,
                              help="SL reset threshold — return to neutral above this (default: -8%%)")
    swing_parser.add_argument("--min-value", type=float, default=0.0,
                              help="Min position value to alert on (default: 0 = no filter)")
    swing_parser.add_argument("--interval", type=int, default=60, help="Seconds between checks (default: 60)")
    swing_parser.add_argument("--heartbeat", type=int, default=5, help="Heartbeat interval in minutes (default: 5)")
    swing_parser.add_argument("--arm", action="store_true",
                              help="Baseline all positions and exit (no alerts)")
    swing_parser.add_argument("--alert-existing", action="store_true",
                              help="Alert on already-breached positions on first run (default: suppress)")
    swing_parser.add_argument("--notify", action="store_true", help="Enable macOS notifications")
    swing_parser.add_argument("--once", action="store_true", help="Run once then exit")
    _add_tws_args(swing_parser)

    # --- PULL subcommand (backfill execution history) ---
    pull_parser = subparsers.add_parser("pull", help="Backfill execution history from TWS")
    pull_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")
    pull_parser.add_argument("--scope", choices=["options", "stocks", "both"], default="options",
                             help="Instrument scope (default: options)")
    pull_date_group = pull_parser.add_mutually_exclusive_group()
    pull_date_group.add_argument("--days", type=int, default=30,
                                 help="Number of days to look back (default: 30)")
    pull_date_group.add_argument("--from", dest="from_date",
                                 help="Start date YYYY-MM-DD (mutually exclusive with --days)")
    pull_parser.add_argument("--to", dest="to_date",
                             help="End date YYYY-MM-DD, used with --from (default: today)")
    _add_tws_args(pull_parser)

    # --- WEEK subcommand (weekly aggregated report) ---
    week_parser = subparsers.add_parser("week", help="Weekly options report")
    week_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")
    week_parser.add_argument("--source", choices=["live", "demo"], default="live",
                             help="Data source (default: live)")
    week_parser.add_argument("--scope", choices=["options", "stocks", "both"], default="options",
                             help="Instrument scope (default: options)")
    week_parser.add_argument("--session", choices=["rth", "ah", "both"], default="both",
                             help="Session filter (default: both)")
    _add_tws_args(week_parser)
    week_group = week_parser.add_mutually_exclusive_group()
    week_group.add_argument("--week", help="Date within target week (YYYY-MM-DD), finds its Monday")
    week_group.add_argument("--from", dest="from_date", help="Explicit start date (YYYY-MM-DD)")
    week_parser.add_argument("--to", dest="to_date", help="Explicit end date (YYYY-MM-DD), used with --from")
    week_parser.add_argument("--explain", metavar="SYMBOL",
                             help="Show detailed FIFO lot matching for a symbol")

    # --- IMPORT subcommand (IBKR CSV backfill) ---
    import_parser = subparsers.add_parser("import", help="Import executions from IBKR Activity Statement CSV")
    import_parser.add_argument("--csv", required=True, help="Path to IBKR CSV file")
    import_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")

    # --- SWING-DAY subcommand (day summary from stored marks) ---
    sd_parser = subparsers.add_parser("swing-day", help="Swing day summary — replay stored marks for crossing detection")
    sd_parser.add_argument("--db", default="copilot.sqlite", help="SQLite database file")
    sd_parser.add_argument("--date", help="Date to summarize (YYYY-MM-DD), default=today")
    sd_parser.add_argument("--session", choices=["rth", "ah", "both"], default="both",
                           help="Session filter (default: both)")
    sd_parser.add_argument("--tp", type=float, default=0.25,
                           help="Take profit threshold (default: +25%%)")
    sd_parser.add_argument("--tp-reset", type=float, default=0.23,
                           help="TP reset threshold (default: +23%%)")
    sd_parser.add_argument("--sl", type=float, default=-0.10,
                           help="Stop loss threshold (default: -10%%)")
    sd_parser.add_argument("--sl-reset", type=float, default=-0.08,
                           help="SL reset threshold (default: -8%%)")
    sd_parser.add_argument("--min-value", type=float, default=0.0,
                           help="Min position value filter (default: 0 = no filter)")

    args = parser.parse_args()

    if args.command == "report":
        return cmd_report(args)
    elif args.command == "pull":
        return cmd_pull(args)
    elif args.command == "monitor":
        return cmd_monitor(args)
    elif args.command == "swing":
        return cmd_swing(args)
    elif args.command == "week":
        return cmd_week(args)
    elif args.command == "swing-day":
        return cmd_swing_day(args)
    elif args.command == "import":
        return cmd_import(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
