# -*- coding: utf-8 -*-
"""
프로그램명 : insurance_repository.py
파일경로   : repositories/insurance_repository.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.common_reference_repository import (
    get_code_options_by_mst_code,
    get_code_options_by_mst_code_id,
    get_insurance_type_options,
    get_worker_options,
    label_by_value,
    table_exists,
)


def _dict_row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ymd(value: Any) -> str:
    raw = re.sub(r"[^0-9]", "", str(value or ""))
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return str(value or "").strip()


def _to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", "") or 0)
    except Exception:
        return 0.0


def _next_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(worker_insurance_id), 800000) + 1 FROM worker_insurance").fetchone()
    return int(row[0]) if row and row[0] else 800001


def get_insurance_options(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "worker": get_worker_options(conn),
        "insurance_type": get_insurance_type_options(conn),
        "insurance_status": get_code_options_by_mst_code_id(conn, 100016, "보험가입상태") or get_code_options_by_mst_code(conn, "INSURANCE_STATUS", "보험가입상태"),
    }


def list_insurances(conn: sqlite3.Connection, search_text: str = "") -> pd.DataFrame:
    if not table_exists(conn, "worker_insurance"):
        return pd.DataFrame()

    opts = get_insurance_options(conn)
    sql = """
        SELECT wi.worker_insurance_id, wi.worker_id, wi.insurance_type_code_id, wi.insurance_status_code_id,
               wi.join_date, wi.leave_date, wi.company_rate, wi.worker_rate, wi.note, wi.created_at, wi.updated_at,
               COALESCE(w.worker_name, '') AS worker_name, COALESCE(w.worker_code, '') AS worker_code
          FROM worker_insurance wi
          LEFT JOIN workers w
            ON wi.worker_id = w.worker_id
         WHERE 1 = 1
    """
    params: List[Any] = []
    if str(search_text).strip():
        keyword = f"%{str(search_text).strip()}%"
        sql += """ AND (
                CAST(wi.worker_insurance_id AS TEXT) LIKE ?
             OR CAST(wi.worker_id AS TEXT) LIKE ?
             OR COALESCE(w.worker_name, '') LIKE ?
             OR COALESCE(w.worker_code, '') LIKE ?
             OR COALESCE(wi.note, '') LIKE ?
        ) """
        params.extend([keyword] * 5)
    sql += " ORDER BY wi.worker_insurance_id DESC"
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df
    df["작업자"] = df.apply(lambda r: f"{r['worker_name']} [{r['worker_code']}]" if str(r.get('worker_code') or '') else str(r.get('worker_name') or ''), axis=1)
    df["보험종류"] = df["insurance_type_code_id"].apply(lambda x: label_by_value(opts["insurance_type"], x))
    df["보험가입상태"] = df["insurance_status_code_id"].apply(lambda x: label_by_value(opts["insurance_status"], x))
    df["회사부담률"] = df["company_rate"].apply(lambda x: f"{_to_float(x):,.2f}")
    df["근로자부담률"] = df["worker_rate"].apply(lambda x: f"{_to_float(x):,.2f}")
    return df


def get_insurance(conn: sqlite3.Connection, worker_insurance_id: int) -> Optional[Dict[str, Any]]:
    if not table_exists(conn, "worker_insurance"):
        return None
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM worker_insurance WHERE worker_insurance_id = ?", (worker_insurance_id,)).fetchone()
    conn.row_factory = None
    return _dict_row(row)


def save_insurance(conn: sqlite3.Connection, payload: Dict[str, Any], actor: str) -> int:
    if not table_exists(conn, "worker_insurance"):
        raise ValueError("worker_insurance 테이블이 없습니다.")

    pk = int(payload.get("worker_insurance_id") or 0)
    values = {
        "worker_id": payload.get("worker_id") or None,
        "insurance_type_code_id": payload.get("insurance_type_code_id") or None,
        "insurance_status_code_id": payload.get("insurance_status_code_id") or None,
        "join_date": _ymd(payload.get("join_date")),
        "leave_date": _ymd(payload.get("leave_date")),
        "company_rate": _to_float(payload.get("company_rate")),
        "worker_rate": _to_float(payload.get("worker_rate")),
        "note": str(payload.get("note") or "").strip(),
    }
    if not values["worker_id"]:
        raise ValueError("작업자를 선택해 주세요.")
    if not values["insurance_type_code_id"]:
        raise ValueError("보험종류를 선택해 주세요.")
    if not values["insurance_status_code_id"]:
        raise ValueError("보험가입상태를 선택해 주세요.")

    if pk:
        conn.execute(
            """
            UPDATE worker_insurance
               SET worker_id=:worker_id, insurance_type_code_id=:insurance_type_code_id,
                   insurance_status_code_id=:insurance_status_code_id, join_date=:join_date,
                   leave_date=:leave_date, company_rate=:company_rate, worker_rate=:worker_rate,
                   note=:note, updated_at=:updated_at, updated_by=:updated_by
             WHERE worker_insurance_id=:worker_insurance_id
            """,
            {**values, "updated_at": _now_str(), "updated_by": actor, "worker_insurance_id": pk},
        )
        return pk

    new_id = _next_id(conn)
    conn.execute(
        """
        INSERT INTO worker_insurance (
            worker_insurance_id, worker_id, insurance_type_code_id, insurance_status_code_id,
            join_date, leave_date, company_rate, worker_rate, note, created_at, created_by
        ) VALUES (
            :worker_insurance_id, :worker_id, :insurance_type_code_id, :insurance_status_code_id,
            :join_date, :leave_date, :company_rate, :worker_rate, :note, :created_at, :created_by
        )
        """,
        {**values, "worker_insurance_id": new_id, "created_at": _now_str(), "created_by": actor},
    )
    return new_id


def delete_insurances(conn: sqlite3.Connection, ids: List[int]) -> int:
    target_ids = [int(x) for x in ids if str(x).strip()]
    if not target_ids:
        return 0
    marks = ",".join(["?"] * len(target_ids))
    cur = conn.execute(f"DELETE FROM worker_insurance WHERE worker_insurance_id IN ({marks})", target_ids)
    return int(cur.rowcount or 0)
