# -*- coding: utf-8 -*-
"""
프로그램명 : project_repository.py
파일경로   : repositories/project_repository.py
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
    get_company_options,
    get_use_yn_options,
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


def _digits(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def _ymd(value: Any) -> str:
    raw = _digits(value)
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return str(value or "").strip()


def _to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", "") or 0)
    except Exception:
        return 0.0


def _next_project_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(project_id), 1200000) + 1 FROM projects").fetchone()
    return int(row[0]) if row and row[0] else 1200001


def _next_project_code(conn: sqlite3.Connection) -> str:
    rows = conn.execute("SELECT project_code FROM projects WHERE project_code LIKE 'JS-P-%'").fetchall()
    max_no = 0
    for row in rows:
        code = str(row[0] or "")
        m = re.search(r"JS-P-(\d+)$", code)
        if m:
            max_no = max(max_no, int(m.group(1)))
    return f"JS-P-{max_no + 1:03d}"


def get_project_options(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "project_type": get_code_options_by_mst_code_id(conn, 100027, "공사구분") or get_code_options_by_mst_code(conn, "PROJECT_TYPE", "공사구분"),
        "project_status": get_code_options_by_mst_code_id(conn, 100028, "프로젝트상태") or get_code_options_by_mst_code(conn, "PROJECT_STATUS", "프로젝트상태"),
        "contract_status": get_code_options_by_mst_code_id(conn, 100029, "계약상태") or get_code_options_by_mst_code(conn, "CONTRACT_STATUS", "계약상태"),
        "use_yn": get_use_yn_options(conn),
        "companies": get_company_options(conn),
        "workers": get_worker_options(conn),
    }


def list_projects(conn: sqlite3.Connection, search_text: str = "") -> pd.DataFrame:
    if not table_exists(conn, "projects"):
        return pd.DataFrame()

    opts = get_project_options(conn)
    sql = """
        SELECT project_id, project_code, project_name, project_type_code_id, project_status_code_id,
               contract_status_code_id, client_company_id, main_contractor_company_id, performing_company_id,
               project_manager_user_id, contract_date, start_date, end_date, site_name, zip_code, address,
               address_detail, contract_amount, description, use_yn_code_id, note, created_at, updated_at
          FROM projects
         WHERE 1 = 1
    """
    params: List[Any] = []
    if str(search_text).strip():
        keyword = f"%{str(search_text).strip()}%"
        sql += """ AND (
                COALESCE(project_code, '') LIKE ?
             OR COALESCE(project_name, '') LIKE ?
             OR COALESCE(site_name, '') LIKE ?
             OR COALESCE(address, '') LIKE ?
        ) """
        params.extend([keyword] * 4)
    sql += " ORDER BY project_id DESC"
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df

    df["공사구분"] = df["project_type_code_id"].apply(lambda x: label_by_value(opts["project_type"], x))
    df["프로젝트상태"] = df["project_status_code_id"].apply(lambda x: label_by_value(opts["project_status"], x))
    df["계약상태"] = df["contract_status_code_id"].apply(lambda x: label_by_value(opts["contract_status"], x))
    df["발주처"] = df["client_company_id"].apply(lambda x: label_by_value(opts["companies"], x))
    df["원청업체"] = df["main_contractor_company_id"].apply(lambda x: label_by_value(opts["companies"], x))
    df["실제수행업체"] = df["performing_company_id"].apply(lambda x: label_by_value(opts["companies"], x))
    df["프로젝트관리자"] = df["project_manager_user_id"].apply(lambda x: label_by_value(opts["workers"], x))
    df["사용여부"] = df["use_yn_code_id"].apply(lambda x: label_by_value(opts["use_yn"], x))
    df["계약금액"] = df["contract_amount"].apply(lambda x: f"{_to_float(x):,.0f}")
    return df


def get_project(conn: sqlite3.Connection, project_id: int) -> Optional[Dict[str, Any]]:
    if not table_exists(conn, "projects"):
        return None
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM projects WHERE project_id = ?", (project_id,)).fetchone()
    conn.row_factory = None
    return _dict_row(row)


def save_project(conn: sqlite3.Connection, payload: Dict[str, Any], actor: str) -> int:
    if not table_exists(conn, "projects"):
        raise ValueError("projects 테이블이 없습니다.")

    pk = int(payload.get("project_id") or 0)
    values = {
        "project_code": str(payload.get("project_code") or "").strip(),
        "project_name": str(payload.get("project_name") or "").strip(),
        "project_type_code_id": payload.get("project_type_code_id") or None,
        "project_status_code_id": payload.get("project_status_code_id") or None,
        "contract_status_code_id": payload.get("contract_status_code_id") or None,
        "client_company_id": payload.get("client_company_id") or None,
        "main_contractor_company_id": payload.get("main_contractor_company_id") or None,
        "performing_company_id": payload.get("performing_company_id") or None,
        "project_manager_user_id": payload.get("project_manager_user_id") or None,
        "contract_date": _ymd(payload.get("contract_date")),
        "start_date": _ymd(payload.get("start_date")),
        "end_date": _ymd(payload.get("end_date")),
        "site_name": str(payload.get("site_name") or "").strip(),
        "zip_code": _digits(payload.get("zip_code")),
        "address": str(payload.get("address") or "").strip(),
        "address_detail": str(payload.get("address_detail") or "").strip(),
        "contract_amount": _to_float(payload.get("contract_amount")),
        "description": str(payload.get("description") or "").strip(),
        "use_yn_code_id": payload.get("use_yn_code_id") or None,
        "note": str(payload.get("note") or "").strip(),
    }
    if not values["project_code"]:
        values["project_code"] = _next_project_code(conn)
    if not values["project_name"]:
        raise ValueError("프로젝트명을 입력해 주세요.")
    if not values["project_type_code_id"]:
        raise ValueError("공사구분을 선택해 주세요.")
    if not values["project_status_code_id"]:
        raise ValueError("프로젝트상태를 선택해 주세요.")

    dup = conn.execute("SELECT project_id FROM projects WHERE project_code = ? AND project_id != ?", (values["project_code"], pk)).fetchone()
    if dup:
        raise ValueError("동일한 프로젝트코드가 이미 존재합니다.")

    if pk:
        conn.execute(
            """
            UPDATE projects
               SET project_code=:project_code, project_name=:project_name, project_type_code_id=:project_type_code_id,
                   project_status_code_id=:project_status_code_id, contract_status_code_id=:contract_status_code_id,
                   client_company_id=:client_company_id, main_contractor_company_id=:main_contractor_company_id,
                   performing_company_id=:performing_company_id, project_manager_user_id=:project_manager_user_id,
                   contract_date=:contract_date, start_date=:start_date, end_date=:end_date, site_name=:site_name,
                   zip_code=:zip_code, address=:address, address_detail=:address_detail, contract_amount=:contract_amount,
                   description=:description, use_yn_code_id=:use_yn_code_id, note=:note, updated_at=:updated_at,
                   updated_by=:updated_by
             WHERE project_id=:project_id
            """,
            {**values, "updated_at": _now_str(), "updated_by": actor, "project_id": pk},
        )
        return pk

    new_id = _next_project_id(conn)
    conn.execute(
        """
        INSERT INTO projects (
            project_id, project_code, project_name, project_type_code_id, project_status_code_id, contract_status_code_id,
            client_company_id, main_contractor_company_id, performing_company_id, project_manager_user_id, contract_date,
            start_date, end_date, site_name, zip_code, address, address_detail, contract_amount, description,
            use_yn_code_id, note, created_at, created_by
        ) VALUES (
            :project_id, :project_code, :project_name, :project_type_code_id, :project_status_code_id, :contract_status_code_id,
            :client_company_id, :main_contractor_company_id, :performing_company_id, :project_manager_user_id, :contract_date,
            :start_date, :end_date, :site_name, :zip_code, :address, :address_detail, :contract_amount, :description,
            :use_yn_code_id, :note, :created_at, :created_by
        )
        """,
        {**values, "project_id": new_id, "created_at": _now_str(), "created_by": actor},
    )
    return new_id


def delete_projects(conn: sqlite3.Connection, ids: List[int]) -> int:
    target_ids = [int(x) for x in ids if str(x).strip()]
    if not target_ids:
        return 0
    marks = ",".join(["?"] * len(target_ids))
    cur = conn.execute(f"DELETE FROM projects WHERE project_id IN ({marks})", target_ids)
    return int(cur.rowcount or 0)
