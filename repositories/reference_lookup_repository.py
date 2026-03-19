# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 참조정보 Repository
파일명     : reference_lookup_repository.py
설명       : dtl_code / companies / users / workers 참조 목록 및 표시명 조회.
사용 테이블 :
    - mst_code
    - dtl_code
    - companies
    - users
    - workers
작성일시   : 2026-03-18
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from repositories.common_schema_repository import get_table_columns, table_exists


def _safe_fetchall(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    try:
        rows = conn.execute(sql, params).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, sqlite3.Row):
                result.append({key: row[key] for key in row.keys()})
            else:
                result.append(dict(row))
        return result
    except Exception:
        return []


def get_code_options_by_mst_candidates(
    conn: sqlite3.Connection,
    mst_candidates: List[str],
) -> List[Dict[str, Any]]:
    if not table_exists(conn, "mst_code") or not table_exists(conn, "dtl_code"):
        return []

    mst_columns = get_table_columns(conn, "mst_code")
    dtl_columns = get_table_columns(conn, "dtl_code")
    if "mst_code_id" not in mst_columns or "mst_code_id" not in dtl_columns:
        return []

    where_clauses: List[str] = []
    params: List[Any] = []

    if "mst_code" in mst_columns:
        where_clauses.extend(["UPPER(COALESCE(m.mst_code,'')) = ?"] * len(mst_candidates))
        params.extend([candidate.upper() for candidate in mst_candidates])

    if "code_name" in mst_columns:
        where_clauses.extend(["UPPER(COALESCE(m.code_name,'')) = ?"] * len(mst_candidates))
        params.extend([candidate.upper() for candidate in mst_candidates])

    if not where_clauses:
        return []

    sql = f"""
        SELECT d.dtl_code_id,
               COALESCE(d.dtl_code, '') AS dtl_code,
               COALESCE(d.code_name, '') AS code_name,
               COALESCE(d.use_yn, 'Y') AS use_yn,
               COALESCE(d.sort_order, 999999) AS sort_order
          FROM dtl_code d
          JOIN mst_code m
            ON d.mst_code_id = m.mst_code_id
         WHERE ({' OR '.join(where_clauses)})
           AND COALESCE(d.use_yn, 'Y') = 'Y'
         ORDER BY COALESCE(d.sort_order, 999999), d.dtl_code_id
    """
    return _safe_fetchall(conn, sql, tuple(params))


def get_company_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not table_exists(conn, "companies"):
        return []
    columns = get_table_columns(conn, "companies")
    if "company_id" not in columns:
        return []

    name_expr = "COALESCE(company_name, '')"
    code_expr = "COALESCE(company_code, '')"
    sql = f"""
        SELECT company_id,
               {code_expr} AS company_code,
               {name_expr} AS company_name
          FROM companies
         ORDER BY company_id
    """
    return _safe_fetchall(conn, sql)


def get_user_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not table_exists(conn, "users"):
        return []
    columns = get_table_columns(conn, "users")
    if "user_id" not in columns:
        return []

    name_column = "user_name" if "user_name" in columns else ("login_id" if "login_id" in columns else "user_id")
    sql = f"""
        SELECT user_id,
               COALESCE({name_column}, '') AS user_name
          FROM users
         ORDER BY user_id
    """
    return _safe_fetchall(conn, sql)


def get_worker_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not table_exists(conn, "workers"):
        return []
    columns = get_table_columns(conn, "workers")
    if "worker_id" not in columns:
        return []

    worker_no_column = "worker_no" if "worker_no" in columns else ("employee_no" if "employee_no" in columns else "worker_id")
    worker_name_column = "worker_name" if "worker_name" in columns else ("name" if "name" in columns else "worker_id")
    sql = f"""
        SELECT worker_id,
               COALESCE({worker_no_column}, '') AS worker_no,
               COALESCE({worker_name_column}, '') AS worker_name
          FROM workers
         ORDER BY worker_id
    """
    return _safe_fetchall(conn, sql)


def get_code_label(options: List[Dict[str, Any]], code_id: Optional[int]) -> str:
    if not code_id:
        return ""
    for item in options:
        if int(item.get("dtl_code_id") or 0) == int(code_id):
            dtl_code = str(item.get("dtl_code") or "")
            code_name = str(item.get("code_name") or "")
            return f"{dtl_code} {code_name}".strip()
    return str(code_id)


def get_company_label(options: List[Dict[str, Any]], company_id: Optional[int]) -> str:
    if not company_id:
        return ""
    for item in options:
        if int(item.get("company_id") or 0) == int(company_id):
            company_code = str(item.get("company_code") or "")
            company_name = str(item.get("company_name") or "")
            return f"{company_code} {company_name}".strip()
    return str(company_id)


def get_user_label(options: List[Dict[str, Any]], user_id: Optional[int]) -> str:
    if not user_id:
        return ""
    for item in options:
        if int(item.get("user_id") or 0) == int(user_id):
            return str(item.get("user_name") or user_id)
    return str(user_id)


def get_worker_label(options: List[Dict[str, Any]], worker_id: Optional[int]) -> str:
    if not worker_id:
        return ""
    for item in options:
        if int(item.get("worker_id") or 0) == int(worker_id):
            return f"{item.get('worker_no', '')} {item.get('worker_name', '')}".strip()
    return str(worker_id)
