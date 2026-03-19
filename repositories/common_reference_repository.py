# -*- coding: utf-8 -*-
"""
프로그램명 : common_reference_repository.py
파일경로   : repositories/common_reference_repository.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List


def _dict_rows(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
          FROM sqlite_master
         WHERE type = 'table'
           AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    if not table_exists(conn, table_name):
        return []
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]


def _use_sql(alias: str = "d") -> str:
    return f"COALESCE({alias}.use_yn, 'Y') IN ('Y', 'y', '사용', '1', 1)"


def get_code_options_by_mst_code(conn: sqlite3.Connection, mst_code: str, fallback_label: str = "") -> List[Dict[str, Any]]:
    if not table_exists(conn, "mst_code") or not table_exists(conn, "dtl_code"):
        return []
    rows = _dict_rows(
        conn.execute(
            f"""
            SELECT d.dtl_code_id AS value,
                   d.code_name AS label,
                   d.dtl_code AS code,
                   COALESCE(d.sort_order, 999999) AS sort_order
              FROM dtl_code d
              JOIN mst_code m
                ON d.mst_code_id = m.mst_code_id
             WHERE m.mst_code = ?
               AND {_use_sql('d')}
             ORDER BY COALESCE(d.sort_order, 999999), d.dtl_code_id
            """,
            (mst_code,),
        )
    )
    if rows:
        return rows
    if fallback_label:
        like_value = f"%{fallback_label}%"
        return _dict_rows(
            conn.execute(
                f"""
                SELECT d.dtl_code_id AS value,
                       d.code_name AS label,
                       d.dtl_code AS code,
                       COALESCE(d.sort_order, 999999) AS sort_order
                  FROM dtl_code d
                  JOIN mst_code m
                    ON d.mst_code_id = m.mst_code_id
                 WHERE {_use_sql('d')}
                   AND (UPPER(m.mst_code) LIKE UPPER(?) OR UPPER(m.code_name) LIKE UPPER(?))
                 ORDER BY COALESCE(d.sort_order, 999999), d.dtl_code_id
                """,
                (like_value, like_value),
            )
        )
    return []


def get_code_options_by_mst_code_id(conn: sqlite3.Connection, mst_code_id: Any, fallback_label: str = "") -> List[Dict[str, Any]]:
    if not table_exists(conn, "dtl_code"):
        return []
    rows = _dict_rows(
        conn.execute(
            f"""
            SELECT d.dtl_code_id AS value,
                   d.code_name AS label,
                   d.dtl_code AS code,
                   COALESCE(d.sort_order, 999999) AS sort_order
              FROM dtl_code d
             WHERE d.mst_code_id = ?
               AND {_use_sql('d')}
             ORDER BY COALESCE(d.sort_order, 999999), d.dtl_code_id
            """,
            (mst_code_id,),
        )
    )
    if rows:
        return rows
    if fallback_label:
        return get_code_options_by_mst_code(conn, fallback_label, fallback_label)
    return []


def get_use_yn_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = get_code_options_by_mst_code_id(conn, 100065, "사용여부")
    if rows:
        return rows
    rows = get_code_options_by_mst_code(conn, "USE_YN", "사용여부")
    if rows:
        return rows
    return [
        {"value": 1, "label": "사용", "code": "Y", "sort_order": 1},
        {"value": 2, "label": "미사용", "code": "N", "sort_order": 2},
    ]


def get_company_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not table_exists(conn, "companies"):
        return []
    return _dict_rows(
        conn.execute(
            """
            SELECT company_id AS value,
                   CASE
                       WHEN COALESCE(company_code, '') = '' THEN COALESCE(company_name, '')
                       ELSE COALESCE(company_name, '') || ' [' || COALESCE(company_code, '') || ']'
                   END AS label
              FROM companies
             ORDER BY company_name, company_id
            """
        )
    )


def get_worker_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not table_exists(conn, "workers"):
        return []
    columns = set(get_table_columns(conn, "workers"))
    name_col = "worker_name" if "worker_name" in columns else None
    code_col = "worker_code" if "worker_code" in columns else None
    if not name_col:
        return _dict_rows(conn.execute("SELECT worker_id AS value, CAST(worker_id AS TEXT) AS label FROM workers ORDER BY worker_id"))
    if code_col:
        return _dict_rows(
            conn.execute(
                f"""
                SELECT worker_id AS value,
                       CASE
                           WHEN COALESCE({code_col}, '') = '' THEN COALESCE({name_col}, '')
                           ELSE COALESCE({name_col}, '') || ' [' || COALESCE({code_col}, '') || ']'
                       END AS label
                  FROM workers
                 ORDER BY {name_col}, worker_id
                """
            )
        )
    return _dict_rows(conn.execute(f"SELECT worker_id AS value, COALESCE({name_col}, '') AS label FROM workers ORDER BY {name_col}, worker_id"))


def get_user_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if not table_exists(conn, "users"):
        return []
    columns = set(get_table_columns(conn, "users"))
    if "user_name" in columns:
        return _dict_rows(conn.execute("SELECT user_id AS value, COALESCE(user_name, '') AS label FROM users ORDER BY user_name, user_id"))
    return _dict_rows(conn.execute("SELECT user_id AS value, CAST(user_id AS TEXT) AS label FROM users ORDER BY user_id"))


def get_insurance_type_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if table_exists(conn, "insurance_types"):
        cols = set(get_table_columns(conn, "insurance_types"))
        id_col = "insurance_type_id" if "insurance_type_id" in cols else "deduction_item_id" if "deduction_item_id" in cols else None
        name_col = "item_name" if "item_name" in cols else "insurance_type_name" if "insurance_type_name" in cols else None
        use_col = "use_yn" if "use_yn" in cols else None
        if id_col and name_col:
            sql = f"SELECT {id_col} AS value, {name_col} AS label, {id_col} AS sort_order FROM insurance_types"
            if use_col:
                sql += f" WHERE COALESCE({use_col}, 'Y') IN ('Y', 'y', '사용', '1', 1)"
            sql += f" ORDER BY {id_col}"
            return _dict_rows(conn.execute(sql))
    if table_exists(conn, "payroll_deduction_items"):
        cols = set(get_table_columns(conn, "payroll_deduction_items"))
        if "deduction_item_id" in cols and "item_name" in cols:
            sql = "SELECT deduction_item_id AS value, item_name AS label, deduction_item_id AS sort_order FROM payroll_deduction_items"
            if "use_yn" in cols:
                sql += " WHERE COALESCE(use_yn, 'Y') IN ('Y', 'y', '사용', '1', 1)"
            sql += " ORDER BY deduction_item_id"
            return _dict_rows(conn.execute(sql))
    rows = get_code_options_by_mst_code_id(conn, 100015, "보험종류")
    if rows:
        return rows
    return get_code_options_by_mst_code(conn, "INSURANCE_TYPE", "보험종류")


def label_by_value(options: List[Dict[str, Any]], value: Any) -> str:
    for row in options:
        if str(row.get("value")) == str(value):
            return str(row.get("label", ""))
    return ""


def option_index(options: List[Dict[str, Any]], current_value: Any) -> int:
    if not options:
        return 0
    for idx, row in enumerate(options):
        if str(row.get("value")) == str(current_value):
            return idx
    return 0
