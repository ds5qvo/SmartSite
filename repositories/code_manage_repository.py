# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 코드관리 저장소
파일경로 : repositories/code_manage_repository.py
기능설명 :
    - mst_code / dtl_code 조회, 저장, 삭제
    - 실제 DB 컬럼명(code_name) 기준으로 처리
    - 구버전 컬럼명(mst_code_name / dtl_code_name)도 함께 호환
작성일시 : 2026-03-18 18:40
주요기능   :
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple


RowDict = Dict[str, Any]


def _rows_to_dict_list(cursor: sqlite3.Cursor, rows: List[sqlite3.Row]) -> List[RowDict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def _resolve_master_name_column(conn: sqlite3.Connection) -> str:
    columns = _get_table_columns(conn, "mst_code")
    if "code_name" in columns:
        return "code_name"
    if "mst_code_name" in columns:
        return "mst_code_name"
    return "code_name"


def _resolve_detail_name_column(conn: sqlite3.Connection) -> str:
    columns = _get_table_columns(conn, "dtl_code")
    if "code_name" in columns:
        return "code_name"
    if "dtl_code_name" in columns:
        return "dtl_code_name"
    return "code_name"


def _has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    return column_name in _get_table_columns(conn, table_name)


def select_mst_code_list(conn: sqlite3.Connection, search_text: str = "", use_yn: str = "") -> List[RowDict]:
    return select_master_code_list(conn, {"mst_code": search_text, "code_name": search_text, "use_yn": use_yn})


def select_master_code_list(conn: sqlite3.Connection, search_params: Optional[Dict[str, Any]] = None) -> List[RowDict]:
    search_params = search_params or {}
    master_name_col = _resolve_master_name_column(conn)
    cursor = conn.cursor()

    sql = f"""
    SELECT
        mst_code_id,
        mst_code,
        {master_name_col} AS code_name,
        {master_name_col} AS mst_code_name,
        COALESCE(sort_order, 0) AS sort_order,
        COALESCE(use_yn, 'Y') AS use_yn,
        COALESCE(ip_address, '') AS ip_address,
        COALESCE(mac_address, '') AS mac_address,
        COALESCE(created_by, '') AS created_by,
        COALESCE(created_at, '') AS created_at,
        COALESCE(updated_by, '') AS updated_by,
        COALESCE(updated_at, '') AS updated_at
    FROM mst_code
    WHERE 1 = 1
    """
    params: List[Any] = []

    mst_code_id = search_params.get("mst_code_id")
    mst_code = (search_params.get("mst_code") or "").strip()
    code_name = (search_params.get("code_name") or search_params.get("mst_code_name") or "").strip()
    use_yn = (search_params.get("use_yn") or "").strip()

    if mst_code_id not in (None, ""):
        sql += " AND mst_code_id = ? "
        params.append(int(mst_code_id))

    if mst_code:
        sql += " AND mst_code LIKE ? "
        params.append(f"%{mst_code}%")

    if code_name:
        sql += f" AND {master_name_col} LIKE ? "
        params.append(f"%{code_name}%")

    if use_yn:
        sql += " AND use_yn = ? "
        params.append(use_yn)

    sql += " ORDER BY COALESCE(sort_order, 0), mst_code, mst_code_id "
    cursor.execute(sql, params)
    return _rows_to_dict_list(cursor, cursor.fetchall())


def select_dtl_code_list(conn: sqlite3.Connection, mst_code_id: Optional[int] = None) -> List[RowDict]:
    return select_detail_code_list(conn, {"mst_code_id": mst_code_id or ""})


def select_detail_code_list(conn: sqlite3.Connection, search_params: Optional[Dict[str, Any]] = None) -> List[RowDict]:
    search_params = search_params or {}
    master_name_col = _resolve_master_name_column(conn)
    detail_name_col = _resolve_detail_name_column(conn)
    cursor = conn.cursor()

    sql = f"""
    SELECT
        d.dtl_code_id,
        d.mst_code_id,
        m.mst_code,
        m.{master_name_col} AS mst_code_name,
        m.{master_name_col} AS parent_code_name,
        d.dtl_code,
        d.{detail_name_col} AS code_name,
        d.{detail_name_col} AS dtl_code_name,
        COALESCE(d.sort_order, 0) AS sort_order,
        COALESCE(d.use_yn, 'Y') AS use_yn,
        COALESCE(d.ip_address, '') AS ip_address,
        COALESCE(d.mac_address, '') AS mac_address,
        COALESCE(d.created_by, '') AS created_by,
        COALESCE(d.created_at, '') AS created_at,
        COALESCE(d.updated_by, '') AS updated_by,
        COALESCE(d.updated_at, '') AS updated_at
    FROM dtl_code d
    JOIN mst_code m
      ON d.mst_code_id = m.mst_code_id
    WHERE 1 = 1
    """
    params: List[Any] = []

    mst_code_id = search_params.get("mst_code_id")
    mst_code = (search_params.get("mst_code") or "").strip()
    mst_code_name = (search_params.get("mst_code_name") or "").strip()
    dtl_code = (search_params.get("dtl_code") or "").strip()
    code_name = (search_params.get("code_name") or search_params.get("dtl_code_name") or "").strip()
    use_yn = (search_params.get("use_yn") or "").strip()

    if mst_code_id not in (None, ""):
        sql += " AND d.mst_code_id = ? "
        params.append(int(mst_code_id))

    if mst_code:
        sql += " AND m.mst_code LIKE ? "
        params.append(f"%{mst_code}%")

    if mst_code_name:
        sql += f" AND m.{master_name_col} LIKE ? "
        params.append(f"%{mst_code_name}%")

    if dtl_code:
        sql += " AND d.dtl_code LIKE ? "
        params.append(f"%{dtl_code}%")

    if code_name:
        sql += f" AND d.{detail_name_col} LIKE ? "
        params.append(f"%{code_name}%")

    if use_yn:
        sql += " AND d.use_yn = ? "
        params.append(use_yn)

    sql += " ORDER BY COALESCE(d.sort_order, 0), d.dtl_code, d.dtl_code_id "
    cursor.execute(sql, params)
    return _rows_to_dict_list(cursor, cursor.fetchall())


def insert_mst_code(conn: sqlite3.Connection, mst_code: str, mst_code_name: str, sort_order: int, use_yn: str, remark: str) -> int:
    name_col = _resolve_master_name_column(conn)
    columns = ["mst_code", name_col, "sort_order", "use_yn"]
    values = [mst_code, mst_code_name, sort_order, use_yn]

    if _has_column(conn, "mst_code", "remark"):
        columns.append("remark")
        values.append(remark)
    if _has_column(conn, "mst_code", "created_at"):
        columns.append("created_at")
        values.append("CURRENT_TIMESTAMP")
    if _has_column(conn, "mst_code", "updated_at"):
        columns.append("updated_at")
        values.append("CURRENT_TIMESTAMP")

    sql_columns = ", ".join(columns)
    placeholders = ", ".join(["?" if v != "CURRENT_TIMESTAMP" else "CURRENT_TIMESTAMP" for v in values])
    execute_values = [v for v in values if v != "CURRENT_TIMESTAMP"]

    cursor = conn.cursor()
    cursor.execute(
        f"INSERT INTO mst_code ({sql_columns}) VALUES ({placeholders})",
        execute_values,
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_mst_code(conn: sqlite3.Connection, mst_code_id: int, mst_code: str, mst_code_name: str, sort_order: int, use_yn: str, remark: str) -> None:
    name_col = _resolve_master_name_column(conn)
    set_clauses = [
        "mst_code = ?",
        f"{name_col} = ?",
        "sort_order = ?",
        "use_yn = ?",
    ]
    params: List[Any] = [mst_code, mst_code_name, sort_order, use_yn]

    if _has_column(conn, "mst_code", "remark"):
        set_clauses.append("remark = ?")
        params.append(remark)
    if _has_column(conn, "mst_code", "updated_at"):
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

    params.append(mst_code_id)
    conn.execute(
        f"UPDATE mst_code SET {', '.join(set_clauses)} WHERE mst_code_id = ?",
        params,
    )
    conn.commit()


def insert_dtl_code(conn: sqlite3.Connection, mst_code_id: int, dtl_code: str, dtl_code_name: str, sort_order: int, use_yn: str) -> int:
    name_col = _resolve_detail_name_column(conn)
    columns = ["mst_code_id", "dtl_code", name_col, "sort_order", "use_yn"]
    values = [mst_code_id, dtl_code, dtl_code_name, sort_order, use_yn]

    if _has_column(conn, "dtl_code", "created_at"):
        columns.append("created_at")
        values.append("CURRENT_TIMESTAMP")
    if _has_column(conn, "dtl_code", "updated_at"):
        columns.append("updated_at")
        values.append("CURRENT_TIMESTAMP")

    sql_columns = ", ".join(columns)
    placeholders = ", ".join(["?" if v != "CURRENT_TIMESTAMP" else "CURRENT_TIMESTAMP" for v in values])
    execute_values = [v for v in values if v != "CURRENT_TIMESTAMP"]

    cursor = conn.cursor()
    cursor.execute(
        f"INSERT INTO dtl_code ({sql_columns}) VALUES ({placeholders})",
        execute_values,
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_dtl_code(conn: sqlite3.Connection, dtl_code_id: int, mst_code_id: int, dtl_code: str, dtl_code_name: str, sort_order: int, use_yn: str) -> None:
    name_col = _resolve_detail_name_column(conn)
    set_clauses = [
        "mst_code_id = ?",
        "dtl_code = ?",
        f"{name_col} = ?",
        "sort_order = ?",
        "use_yn = ?",
    ]
    params: List[Any] = [mst_code_id, dtl_code, dtl_code_name, sort_order, use_yn]

    if _has_column(conn, "dtl_code", "updated_at"):
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

    params.append(dtl_code_id)
    conn.execute(
        f"UPDATE dtl_code SET {', '.join(set_clauses)} WHERE dtl_code_id = ?",
        params,
    )
    conn.commit()


def get_mst_code_by_id(conn: sqlite3.Connection, mst_code_id: int) -> Optional[RowDict]:
    rows = select_master_code_list(conn, {"mst_code_id": mst_code_id})
    return rows[0] if rows else None


def get_dtl_code_by_id(conn: sqlite3.Connection, dtl_code_id: int) -> Optional[RowDict]:
    rows = select_detail_code_list(conn, {})
    for row in rows:
        if int(row.get("dtl_code_id", 0)) == int(dtl_code_id):
            return row
    return None


def delete_mst_code_by_id(conn: sqlite3.Connection, mst_code_id: int) -> Tuple[bool, str]:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dtl_code WHERE mst_code_id = ?", (mst_code_id,))
    detail_count = cursor.fetchone()[0]
    if detail_count > 0:
        return False, "하위코드가 존재하여 상위코드를 삭제할 수 없습니다."

    cursor.execute("DELETE FROM mst_code WHERE mst_code_id = ?", (mst_code_id,))
    conn.commit()
    return True, "상위코드가 삭제되었습니다."


def delete_dtl_code_by_id(conn: sqlite3.Connection, dtl_code_id: int) -> Tuple[bool, str]:
    conn.execute("DELETE FROM dtl_code WHERE dtl_code_id = ?", (dtl_code_id,))
    conn.commit()
    return True, "하위코드가 삭제되었습니다."
