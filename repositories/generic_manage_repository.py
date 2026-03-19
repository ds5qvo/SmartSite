# -*- coding: utf-8 -*-
"""
프로그램명 : 공통 관리 저장소
파일경로   : repositories/generic_manage_repository.py
기능설명   :
사용테이블 :
작성일시   : 2026-03-16 20:20
작성자     : ChatGPT
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd


def get_existing_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [row[0] for row in rows]


def resolve_table_name(conn: sqlite3.Connection, candidate_tables: List[str]) -> Optional[str]:
    existing = set(get_existing_tables(conn))
    for table_name in candidate_tables:
        if table_name in existing:
            return table_name
    return None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    rows = conn.execute(f"PRAGMA table_info('{{table_name}}')").fetchall()
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2] or "TEXT",
            "notnull": row[3],
            "default_value": row[4],
            "pk": row[5],
        }
        for row in rows
    ]


def get_primary_key_column(conn: sqlite3.Connection, table_name: str) -> Optional[str]:
    for col in get_table_columns(conn, table_name):
        if int(col.get("pk", 0)) == 1:
            return col["name"]
    return None


def search_table_df(conn: sqlite3.Connection, table_name: str, keyword: str = "", limit: int = 200) -> pd.DataFrame:
    columns = get_table_columns(conn, table_name)
    col_names = [col["name"] for col in columns]
    if not col_names:
        return pd.DataFrame()
    order_col = get_primary_key_column(conn, table_name) or col_names[0]
    base_sql = f'SELECT * FROM "{{table_name}}"'
    params: List[Any] = []
    if keyword.strip():
        like_parts = [f'CAST("{{col}}" AS TEXT) LIKE ?' for col in col_names]
        base_sql += " WHERE " + " OR ".join(like_parts)
        params.extend([f"%{{keyword.strip()}}%"] * len(col_names))
    base_sql += f' ORDER BY "{{order_col}}" DESC LIMIT {{int(limit)}}'
    return pd.read_sql_query(base_sql, conn, params=params)


def get_row_by_pk(conn: sqlite3.Connection, table_name: str, pk_value: Any) -> Optional[Dict[str, Any]]:
    pk_col = get_primary_key_column(conn, table_name)
    if not pk_col:
        return None
    row = conn.execute(f'SELECT * FROM "{{table_name}}" WHERE "{{pk_col}}" = ?', (pk_value,)).fetchone()
    return dict(row) if row else None


def delete_row_by_pk(conn: sqlite3.Connection, table_name: str, pk_value: Any) -> tuple[bool, str]:
    pk_col = get_primary_key_column(conn, table_name)
    if not pk_col:
        return False, "PK 컬럼을 찾을 수 없습니다."
    try:
        conn.execute(f'DELETE FROM "{{table_name}}" WHERE "{{pk_col}}" = ?', (pk_value,))
        conn.commit()
        return True, "삭제되었습니다."
    except Exception as exc:
        conn.rollback()
        return False, f"삭제 중 오류가 발생했습니다. {{exc}}"


def save_row(conn: sqlite3.Connection, table_name: str, row_data: Dict[str, Any]) -> tuple[bool, str]:
    columns = get_table_columns(conn, table_name)
    pk_col = get_primary_key_column(conn, table_name)
    col_names = [col["name"] for col in columns]
    filtered = {{k: v for k, v in row_data.items() if k in col_names}}
    if not filtered:
        return False, "저장할 데이터가 없습니다."
    pk_value = filtered.get(pk_col) if pk_col else None
    is_update = pk_col and pk_value not in (None, "")
    try:
        if is_update:
            update_cols = [col for col in filtered.keys() if col != pk_col]
            set_sql = ", ".join([f'"{{col}}" = ?' for col in update_cols])
            if not set_sql:
                return False, "수정할 항목이 없습니다."
            params = [filtered[col] for col in update_cols] + [pk_value]
            conn.execute(f'UPDATE "{{table_name}}" SET {{set_sql}} WHERE "{{pk_col}}" = ?', params)
        else:
            insert_cols = list(filtered.keys())
            if pk_col and pk_col in insert_cols and filtered.get(pk_col) in (None, ""):
                insert_cols.remove(pk_col)
            placeholders = ", ".join(["?"] * len(insert_cols))
            col_sql = ", ".join([f'"{{col}}"' for col in insert_cols])
            params = [filtered[col] for col in insert_cols]
            conn.execute(f'INSERT INTO "{{table_name}}" ({{col_sql}}) VALUES ({{placeholders}})', params)
        conn.commit()
        return True, "저장되었습니다."
    except Exception as exc:
        conn.rollback()
        return False, f"저장 중 오류가 발생했습니다. {{exc}}"
