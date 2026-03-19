# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 공통 스키마 Repository
파일명     : common_schema_repository.py
설명       : 업로드된 전체 테이블 기준으로 실제 존재 컬럼/테이블을 확인하는 공통 함수 모음.
사용 테이블 :
    - sqlite_master
    - PRAGMA table_info
작성일시   : 2026-03-18
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
          FROM sqlite_master
         WHERE type='table'
           AND name=?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    if not table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: List[str] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            columns.append(str(row["name"]))
        else:
            columns.append(str(row[1]))
    return columns


def require_table(conn: sqlite3.Connection, table_name: str) -> None:
    if not table_exists(conn, table_name):
        raise ValueError(
            f"필수 테이블 [{table_name}] 이(가) 없습니다. "
            "업로드하신 전체 테이블 기준으로 DB를 먼저 맞춘 후 다시 실행해 주세요."
        )


def has_columns(conn: sqlite3.Connection, table_name: str, required_columns: Iterable[str]) -> bool:
    columns = set(get_table_columns(conn, table_name))
    return all(column in columns for column in required_columns)


def row_to_dict(row) -> Dict:
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)
