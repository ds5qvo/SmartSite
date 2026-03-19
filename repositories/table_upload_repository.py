# -*- coding: utf-8 -*-
"""
프로그램명 : table_upload_repository.py
파일경로   : repositories/table_upload_repository.py
기능설명   :
    - 임시/관리 테이블 조회, 생성, 삭제, SQL 실행, 업로드용 Repository
    - SQLite 실제 연결 DB 기준으로 테이블 목록 조회
    - FK 참조관계를 반영한 자식 테이블 우선 삭제 처리
    - SQL 붙여넣기 기반 CREATE TABLE / ALTER TABLE / DROP TABLE 실행 지원
    - 생성된 테이블의 컬럼 구조 조회 및 업로드 INSERT 지원
사용테이블 :
    - sqlite_master
    - mst_code
    - dtl_code
    - hist_code
    - workers
    - worker_details
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
주의사항   :
"""

from __future__ import annotations

import os
import re
import sqlite3
from typing import Any, Dict, List, Tuple

import pandas as pd


SYSTEM_TABLES = {
    "sqlite_sequence",
}


def get_db_file_path(conn: sqlite3.Connection) -> str:
    """
    현재 sqlite connection 이 실제로 바라보는 DB 파일 경로 반환
    """
    row = conn.execute("PRAGMA database_list").fetchone()
    if not row:
        return ""
    return row[2] or ""


def get_all_table_names(conn: sqlite3.Connection) -> List[str]:
    """
    현재 DB의 사용자 테이블 목록 조회
    """
    query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """
    rows = conn.execute(query).fetchall()
    return [row[0] for row in rows if row[0] not in SYSTEM_TABLES]


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    query = """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type='table'
          AND name=?
    """
    row = conn.execute(query, (table_name,)).fetchone()
    return bool(row and row[0] > 0)


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    """
    PRAGMA table_info 로 컬럼 정보 조회
    """
    if not table_exists(conn, table_name):
        return []

    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": row[3],
            "default_value": row[4],
            "pk": row[5],
        }
        for row in rows
    ]


def get_table_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    if not table_exists(conn, table_name):
        return 0
    row = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    return int(row[0]) if row else 0


def get_table_preview_df(conn: sqlite3.Connection, table_name: str, limit: int = 100) -> pd.DataFrame:
    if not table_exists(conn, table_name):
        return pd.DataFrame()

    query = f'SELECT * FROM "{table_name}" LIMIT {int(limit)}'
    return pd.read_sql_query(query, conn)


def sanitize_sql_script(sql_text: str) -> str:
    """
    사용자가 붙여넣은 SQL에서 commit/rollback 등 불필요 문장 제거
    """
    sql = sql_text.strip()
    if not sql:
        return ""

    blocked_patterns = [
        r"\bcommit\s*;?",
        r"\brollback\s*;?",
        r"\bbegin\s+transaction\s*;?",
        r"\bend\s+transaction\s*;?",
    ]

    for pattern in blocked_patterns:
        sql = re.sub(pattern, "", sql, flags=re.IGNORECASE)

    return sql.strip()


def execute_sql_script(conn: sqlite3.Connection, sql_text: str) -> Tuple[bool, str]:
    """
    CREATE TABLE / ALTER TABLE / DROP TABLE 등 직접 실행
    """
    sql_script = sanitize_sql_script(sql_text)
    if not sql_script:
        return False, "실행할 SQL 내용이 없습니다."

    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.executescript(sql_script)
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON")
        return True, "SQL이 정상 실행되었습니다."
    except Exception as exc:
        conn.rollback()
        try:
            conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            pass
        return False, f"SQL 실행 중 오류가 발생했습니다. {exc}"


def get_create_table_sql(conn: sqlite3.Connection, table_name: str) -> str:
    query = """
        SELECT sql
        FROM sqlite_master
        WHERE type='table'
          AND name=?
    """
    row = conn.execute(query, (table_name,)).fetchone()
    return row[0] if row and row[0] else ""


def get_dependent_table_map(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    """
    부모테이블 -> [자식테이블 목록]
    예:
        mst_code -> [dtl_code, hist_code]
        dtl_code -> [hist_code]
        workers -> [worker_details]
    """
    all_tables = get_all_table_names(conn)
    dependent_map: Dict[str, List[str]] = {table: [] for table in all_tables}

    for child_table in all_tables:
        fk_rows = conn.execute(f"PRAGMA foreign_key_list('{child_table}')").fetchall()
        for fk in fk_rows:
            parent_table = fk[2]
            if parent_table in dependent_map:
                if child_table not in dependent_map[parent_table]:
                    dependent_map[parent_table].append(child_table)

    return dependent_map


def resolve_drop_order(conn: sqlite3.Connection, target_table: str) -> List[str]:
    """
    자식 → 부모 순으로 DROP 순서 계산
    target_table 이 부모이면 해당 자식들을 먼저 포함
    """
    dependent_map = get_dependent_table_map(conn)
    visited: set[str] = set()
    ordered: List[str] = []

    def visit(table_name: str) -> None:
        if table_name in visited:
            return
        visited.add(table_name)

        child_tables = dependent_map.get(table_name, [])
        for child in child_tables:
            visit(child)

        ordered.append(table_name)

    visit(target_table)

    unique_ordered: List[str] = []
    for item in ordered:
        if item not in unique_ordered:
            unique_ordered.append(item)

    return unique_ordered


def drop_table_with_dependencies(conn: sqlite3.Connection, target_table: str) -> Tuple[bool, str, List[str]]:
    """
    FK 참조관계를 고려하여 자식 테이블부터 DROP
    """
    if not table_exists(conn, target_table):
        return False, f"[{target_table}] 테이블이 존재하지 않습니다.", []

    drop_order = resolve_drop_order(conn, target_table)

    try:
        conn.execute("PRAGMA foreign_keys = OFF")

        executed: List[str] = []
        for table_name in drop_order:
            if table_exists(conn, table_name):
                conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                executed.append(table_name)

        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON")

        db_path = get_db_file_path(conn)
        message = (
            f"테이블 삭제 완료: {', '.join(executed)}\n"
            f"실제 DB 경로: {db_path}"
        )
        return True, message, executed

    except Exception as exc:
        conn.rollback()
        try:
            conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            pass
        return False, f"테이블 삭제 중 오류가 발생했습니다. {exc}", []


def build_empty_sample_dataframe(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """
    업로드 샘플용 빈 DataFrame 생성
    """
    columns = get_table_columns(conn, table_name)
    column_names = [col["name"] for col in columns]
    return pd.DataFrame(columns=column_names)


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    copied = df.copy()
    copied.columns = [str(col).strip() for col in copied.columns]
    return copied


def validate_upload_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> Tuple[bool, str, pd.DataFrame]:
    """
    업로드 파일 컬럼 검증
    - 테이블 컬럼과 파일 컬럼 비교
    - 파일에 없는 컬럼이 있으면 오류
    - 파일에 있는 불필요한 컬럼은 오류
    """
    if not table_exists(conn, table_name):
        return False, f"[{table_name}] 테이블이 존재하지 않습니다.", df

    df = normalize_dataframe_columns(df)

    table_columns = get_table_columns(conn, table_name)
    table_column_names = [col["name"] for col in table_columns]

    file_column_names = list(df.columns)

    missing_columns = [col for col in table_column_names if col not in file_column_names]
    extra_columns = [col for col in file_column_names if col not in table_column_names]

    if missing_columns:
        return False, f"업로드 파일에 누락된 컬럼이 있습니다: {missing_columns}", df

    if extra_columns:
        return False, f"업로드 파일에 테이블에 없는 컬럼이 있습니다: {extra_columns}", df

    ordered_df = df[table_column_names].copy()
    return True, "업로드 컬럼 검증이 완료되었습니다.", ordered_df


def _convert_nan_to_none(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


def insert_dataframe_to_table(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> Tuple[bool, str, int]:
    """
    DataFrame 전체 INSERT
    """
    if df.empty:
        return False, "업로드할 데이터가 없습니다.", 0

    columns = list(df.columns)
    placeholders = ", ".join(["?"] * len(columns))
    column_sql = ", ".join([f'"{col}"' for col in columns])

    insert_sql = f'INSERT INTO "{table_name}" ({column_sql}) VALUES ({placeholders})'

    try:
        rows = []
        for _, row in df.iterrows():
            rows.append(tuple(_convert_nan_to_none(row[col]) for col in columns))

        conn.executemany(insert_sql, rows)
        conn.commit()
        return True, f"{len(rows)}건 업로드 완료", len(rows)

    except Exception as exc:
        conn.rollback()
        return False, f"데이터 업로드 중 오류가 발생했습니다. {exc}", 0