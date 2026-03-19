# -*- coding: utf-8 -*-
"""
프로그램명 : 테이블 데이터 입력 저장소
파일경로   : repositories/table_data_input_repository.py
기능설명   :
사용테이블 :
작성일시   : 2026-03-16 19:40
작성자     : ChatGPT
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from io import BytesIO
from typing import Any, Dict, List, Tuple

import pandas as pd

from config.schema_meta import SQLITE_DATE_TYPES, SQLITE_NUMERIC_TYPES


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": (row[2] or "TEXT").upper(),
            "notnull": row[3],
            "default_value": row[4],
            "pk": row[5],
        }
        for row in rows
    ]


def build_sample_dataframe(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    columns = get_table_columns(conn, table_name)
    return pd.DataFrame(columns=[col["name"] for col in columns])


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31] or "sample")
    return output.getvalue()


def read_upload_file(uploaded_file) -> pd.DataFrame:
    lower_name = uploaded_file.name.lower()
    if lower_name.endswith(".xlsx"):
        return pd.read_excel(uploaded_file)
    if lower_name.endswith(".csv"):
        try:
            return pd.read_csv(uploaded_file, encoding="utf-8-sig")
        except Exception:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, encoding="cp949")
    raise ValueError("xlsx 또는 csv 파일만 업로드 가능합니다.")


def _to_none(value: Any) -> Any:
    return None if pd.isna(value) or value == "" else value


def _convert_value_by_type(value: Any, data_type: str) -> Any:
    value = _to_none(value)
    if value is None:
        return None

    data_type = (data_type or "TEXT").upper()

    if data_type in SQLITE_NUMERIC_TYPES:
        text = str(value).strip().replace(",", "")
        if text == "":
            return None
        if data_type == "INTEGER":
            return int(float(text))
        return float(text)

    if data_type in SQLITE_DATE_TYPES:
        return str(value).strip()

    return str(value).strip()


def validate_and_reorder_dataframe(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
) -> Tuple[bool, str, pd.DataFrame, List[Dict[str, Any]]]:
    columns = get_table_columns(conn, table_name)
    if not columns:
        return False, "테이블 컬럼 정보를 찾을 수 없습니다.", df, []

    db_column_names = [col["name"] for col in columns]
    file_df = df.copy()
    file_df.columns = [str(col).strip() for col in file_df.columns]

    missing = [col for col in db_column_names if col not in file_df.columns]
    extra = [col for col in file_df.columns if col not in db_column_names]

    if missing:
        return False, f"업로드 파일에 누락된 컬럼이 있습니다: {missing}", file_df, columns
    if extra:
        return False, f"업로드 파일에 정의되지 않은 컬럼이 있습니다: {extra}", file_df, columns

    ordered_df = file_df[db_column_names].copy()
    return True, "컬럼 검증 완료", ordered_df, columns


def convert_dataframe_types(df: pd.DataFrame, columns: List[Dict[str, Any]]) -> pd.DataFrame:
    converted_df = df.copy()
    for col_meta in columns:
        col_name = col_meta["name"]
        data_type = col_meta["type"]
        converted_df[col_name] = converted_df[col_name].apply(lambda v: _convert_value_by_type(v, data_type))
    return converted_df


def insert_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> Tuple[bool, str, int]:
    if df.empty:
        return False, "업로드할 데이터가 없습니다.", 0

    columns = list(df.columns)
    placeholder_sql = ", ".join(["?"] * len(columns))
    column_sql = ", ".join([f'"{col}"' for col in columns])
    insert_sql = f'INSERT INTO "{table_name}" ({column_sql}) VALUES ({placeholder_sql})'

    rows = []
    for _, row in df.iterrows():
        rows.append(tuple(row[col] for col in columns))

    try:
        conn.executemany(insert_sql, rows)
        conn.commit()
        return True, f"{len(rows)}건 INSERT 완료", len(rows)
    except Exception as exc:
        conn.rollback()
        return False, f"데이터 입력 중 오류가 발생했습니다. {exc}", 0
