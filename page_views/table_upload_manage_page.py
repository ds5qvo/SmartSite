# -*- coding: utf-8 -*-
"""
프로그램명 : table_upload_manage_page.py
파일경로   : page_views/table_upload_manage_page.py
기능설명   : SmartSite 테이블 업로드/생성/초기화 관리 화면
사용테이블 : 전체 기준 테이블, sqlite_master
작성일시   : 2026-03-17
작성자     : ChatGPT
변경이력   :
    - 2026-03-17 : 전체 기준 테이블 생성/초기데이터 반영/엑셀 업로드/테이블 삭제 안정화 버전 작성
주의사항   :
    - DB 연결은 app.py 에서 전달받은 conn 만 사용
    - DROP TABLE 은 사용자 선택 시에만 실행
    - 기준 테이블 생성은 CREATE TABLE IF NOT EXISTS 기반
"""

from __future__ import annotations

import io
import sqlite3
from datetime import datetime
from typing import List

import pandas as pd
import streamlit as st

from components.common_list_component import CommonListComponent
from sql.schema_order import SCHEMA_CREATE_ORDER
from sql.schema_sql import SCHEMA_SQL
from sql.seed_sql import SEED_SQL


PAGE_TITLE = "테이블업로드관리"


def render_table_upload_manage_page(conn: sqlite3.Connection) -> None:
    st.title(PAGE_TITLE)
    st.caption("기준 테이블 생성 / 초기 데이터 반영 / 엑셀 업로드 / 선택 테이블 삭제")

    tab1, tab2, tab3, tab4 = st.tabs([
        "기준테이블 생성",
        "초기데이터 반영",
        "엑셀 업로드",
        "테이블 삭제",
    ])

    with tab1:
        _render_create_schema_tab(conn)
    with tab2:
        _render_seed_tab(conn)
    with tab3:
        _render_excel_upload_tab(conn)
    with tab4:
        _render_drop_table_tab(conn)


def _render_create_schema_tab(conn: sqlite3.Connection) -> None:
    st.subheader("기준 테이블 전체 생성")

    schema_rows = []
    for sort_no, table_name in enumerate(SCHEMA_CREATE_ORDER, start=1):
        schema_rows.append(
            {
                "순번": sort_no,
                "table_name": table_name,
                "sql_defined": "Y" if table_name in SCHEMA_SQL else "N",
            }
        )

    df = pd.DataFrame(schema_rows)
    result = CommonListComponent.render(
        key_prefix="schema_create",
        df=df,
        searchable_columns=["table_name"],
        disabled_columns=["순번", "table_name", "sql_defined"],
    )
    CommonListComponent.show_selection_message(result)

    btn_col1, btn_col2 = st.columns([1, 3])
    with btn_col1:
        if st.button("선택 테이블 생성", use_container_width=True):
            table_names = result.selected_rows["table_name"].tolist() if not result.selected_rows.empty else []
            if not table_names:
                st.warning("생성할 테이블을 선택하세요.")
            else:
                messages = create_tables(conn, table_names)
                for msg in messages:
                    st.write(f"- {msg}")
                st.success("선택 테이블 생성이 완료되었습니다.")

    with btn_col2:
        if st.button("전체 기준 테이블 생성", use_container_width=True):
            messages = create_tables(conn, SCHEMA_CREATE_ORDER)
            for msg in messages:
                st.write(f"- {msg}")
            st.success("전체 기준 테이블 생성이 완료되었습니다.")


def _render_seed_tab(conn: sqlite3.Connection) -> None:
    st.subheader("초기 데이터 반영")
    st.write("mst_code / dtl_code / mst_role / mst_menu / mst_deduction_item 등의 기본 데이터를 반영합니다.")

    if st.button("초기 데이터 반영 실행", use_container_width=True):
        messages = apply_seed_data(conn)
        for msg in messages:
            st.write(f"- {msg}")
        st.success("초기 데이터 반영이 완료되었습니다.")


def _render_excel_upload_tab(conn: sqlite3.Connection) -> None:
    st.subheader("엑셀 업로드")
    table_names = get_existing_table_names(conn)
    selected_table = st.selectbox("업로드 대상 테이블", options=[""] + table_names, index=0)
    uploaded_file = st.file_uploader("엑셀 파일 업로드", type=["xlsx", "xls"])

    if selected_table:
        st.info(f"대상 테이블: {selected_table}")
        st.download_button(
            label="샘플 엑셀 다운로드",
            data=build_sample_excel_bytes(conn, selected_table),
            file_name=f"{selected_table}_sample.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    if st.button("엑셀 업로드 반영", use_container_width=True):
        if not selected_table:
            st.warning("업로드 대상 테이블을 선택하세요.")
            return
        if uploaded_file is None:
            st.warning("업로드할 엑셀 파일을 선택하세요.")
            return

        try:
            upload_df = pd.read_excel(uploaded_file)
            if upload_df.empty:
                st.warning("엑셀에 데이터가 없습니다.")
                return

            insert_count = bulk_insert_dataframe(conn, selected_table, upload_df)
            st.success(f"엑셀 업로드가 완료되었습니다. 반영 건수: {insert_count}건")
        except Exception as exc:
            st.error(f"엑셀 업로드 중 오류가 발생했습니다: {exc}")


def _render_drop_table_tab(conn: sqlite3.Connection) -> None:
    st.subheader("선택 테이블 삭제")
    st.warning("주의: 선택한 테이블 자체를 삭제합니다. 기존 DB 구조 유지가 원칙이므로 정말 필요한 경우만 사용하세요.")

    table_names = get_existing_table_names(conn)
    df = pd.DataFrame({"table_name": table_names})
    result = CommonListComponent.render(
        key_prefix="drop_table",
        df=df,
        searchable_columns=["table_name"],
        disabled_columns=["table_name"],
    )
    CommonListComponent.show_selection_message(result)

    confirm_drop = st.checkbox("선택 테이블 삭제를 확인합니다.")

    if st.button("선택 테이블 삭제 실행", use_container_width=True):
        table_list = result.selected_rows["table_name"].tolist() if not result.selected_rows.empty else []
        if not table_list:
            st.warning("삭제할 테이블을 선택하세요.")
            return
        if not confirm_drop:
            st.warning("삭제 확인 체크를 먼저 선택하세요.")
            return

        messages = drop_tables(conn, table_list)
        for msg in messages:
            st.write(f"- {msg}")
        st.success("선택 테이블 삭제가 완료되었습니다.")


def create_tables(conn: sqlite3.Connection, table_names: List[str]) -> List[str]:
    messages: List[str] = []
    cursor = conn.cursor()

    try:
        for table_name in table_names:
            sql = SCHEMA_SQL.get(table_name)
            if not sql:
                messages.append(f"{table_name}: 정의 없음")
                continue
            cursor.execute(sql)
            messages.append(f"{table_name}: 생성/확인 완료")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return messages


def apply_seed_data(conn: sqlite3.Connection) -> List[str]:
    messages: List[str] = []
    cursor = conn.cursor()

    try:
        for idx, sql in enumerate(SEED_SQL, start=1):
            cursor.executescript(sql)
            messages.append(f"초기데이터 SQL {idx} 실행 완료")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return messages


def get_existing_table_names(conn: sqlite3.Connection) -> List[str]:
    query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """
    df = pd.read_sql_query(query, conn)
    return df["name"].tolist() if not df.empty else []


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    query = f"PRAGMA table_info({table_name})"
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return []
    return df["name"].tolist()


def build_sample_excel_bytes(conn: sqlite3.Connection, table_name: str) -> bytes:
    columns = get_table_columns(conn, table_name)
    sample_df = pd.DataFrame(columns=columns)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sample_df.to_excel(writer, sheet_name=table_name[:31], index=False)
    buffer.seek(0)
    return buffer.read()


def bulk_insert_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> int:
    target_columns = get_table_columns(conn, table_name)
    if not target_columns:
        raise ValueError(f"테이블 컬럼 정보를 찾을 수 없습니다: {table_name}")

    upload_columns = [col for col in df.columns if col in target_columns]
    if not upload_columns:
        raise ValueError("업로드 엑셀 컬럼과 대상 테이블 컬럼이 일치하지 않습니다.")

    work_df = df[upload_columns].copy()
    work_df = work_df.where(pd.notnull(work_df), None)

    placeholders = ", ".join(["?"] * len(upload_columns))
    column_sql = ", ".join(upload_columns)
    insert_sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"

    values = [tuple(row[col] for col in upload_columns) for _, row in work_df.iterrows()]

    cursor = conn.cursor()
    try:
        cursor.executemany(insert_sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(values)


def drop_tables(conn: sqlite3.Connection, table_names: List[str]) -> List[str]:
    messages: List[str] = []
    cursor = conn.cursor()

    try:
        for table_name in table_names:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            messages.append(f"{table_name}: 삭제 완료")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return messages