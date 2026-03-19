# -*- coding: utf-8 -*-
"""
프로그램명 : 사용자 관리 화면
파일경로   : page_views/user_manage_page.py
기능설명   :
사용테이블 :
작성일시   : 2026-03-16 20:20
작성자     : ChatGPT
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict

import streamlit as st

from services.generic_manage_service import (
    delete_manage_row,
    get_manage_columns,
    get_manage_df,
    get_pk_column,
    get_row,
    save_manage_row,
)


def _build_input_value(value: Any) -> str:
    return "" if value is None else str(value)


def _render_manage_block(conn: sqlite3.Connection, section_title: str, table_name: str, key_prefix: str) -> None:
    st.markdown(f"### {section_title}")
    columns = get_manage_columns(conn, table_name)
    pk_col = get_pk_column(conn, table_name)
    left_col, right_col = st.columns([4, 6])

    with right_col:
        keyword = st.text_input("검색", key=f"{key_prefix}_keyword")
        df = get_manage_df(conn, table_name, keyword)
        if not df.empty and pk_col in df.columns:
            display_df = df.copy()
            display_df.insert(0, "선택", False)
            edited_df = st.data_editor(
                display_df,
                width="stretch",
                height=420,
                hide_index=True,
                key=f"{key_prefix}_editor",
                column_config={"선택": st.column_config.CheckboxColumn(default=False)},
                disabled=[col for col in display_df.columns if col != "선택"],
            )
            selected_rows = edited_df[edited_df["선택"] == True]
            if len(selected_rows) == 1:
                selected_pk = selected_rows.iloc[0][pk_col]
                selected_row = get_row(conn, table_name, selected_pk) or {}
                st.session_state[f"{key_prefix}_selected_row"] = selected_row
            elif len(selected_rows) >= 2:
                st.info("입력 화면에 데이타가 다중 선택되었습니다.")
                st.session_state[f"{key_prefix}_selected_row"] = {}
        else:
            st.warning("조회 결과가 없습니다.")

    with left_col:
        selected_row = st.session_state.get(f"{key_prefix}_selected_row", {}) or {}
        form_values: Dict[str, Any] = {}
        for col in columns:
            name = col["name"]
            is_pk = int(col.get("pk", 0)) == 1
            current_value = selected_row.get(name)
            form_values[name] = st.text_input(
                name,
                value=_build_input_value(current_value),
                key=f"{key_prefix}_{name}",
                disabled=bool(is_pk and current_value not in (None, "")),
            )

        save_col, new_col, del_col = st.columns(3)
        with save_col:
            if st.button("저장", width="stretch", key=f"{key_prefix}_save"):
                success, message = save_manage_row(conn, table_name, form_values)
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
        with new_col:
            if st.button("신규", width="stretch", key=f"{key_prefix}_new"):
                st.session_state[f"{key_prefix}_selected_row"] = {}
                st.rerun()
        with del_col:
            if st.button("삭제", width="stretch", key=f"{key_prefix}_delete"):
                delete_value = selected_row.get(pk_col) if pk_col else None
                if delete_value in (None, ""):
                    st.error("삭제할 데이터를 1건 선택하세요.")
                else:
                    success, message = delete_manage_row(conn, table_name, delete_value)
                    if success:
                        st.success(message)
                        st.session_state[f"{key_prefix}_selected_row"] = {}
                        st.rerun()
                    else:
                        st.error(message)

from services.user_manage_service import get_user_main_table

def render_user_manage_page(conn: sqlite3.Connection) -> None:
    st.subheader("사용자관리")
    table_name = get_user_main_table(conn)
    if not table_name:
        st.warning("mst_user 또는 users 테이블이 없습니다.")
        return
    _render_manage_block(conn, f"사용자 기본정보 관리 ({table_name})", table_name, "user_main")


def render_page(conn: sqlite3.Connection) -> None:
    render_user_manage_page(conn)

