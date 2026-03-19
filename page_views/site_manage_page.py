# -*- coding: utf-8 -*-
"""
프로그램명 : site_manage_page.py
파일경로   : page_views/site_manage_page.py
기능설명   :
주요기능   :
변경이력   :
주의사항   :
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st


def _actor() -> str:
    for key in ["login_user_name", "user_name", "login_id"]:
        if st.session_state.get(key):
            return str(st.session_state.get(key))
    return "system"


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _pick_option(options: List[Dict[str, Any]], current_value: Any) -> int:
    if not options:
        return 0
    for idx, row in enumerate(options):
        if str(row.get("value")) == str(current_value):
            return idx
    return 0


def _option_label(options: List[Dict[str, Any]], value: Any) -> str:
    for row in options:
        if str(row.get("value")) == str(value):
            return str(row.get("label", ""))
    return ""


def _render_multiselect_message(prefix: str) -> None:
    st.info("입력 화면에 데이타가 다중 선택되었습니다.")


from services.site_service import (
    dataframe_to_excel_bytes,
    get_site_detail,
    get_site_list,
    save_site_data,
)


def _init_state() -> None:
    defaults = {
        "site_search_text": "",
        "site_selected_ids": [],
        "site_form": {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    if not st.session_state["site_form"]:
        _reset_form(clear_selection=False)


def _reset_form(clear_selection: bool = True) -> None:
    st.session_state["site_form"] = {
        "project_id": 0,
        "project_code": "",
        "project_name": "",
        "site_name": "",
        "zip_code": "",
        "address": "",
        "address_detail": "",
        "start_date": "",
        "end_date": "",
        "note": "",
    }
    if clear_selection:
        st.session_state["site_selected_ids"] = []


def _fill_form(row: Dict[str, Any]) -> None:
    st.session_state["site_form"] = {
        "project_id": int(row.get("project_id") or 0),
        "project_code": str(row.get("project_code") or ""),
        "project_name": str(row.get("project_name") or ""),
        "site_name": str(row.get("site_name") or ""),
        "zip_code": str(row.get("zip_code") or ""),
        "address": str(row.get("address") or ""),
        "address_detail": str(row.get("address_detail") or ""),
        "start_date": str(row.get("start_date") or ""),
        "end_date": str(row.get("end_date") or ""),
        "note": str(row.get("note") or ""),
    }


def render_site_manage_page(conn) -> None:
    _init_state()
    st.title("현장관리")
    st.caption("현장관리는 projects 테이블의 현장정보 기준으로 처리합니다.")

    left_col, right_col = st.columns([4, 6])

    with right_col:
        st.markdown("### 현장 목록")
        st.session_state["site_search_text"] = st.text_input("검색", value=st.session_state["site_search_text"], key="site_search_text_input")
        df = get_site_list(conn, st.session_state["site_search_text"])
        selected_ids = st.session_state.get("site_selected_ids", [])

        grid_df = df.copy() if not df.empty else pd.DataFrame(columns=["project_id"])
        if not grid_df.empty:
            grid_df.insert(0, "선택", grid_df["project_id"].isin(selected_ids))
        else:
            grid_df = pd.DataFrame(columns=["선택", "project_id"])

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("조회", width="stretch", key="site_btn_search"):
                st.rerun()
        with c2:
            if st.button("전체선택", width="stretch", key="site_btn_all"):
                st.session_state["site_selected_ids"] = [] if df.empty else df["project_id"].astype(int).tolist()
                st.rerun()
        with c3:
            if st.button("선택해제", width="stretch", key="site_btn_clear"):
                st.session_state["site_selected_ids"] = []
                _reset_form(clear_selection=False)
                st.rerun()

        previous_selected = list(st.session_state.get("site_selected_ids", []))
        edited_df = st.data_editor(grid_df, width="stretch", hide_index=True, disabled=[c for c in grid_df.columns if c != "선택"], key="site_grid_editor")
        if not edited_df.empty and "선택" in edited_df.columns:
            new_selected = edited_df.loc[edited_df["선택"] == True, "project_id"].astype(int).tolist()
        else:
            new_selected = []

        if new_selected != previous_selected:
            st.session_state["site_selected_ids"] = new_selected
            if len(new_selected) == 1:
                detail = get_site_detail(conn, new_selected[0])
                if detail:
                    _fill_form(detail)
            else:
                _reset_form(clear_selection=False)
            st.rerun()

        current_selected = list(st.session_state.get("site_selected_ids", []))
        if len(current_selected) == 1:
            current_form = st.session_state.get("site_form", {})
            if int(current_form.get("project_id") or 0) != int(current_selected[0]):
                detail = get_site_detail(conn, current_selected[0])
                if detail:
                    _fill_form(detail)
        elif len(current_selected) > 1:
            _reset_form(clear_selection=False)
            _render_multiselect_message("site")

        st.download_button(
            "선택자료 엑셀 다운로드",
            data=dataframe_to_excel_bytes(df[df["project_id"].isin(current_selected)] if (not df.empty and current_selected) else df, "현장관리"),
            file_name=f"현장관리_{_ts()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
            key="site_download_excel",
        )

    form = st.session_state["site_form"]
    with left_col:
        st.markdown("### 현장 입력")
        b1, b2 = st.columns(2)
        with b1:
            if st.button("선택해제", width="stretch", key="site_btn_new"):
                _reset_form()
                st.rerun()
        with b2:
            if st.button("저장", width="stretch", key="site_btn_save"):
                saved_id = save_site_data(conn, st.session_state["site_form"], _actor())
                conn.commit()
                st.session_state["site_selected_ids"] = [saved_id]
                st.success("저장되었습니다.")
                st.rerun()

        st.text_input("프로젝트ID", value=str(form["project_id"] or ""), disabled=True, key="site_form_project_id_view")
        st.text_input("프로젝트코드", value=form["project_code"], disabled=True, key="site_form_project_code_view")
        st.text_input("프로젝트명", value=form["project_name"], disabled=True, key="site_form_project_name_view")
        st.session_state["site_form"]["site_name"] = st.text_input("현장명", value=form["site_name"], key="site_form_site_name_input")
        c1, c2 = st.columns([1, 2])
        with c1:
            st.session_state["site_form"]["zip_code"] = st.text_input("우편번호", value=form["zip_code"], key="site_form_zip_code_input")
        with c2:
            st.session_state["site_form"]["start_date"] = st.text_input("시작일자", value=form["start_date"], placeholder="YYYY-MM-DD", key="site_form_start_date_input")
        st.session_state["site_form"]["address"] = st.text_input("주소", value=form["address"], key="site_form_address_input")
        st.session_state["site_form"]["address_detail"] = st.text_input("상세주소", value=form["address_detail"], key="site_form_address_detail_input")
        st.session_state["site_form"]["end_date"] = st.text_input("종료일자", value=form["end_date"], placeholder="YYYY-MM-DD", key="site_form_end_date_input")
        st.session_state["site_form"]["note"] = st.text_area("비고", value=form["note"], height=120, key="site_form_note_input")


def run_site_manage_page(conn) -> None:
    render_site_manage_page(conn)
