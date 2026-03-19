# -*- coding: utf-8 -*-
"""
프로그램명 : insurance_manage_page.py
파일경로   : page_views/insurance_manage_page.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from page_views._page_common import _actor, _pick_option, _ts
from services.insurance_service import (
    dataframe_to_excel_bytes,
    get_insurance_detail,
    get_insurance_form_options,
    get_insurance_list,
    remove_insurance_rows,
    save_insurance_data,
)

DISPLAY_COLUMNS = {
    "worker_insurance_id": "보험ID",
    "작업자": "작업자",
    "보험종류": "보험종류",
    "보험가입상태": "보험가입상태",
    "join_date": "가입일자",
    "leave_date": "탈퇴일자",
    "회사부담률": "회사부담률",
    "근로자부담률": "근로자부담률",
    "note": "비고",
}
DEFAULT_LIST_COLUMNS = ["보험ID", "작업자", "보험종류", "보험가입상태", "가입일자", "탈퇴일자", "회사부담률", "근로자부담률"]


def _digits(value) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def _fmt_date(value) -> str:
    raw = _digits(value)
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return str(value or "").strip()


def _to_float(value) -> float:
    try:
        return float(str(value).replace(",", "") or 0)
    except Exception:
        return 0.0


def _fmt_rate(value) -> str:
    try:
        return f"{_to_float(value):,.2f}"
    except Exception:
        return "0.00"


def _default_form(conn) -> dict:
    opts = get_insurance_form_options(conn)
    workers = opts.get("worker", [])
    types = opts.get("insurance_type", [])
    status = opts.get("insurance_status", [])
    return {
        "worker_insurance_id": 0,
        "worker_id": workers[0]["value"] if workers else None,
        "insurance_type_code_id": types[0]["value"] if types else None,
        "insurance_status_code_id": status[0]["value"] if status else None,
        "join_date": "",
        "leave_date": "",
        "company_rate": 0.0,
        "worker_rate": 0.0,
        "note": "",
    }


def _init_state(conn) -> None:
    if "insurance_search_text" not in st.session_state:
        st.session_state["insurance_search_text"] = ""
    if "insurance_selected_ids" not in st.session_state:
        st.session_state["insurance_selected_ids"] = []
    if "insurance_form" not in st.session_state:
        st.session_state["insurance_form"] = _default_form(conn)
    if "insurance_company_rate_text" not in st.session_state:
        st.session_state["insurance_company_rate_text"] = "0.00"
    if "insurance_worker_rate_text" not in st.session_state:
        st.session_state["insurance_worker_rate_text"] = "0.00"
    if "insurance_export_columns" not in st.session_state:
        st.session_state["insurance_export_columns"] = DEFAULT_LIST_COLUMNS[:]


def _reset_form(conn, clear_selection: bool = True) -> None:
    st.session_state["insurance_form"] = _default_form(conn)
    st.session_state["insurance_company_rate_text"] = "0.00"
    st.session_state["insurance_worker_rate_text"] = "0.00"
    if clear_selection:
        st.session_state["insurance_selected_ids"] = []


def _fill_form(row: dict) -> None:
    st.session_state["insurance_form"] = {
        "worker_insurance_id": int(row.get("worker_insurance_id") or 0),
        "worker_id": row.get("worker_id"),
        "insurance_type_code_id": row.get("insurance_type_code_id"),
        "insurance_status_code_id": row.get("insurance_status_code_id"),
        "join_date": _fmt_date(row.get("join_date")),
        "leave_date": _fmt_date(row.get("leave_date")),
        "company_rate": _to_float(row.get("company_rate")),
        "worker_rate": _to_float(row.get("worker_rate")),
        "note": str(row.get("note") or ""),
    }
    st.session_state["insurance_company_rate_text"] = _fmt_rate(row.get("company_rate"))
    st.session_state["insurance_worker_rate_text"] = _fmt_rate(row.get("worker_rate"))


def _normalize_form() -> None:
    form = st.session_state["insurance_form"]
    form["join_date"] = _fmt_date(form.get("join_date"))
    form["leave_date"] = _fmt_date(form.get("leave_date"))
    st.session_state["insurance_form"] = form
    st.session_state["insurance_company_rate_text"] = _fmt_rate(form.get("company_rate"))
    st.session_state["insurance_worker_rate_text"] = _fmt_rate(form.get("worker_rate"))


def _select(options, label, value, key):
    data = [{"value": None, "label": "선택"}] + (options or [])
    idx = st.selectbox(label, range(len(data)), index=_pick_option(data, value), format_func=lambda x: data[x]["label"], key=key)
    return data[idx]["value"]


def _display_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=["선택"] + DEFAULT_LIST_COLUMNS)
    df = raw_df.copy()
    rename_map = {k: v for k, v in DISPLAY_COLUMNS.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    ordered = [col for col in DISPLAY_COLUMNS.values() if col in df.columns]
    return df[ordered]


def render_insurance_manage_page(conn) -> None:
    _init_state(conn)
    _normalize_form()
    opts = get_insurance_form_options(conn)

    st.title("보험관리")
    left_col, right_col = st.columns([4, 6])

    with right_col:
        st.markdown("### 보험 목록")
        st.session_state["insurance_search_text"] = st.text_input("검색", value=st.session_state["insurance_search_text"], key="insurance_search_text_input")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("조회", width="stretch", key="insurance_btn_search"):
                st.rerun()
        with b2:
            if st.button("전체선택", width="stretch", key="insurance_btn_all"):
                df_all = get_insurance_list(conn, st.session_state["insurance_search_text"])
                st.session_state["insurance_selected_ids"] = [] if df_all.empty else df_all["worker_insurance_id"].astype(int).tolist()
                st.rerun()
        with b3:
            if st.button("선택해제", width="stretch", key="insurance_btn_clear"):
                st.session_state["insurance_selected_ids"] = []
                _reset_form(conn, clear_selection=False)
                st.rerun()

        raw_df = get_insurance_list(conn, st.session_state["insurance_search_text"])
        view_df = _display_df(raw_df)
        if not view_df.empty:
            grid_df = view_df.copy()
            grid_df.insert(0, "선택", raw_df["worker_insurance_id"].isin(st.session_state.get("insurance_selected_ids", [])))
            grid_df.insert(1, "_worker_insurance_id", raw_df["worker_insurance_id"].astype(int))
        else:
            grid_df = pd.DataFrame(columns=["선택", "_worker_insurance_id"] + DEFAULT_LIST_COLUMNS)

        previous_selected = list(st.session_state.get("insurance_selected_ids", []))
        edited_df = st.data_editor(grid_df, hide_index=True, width="stretch", height=720, disabled=[c for c in grid_df.columns if c != "선택"], column_config={"_worker_insurance_id": None}, key="insurance_grid_editor")
        selected = edited_df.loc[edited_df["선택"] == True, "_worker_insurance_id"].astype(int).tolist() if (not edited_df.empty and "선택" in edited_df.columns) else []

        if selected != previous_selected:
            st.session_state["insurance_selected_ids"] = selected
            if len(selected) == 1:
                detail = get_insurance_detail(conn, selected[0])
                if detail:
                    _fill_form(detail)
            else:
                _reset_form(conn, clear_selection=False)
            st.rerun()

        current_selected = list(st.session_state.get("insurance_selected_ids", []))
        if len(current_selected) == 1:
            current_form = st.session_state.get("insurance_form", {})
            if int(current_form.get("worker_insurance_id") or 0) != int(current_selected[0]):
                detail = get_insurance_detail(conn, current_selected[0])
                if detail:
                    _fill_form(detail)
        elif len(current_selected) > 1:
            _reset_form(conn, clear_selection=False)
            st.info("입력 화면에 데이타가 다중 선택되었습니다.")

        st.session_state["insurance_export_columns"] = st.multiselect(
            "엑셀 다운로드 필드 선택",
            options=list(DISPLAY_COLUMNS.values()),
            default=st.session_state.get("insurance_export_columns", list(DISPLAY_COLUMNS.values())),
            key="insurance_export_columns_select",
        )
        export_cols = st.session_state["insurance_export_columns"] or list(DISPLAY_COLUMNS.values())
        export_df = view_df[export_cols] if (not view_df.empty and all(c in view_df.columns for c in export_cols)) else view_df
        if current_selected and not raw_df.empty:
            export_df = export_df.loc[raw_df["worker_insurance_id"].isin(set(current_selected))].reset_index(drop=True)
        st.download_button("선택자료 엑셀 다운로드", data=dataframe_to_excel_bytes(export_df, "보험관리"), file_name=f"보험관리_{_ts()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch", key="insurance_selected_excel_download")

    form = st.session_state["insurance_form"]
    with left_col:
        st.markdown("### 보험 입력")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("신규", width="stretch", key="insurance_btn_new"):
                _reset_form(conn)
                st.rerun()
        with b2:
            if st.button("저장", width="stretch", key="insurance_btn_save"):
                try:
                    st.session_state["insurance_form"]["company_rate"] = _to_float(st.session_state.get("insurance_company_rate_text", "0"))
                    st.session_state["insurance_form"]["worker_rate"] = _to_float(st.session_state.get("insurance_worker_rate_text", "0"))
                    new_id = save_insurance_data(conn, st.session_state["insurance_form"], _actor())
                    conn.commit()
                    st.session_state["insurance_selected_ids"] = [new_id]
                    st.success("저장되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        with b3:
            if st.button("삭제", width="stretch", key="insurance_btn_delete"):
                deleted = remove_insurance_rows(conn, st.session_state.get("insurance_selected_ids", []))
                conn.commit()
                _reset_form(conn)
                st.success(f"{deleted}건 삭제되었습니다.")
                st.rerun()

        st.text_input("보험ID", value=str(form.get("worker_insurance_id") or ""), disabled=True, key="insurance_form_id_view")

        type_options = opts.get("insurance_type") or []
        if type_options:
            type_tabs = st.tabs([row["label"] for row in type_options])
            for idx, tab in enumerate(type_tabs):
                with tab:
                    if st.button(f"{type_options[idx]['label']} 선택", key=f"insurance_type_btn_{idx}", width="stretch"):
                        st.session_state["insurance_form"]["insurance_type_code_id"] = type_options[idx]["value"]
                        st.rerun()
        form["worker_id"] = _select(opts.get("worker"), "작업자", form["worker_id"], "insurance_form_worker")
        form["insurance_type_code_id"] = _select(type_options, "보험종류", form["insurance_type_code_id"], "insurance_form_type")
        form["insurance_status_code_id"] = _select(opts.get("insurance_status"), "보험가입상태", form["insurance_status_code_id"], "insurance_form_status")
        c1, c2 = st.columns(2)
        with c1:
            form["join_date"] = st.text_input("가입일자", value=form["join_date"], key="insurance_form_join_date")
        with c2:
            form["leave_date"] = st.text_input("탈퇴일자", value=form["leave_date"], key="insurance_form_leave_date")
        c3, c4 = st.columns(2)
        with c3:
            st.session_state["insurance_company_rate_text"] = st.text_input("회사부담률", value=st.session_state.get("insurance_company_rate_text", "0.00"), key="insurance_company_rate_text_input")
        with c4:
            st.session_state["insurance_worker_rate_text"] = st.text_input("근로자부담률", value=st.session_state.get("insurance_worker_rate_text", "0.00"), key="insurance_worker_rate_text_input")
        form["note"] = st.text_area("비고", value=form["note"], height=120, key="insurance_form_note")
        st.session_state["insurance_form"] = form


def run_insurance_manage_page(conn) -> None:
    render_insurance_manage_page(conn)
