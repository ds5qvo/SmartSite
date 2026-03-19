# -*- coding: utf-8 -*-
"""
프로그램명 : project_manage_page.py
파일경로   : page_views/project_manage_page.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from page_views._page_common import _actor, _excel_upload_to_df, _pick_option, _ts
from services.project_service import (
    dataframe_to_excel_bytes,
    get_project_detail,
    get_project_form_options,
    get_project_list,
    remove_project_rows,
    save_project_data,
)

DISPLAY_COLUMNS = {
    "project_id": "프로젝트ID",
    "project_code": "프로젝트코드",
    "project_name": "프로젝트명",
    "공사구분": "공사구분",
    "프로젝트상태": "프로젝트상태",
    "계약상태": "계약상태",
    "발주처": "발주처",
    "원청업체": "원청업체",
    "실제수행업체": "실제수행업체",
    "프로젝트관리자": "프로젝트관리자",
    "contract_date": "계약일자",
    "start_date": "시작일자",
    "end_date": "종료일자",
    "site_name": "현장명",
    "zip_code": "우편번호",
    "address": "주소",
    "address_detail": "상세주소",
    "계약금액": "계약금액",
    "description": "설명",
    "사용여부": "사용여부",
    "note": "비고",
}
DEFAULT_LIST_COLUMNS = ["프로젝트ID", "프로젝트코드", "프로젝트명", "공사구분", "프로젝트상태", "현장명", "계약금액", "사용여부"]


def _digits(value) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def _fmt_date(value) -> str:
    raw = _digits(value)
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return str(value or "").strip()


def _fmt_amount_text(value) -> str:
    try:
        return f"{float(str(value).replace(',', '') or 0):,.0f}"
    except Exception:
        return "0"


def _to_float(value) -> float:
    try:
        return float(str(value).replace(',', '') or 0)
    except Exception:
        return 0.0


def _default_form(conn) -> dict:
    options = get_project_form_options(conn)
    use_opts = options.get("use_yn", [])
    default_use = use_opts[0]["value"] if use_opts else None
    workers = options.get("workers", [])
    default_worker = workers[0]["value"] if workers else None
    return {
        "project_id": 0,
        "project_code": "",
        "project_name": "",
        "project_type_code_id": None,
        "project_status_code_id": None,
        "contract_status_code_id": None,
        "client_company_id": None,
        "main_contractor_company_id": None,
        "performing_company_id": None,
        "project_manager_user_id": default_worker,
        "contract_date": "",
        "start_date": "",
        "end_date": "",
        "site_name": "",
        "zip_code": "",
        "address": "",
        "address_detail": "",
        "contract_amount": 0.0,
        "description": "",
        "use_yn_code_id": default_use,
        "note": "",
    }


def _init_state(conn) -> None:
    if "project_search_text" not in st.session_state:
        st.session_state["project_search_text"] = ""
    if "project_selected_ids" not in st.session_state:
        st.session_state["project_selected_ids"] = []
    if "project_form" not in st.session_state:
        st.session_state["project_form"] = _default_form(conn)
    if "project_contract_amount_text" not in st.session_state:
        st.session_state["project_contract_amount_text"] = "0"
    if "project_export_columns" not in st.session_state:
        st.session_state["project_export_columns"] = DEFAULT_LIST_COLUMNS[:]


def _reset_form(conn, clear_selection: bool = True) -> None:
    st.session_state["project_form"] = _default_form(conn)
    st.session_state["project_contract_amount_text"] = "0"
    if clear_selection:
        st.session_state["project_selected_ids"] = []


def _fill_form(row: dict) -> None:
    st.session_state["project_form"] = {
        "project_id": int(row.get("project_id") or 0),
        "project_code": str(row.get("project_code") or ""),
        "project_name": str(row.get("project_name") or ""),
        "project_type_code_id": row.get("project_type_code_id"),
        "project_status_code_id": row.get("project_status_code_id"),
        "contract_status_code_id": row.get("contract_status_code_id"),
        "client_company_id": row.get("client_company_id"),
        "main_contractor_company_id": row.get("main_contractor_company_id"),
        "performing_company_id": row.get("performing_company_id"),
        "project_manager_user_id": row.get("project_manager_user_id"),
        "contract_date": _fmt_date(row.get("contract_date")),
        "start_date": _fmt_date(row.get("start_date")),
        "end_date": _fmt_date(row.get("end_date")),
        "site_name": str(row.get("site_name") or ""),
        "zip_code": str(row.get("zip_code") or ""),
        "address": str(row.get("address") or ""),
        "address_detail": str(row.get("address_detail") or ""),
        "contract_amount": _to_float(row.get("contract_amount")),
        "description": str(row.get("description") or ""),
        "use_yn_code_id": row.get("use_yn_code_id"),
        "note": str(row.get("note") or ""),
    }
    st.session_state["project_contract_amount_text"] = _fmt_amount_text(row.get("contract_amount"))


def _normalize_form() -> None:
    form = st.session_state["project_form"]
    form["contract_date"] = _fmt_date(form.get("contract_date"))
    form["start_date"] = _fmt_date(form.get("start_date"))
    form["end_date"] = _fmt_date(form.get("end_date"))
    st.session_state["project_form"] = form
    st.session_state["project_contract_amount_text"] = _fmt_amount_text(form.get("contract_amount"))


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


def render_project_manage_page(conn) -> None:
    _init_state(conn)
    _normalize_form()
    opts = get_project_form_options(conn)

    st.title("프로젝트관리")
    left_col, right_col = st.columns([4, 6])

    with right_col:
        st.markdown("### 프로젝트 목록")
        st.session_state["project_search_text"] = st.text_input("검색", value=st.session_state["project_search_text"], key="project_search_text_input")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("조회", width="stretch", key="project_btn_search"):
                st.rerun()
        with b2:
            if st.button("전체선택", width="stretch", key="project_btn_all"):
                df_all = get_project_list(conn, st.session_state["project_search_text"])
                st.session_state["project_selected_ids"] = [] if df_all.empty else df_all["project_id"].astype(int).tolist()
                st.rerun()
        with b3:
            if st.button("선택해제", width="stretch", key="project_btn_clear"):
                st.session_state["project_selected_ids"] = []
                _reset_form(conn, clear_selection=False)
                st.rerun()

        raw_df = get_project_list(conn, st.session_state["project_search_text"])
        view_df = _display_df(raw_df)
        if not view_df.empty:
            grid_df = view_df.copy()
            grid_df.insert(0, "선택", raw_df["project_id"].isin(st.session_state.get("project_selected_ids", [])))
            grid_df.insert(1, "_project_id", raw_df["project_id"].astype(int))
        else:
            grid_df = pd.DataFrame(columns=["선택", "_project_id"] + DEFAULT_LIST_COLUMNS)

        previous_selected = list(st.session_state.get("project_selected_ids", []))
        edited_df = st.data_editor(
            grid_df,
            hide_index=True,
            width="stretch",
            height=720,
            disabled=[c for c in grid_df.columns if c != "선택"],
            column_config={"_project_id": None},
            key="project_grid_editor",
        )
        selected = edited_df.loc[edited_df["선택"] == True, "_project_id"].astype(int).tolist() if (not edited_df.empty and "선택" in edited_df.columns) else []

        if selected != previous_selected:
            st.session_state["project_selected_ids"] = selected
            if len(selected) == 1:
                detail = get_project_detail(conn, selected[0])
                if detail:
                    _fill_form(detail)
            else:
                _reset_form(conn, clear_selection=False)
            st.rerun()

        current_selected = list(st.session_state.get("project_selected_ids", []))
        if len(current_selected) == 1:
            current_form = st.session_state.get("project_form", {})
            if int(current_form.get("project_id") or 0) != int(current_selected[0]):
                detail = get_project_detail(conn, current_selected[0])
                if detail:
                    _fill_form(detail)
        elif len(current_selected) > 1:
            _reset_form(conn, clear_selection=False)
            st.info("입력 화면에 데이타가 다중 선택되었습니다.")

        st.session_state["project_export_columns"] = st.multiselect(
            "엑셀 다운로드 필드 선택",
            options=list(DISPLAY_COLUMNS.values()),
            default=st.session_state.get("project_export_columns", list(DISPLAY_COLUMNS.values())),
            key="project_export_columns_select",
        )
        export_cols = st.session_state["project_export_columns"] or list(DISPLAY_COLUMNS.values())
        export_df = view_df[export_cols] if (not view_df.empty and all(c in view_df.columns for c in export_cols)) else view_df
        if current_selected and not raw_df.empty:
            export_df = export_df.loc[raw_df["project_id"].isin(set(current_selected))].reset_index(drop=True)
        st.download_button(
            "선택자료 엑셀 다운로드",
            data=dataframe_to_excel_bytes(export_df, "프로젝트관리"),
            file_name=f"프로젝트관리_{_ts()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
            key="project_selected_excel_download",
        )

    form = st.session_state["project_form"]
    with left_col:
        st.markdown("### 프로젝트 입력")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("신규", width="stretch", key="project_btn_new"):
                _reset_form(conn)
                st.rerun()
        with b2:
            if st.button("저장", width="stretch", key="project_btn_save"):
                try:
                    st.session_state["project_form"]["contract_amount"] = _to_float(st.session_state.get("project_contract_amount_text", "0"))
                    new_id = save_project_data(conn, st.session_state["project_form"], _actor())
                    conn.commit()
                    st.session_state["project_selected_ids"] = [new_id]
                    st.success("저장되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        with b3:
            if st.button("삭제", width="stretch", key="project_btn_delete"):
                deleted = remove_project_rows(conn, st.session_state.get("project_selected_ids", []))
                conn.commit()
                _reset_form(conn)
                st.success(f"{deleted}건 삭제되었습니다.")
                st.rerun()

        st.text_input("프로젝트ID", value=str(form.get("project_id") or ""), disabled=True, placeholder="1200001부터 자동부여", key="project_form_project_id_view")
        st.text_input("프로젝트코드", value=str(form.get("project_code") or ""), disabled=True, placeholder="저장 시 JS-P-001 자동부여", key="project_form_project_code_view")

        tabs = st.tabs(["기본정보", "거래정보", "현장정보"])
        with tabs[0]:
            form["project_name"] = st.text_input("프로젝트명", value=form["project_name"], key="project_form_project_name")
            c1, c2 = st.columns(2)
            with c1:
                form["project_type_code_id"] = _select(opts.get("project_type"), "공사구분", form["project_type_code_id"], "project_form_project_type")
            with c2:
                form["project_status_code_id"] = _select(opts.get("project_status"), "프로젝트상태", form["project_status_code_id"], "project_form_project_status")
            c3, c4 = st.columns(2)
            with c3:
                form["contract_status_code_id"] = _select(opts.get("contract_status"), "계약상태", form["contract_status_code_id"], "project_form_contract_status")
            with c4:
                form["use_yn_code_id"] = _select(opts.get("use_yn"), "사용여부", form["use_yn_code_id"], "project_form_use_yn")
            form["project_manager_user_id"] = _select(opts.get("workers") or [], "프로젝트관리자", form["project_manager_user_id"], "project_form_manager")
        with tabs[1]:
            company_opts = opts.get("companies") or []
            form["client_company_id"] = _select(company_opts, "발주처", form["client_company_id"], "project_form_client_company")
            form["main_contractor_company_id"] = _select(company_opts, "원청업체", form["main_contractor_company_id"], "project_form_main_company")
            form["performing_company_id"] = _select(company_opts, "실제수행업체", form["performing_company_id"], "project_form_performing_company")
            d1, d2, d3 = st.columns(3)
            with d1:
                form["contract_date"] = st.text_input("계약일자", value=form["contract_date"], key="project_form_contract_date")
            with d2:
                form["start_date"] = st.text_input("시작일자", value=form["start_date"], key="project_form_start_date")
            with d3:
                form["end_date"] = st.text_input("종료일자", value=form["end_date"], key="project_form_end_date")
            st.session_state["project_contract_amount_text"] = st.text_input("계약금액", value=st.session_state.get("project_contract_amount_text", "0"), key="project_contract_amount_text_input")
            form["description"] = st.text_area("설명", value=form["description"], height=100, key="project_form_description")
        with tabs[2]:
            form["site_name"] = st.text_input("현장명", value=form["site_name"], key="project_form_site_name")
            c5, c6 = st.columns([1, 2])
            with c5:
                form["zip_code"] = st.text_input("우편번호", value=form["zip_code"], key="project_form_zip_code")
            with c6:
                form["address"] = st.text_input("주소", value=form["address"], key="project_form_address")
            form["address_detail"] = st.text_input("상세주소", value=form["address_detail"], key="project_form_address_detail")
            form["note"] = st.text_area("비고", value=form["note"], height=120, key="project_form_note")

        st.session_state["project_form"] = form

        st.markdown("#### 엑셀 기능")
        sample_df = pd.DataFrame(columns=list(DISPLAY_COLUMNS.keys()))
        d1, d2 = st.columns(2)
        with d1:
            st.download_button("샘플엑셀 다운로드", data=dataframe_to_excel_bytes(sample_df, "프로젝트관리샘플"), file_name="프로젝트관리_샘플.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch", key="project_sample_download")
        with d2:
            uploaded = st.file_uploader("엑셀 업로드", type=["xlsx", "xls", "csv"], key="project_excel_upload")
            if uploaded is not None and st.button("업로드 반영", width="stretch", key="project_excel_apply"):
                try:
                    upload_df = _excel_upload_to_df(uploaded)
                    count = 0
                    for row in upload_df.fillna("").to_dict(orient="records"):
                        save_project_data(conn, row, _actor())
                        count += 1
                    conn.commit()
                    st.success(f"{count}건 업로드되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))


def run_project_manage_page(conn) -> None:
    render_project_manage_page(conn)
