# -*- coding: utf-8 -*-
"""
프로그램명 : company_manage_page.py
파일경로   : page_views/company_manage_page.py
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
from services.company_service import (
    dataframe_to_excel_bytes,
    get_company_detail,
    get_company_form_options,
    get_company_list,
    remove_companies,
    save_company_data,
)


DISPLAY_COLUMNS = {
    "company_id": "업체ID",
    "company_code": "업체코드",
    "company_name": "업체명",
    "업체유형": "업체유형",
    "거래상태": "거래상태",
    "법인/개인구분": "법인/개인구분",
    "business_no": "사업자등록번호",
    "corporation_no": "법인등록번호",
    "ceo_name": "대표자명",
    "업태": "업태",
    "종목": "종목",
    "zip_code": "우편번호",
    "address": "주소",
    "address_detail": "상세주소",
    "phone": "전화번호",
    "fax": "팩스번호",
    "email": "이메일",
    "사용여부": "사용여부",
    "note": "비고",
}
DEFAULT_LIST_COLUMNS = ["업체ID", "업체코드", "업체명", "업체유형", "거래상태", "대표자명", "전화번호", "사용여부"]


def _digits(value) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def _fmt_business_no(value) -> str:
    raw = _digits(value)
    if len(raw) >= 10:
        raw = raw[:10]
        return f"{raw[:3]}-{raw[3:5]}-{raw[5:10]}"
    return raw


def _fmt_phone(value) -> str:
    raw = _digits(value)
    if not raw:
        return ""
    if len(raw) == 8:
        return f"{raw[:4]}-{raw[4:]}"
    if raw.startswith("02"):
        if len(raw) == 9:
            return f"{raw[:2]}-{raw[2:5]}-{raw[5:]}"
        if len(raw) == 10:
            return f"{raw[:2]}-{raw[2:6]}-{raw[6:]}"
    if len(raw) == 10:
        return f"{raw[:3]}-{raw[3:6]}-{raw[6:]}"
    if len(raw) == 11:
        return f"{raw[:3]}-{raw[3:7]}-{raw[7:]}"
    return raw


def _default_form(conn) -> dict:
    opts = get_company_form_options(conn)
    use_opts = opts.get("use_yn", [])
    default_use = use_opts[0]["value"] if use_opts else None
    return {
        "company_id": 0,
        "company_code": "",
        "company_name": "",
        "company_type_code_id": None,
        "company_status_code_id": None,
        "company_class_code_id": None,
        "business_no": "",
        "corporation_no": "",
        "ceo_name": "",
        "business_type_code_id": None,
        "business_item_code_id": None,
        "zip_code": "",
        "address": "",
        "address_detail": "",
        "phone": "",
        "fax": "",
        "email": "",
        "use_yn_code_id": default_use,
        "note": "",
    }


def _init_state(conn) -> None:
    if "company_search_text" not in st.session_state:
        st.session_state["company_search_text"] = ""
    if "company_search_use_yn_code_id" not in st.session_state:
        st.session_state["company_search_use_yn_code_id"] = "전체"
    if "company_selected_ids" not in st.session_state:
        st.session_state["company_selected_ids"] = []
    if "company_form" not in st.session_state:
        st.session_state["company_form"] = _default_form(conn)
    if "company_export_columns" not in st.session_state:
        st.session_state["company_export_columns"] = DEFAULT_LIST_COLUMNS[:]


def _reset_form(conn, clear_selection: bool = True) -> None:
    st.session_state["company_form"] = _default_form(conn)
    if clear_selection:
        st.session_state["company_selected_ids"] = []


def _fill_form(row: dict) -> None:
    st.session_state["company_form"] = {
        "company_id": int(row.get("company_id") or 0),
        "company_code": str(row.get("company_code") or ""),
        "company_name": str(row.get("company_name") or ""),
        "company_type_code_id": row.get("company_type_code_id"),
        "company_status_code_id": row.get("company_status_code_id"),
        "company_class_code_id": row.get("company_class_code_id"),
        "business_no": _fmt_business_no(row.get("business_no")),
        "corporation_no": str(row.get("corporation_no") or ""),
        "ceo_name": str(row.get("ceo_name") or ""),
        "business_type_code_id": row.get("business_type_code_id"),
        "business_item_code_id": row.get("business_item_code_id"),
        "zip_code": str(row.get("zip_code") or ""),
        "address": str(row.get("address") or ""),
        "address_detail": str(row.get("address_detail") or ""),
        "phone": _fmt_phone(row.get("phone")),
        "fax": _fmt_phone(row.get("fax")),
        "email": str(row.get("email") or ""),
        "use_yn_code_id": row.get("use_yn_code_id"),
        "note": str(row.get("note") or ""),
    }


def _normalize_form() -> None:
    form = st.session_state["company_form"]
    form["business_no"] = _fmt_business_no(form.get("business_no"))
    form["phone"] = _fmt_phone(form.get("phone"))
    form["fax"] = _fmt_phone(form.get("fax"))
    st.session_state["company_form"] = form


def _select(options, label, value, key):
    data = [{"value": None, "label": "선택"}] + (options or [])
    idx = st.selectbox(label, range(len(data)), index=_pick_option(data, value), format_func=lambda x: data[x]["label"], key=key)
    return data[idx]["value"]


def _display_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        cols = ["선택"] + DEFAULT_LIST_COLUMNS
        return pd.DataFrame(columns=cols)
    df = raw_df.copy()
    rename_map = {k: v for k, v in DISPLAY_COLUMNS.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    ordered = [col for col in DISPLAY_COLUMNS.values() if col in df.columns]
    df = df[ordered]
    return df


def render_company_manage_page(conn) -> None:
    _init_state(conn)
    _normalize_form()
    opts = get_company_form_options(conn)

    st.title("업체관리")
    left_col, right_col = st.columns([4, 6])

    with right_col:
        st.markdown("### 업체 목록")
        c1, c2 = st.columns([2, 1])
        with c1:
            st.session_state["company_search_text"] = st.text_input("검색", value=st.session_state["company_search_text"], key="company_search_text_input")
        with c2:
            st.session_state["company_search_use_yn_code_id"] = _select(
                [{"value": "전체", "label": "전체"}] + (opts.get("use_yn") or []),
                "사용여부",
                st.session_state["company_search_use_yn_code_id"],
                "company_search_use_yn",
            )
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("조회", width="stretch", key="company_btn_search"):
                st.rerun()
        with b2:
            if st.button("전체선택", width="stretch", key="company_btn_all"):
                df_all = get_company_list(conn, st.session_state["company_search_text"], st.session_state["company_search_use_yn_code_id"])
                st.session_state["company_selected_ids"] = [] if df_all.empty else df_all["company_id"].astype(int).tolist()
                st.rerun()
        with b3:
            if st.button("선택해제", width="stretch", key="company_btn_clear"):
                st.session_state["company_selected_ids"] = []
                _reset_form(conn, clear_selection=False)
                st.rerun()

        raw_df = get_company_list(conn, st.session_state["company_search_text"], st.session_state["company_search_use_yn_code_id"])
        view_df = _display_df(raw_df)
        if not view_df.empty:
            grid_df = view_df.copy()
            grid_df.insert(0, "선택", raw_df["company_id"].isin(st.session_state.get("company_selected_ids", [])))
            grid_df.insert(1, "_company_id", raw_df["company_id"].astype(int))
        else:
            grid_df = pd.DataFrame(columns=["선택", "_company_id"] + DEFAULT_LIST_COLUMNS)

        previous_selected = list(st.session_state.get("company_selected_ids", []))
        edited_df = st.data_editor(grid_df, hide_index=True, width="stretch", height=720, disabled=[c for c in grid_df.columns if c != "선택"], column_config={"_company_id": None}, key="company_grid_editor")
        selected = edited_df.loc[edited_df["선택"] == True, "_company_id"].astype(int).tolist() if (not edited_df.empty and "선택" in edited_df.columns) else []

        if selected != previous_selected:
            st.session_state["company_selected_ids"] = selected
            if len(selected) == 1:
                detail = get_company_detail(conn, selected[0])
                if detail:
                    _fill_form(detail)
            else:
                _reset_form(conn, clear_selection=False)
            st.rerun()

        current_selected = list(st.session_state.get("company_selected_ids", []))
        if len(current_selected) == 1:
            current_form = st.session_state.get("company_form", {})
            if int(current_form.get("company_id") or 0) != int(current_selected[0]):
                detail = get_company_detail(conn, current_selected[0])
                if detail:
                    _fill_form(detail)
        elif len(current_selected) > 1:
            _reset_form(conn, clear_selection=False)
            st.info("입력 화면에 데이타가 다중 선택되었습니다.")

        st.session_state["company_export_columns"] = st.multiselect(
            "엑셀 다운로드 필드 선택",
            options=list(DISPLAY_COLUMNS.values()),
            default=st.session_state.get("company_export_columns", list(DISPLAY_COLUMNS.values())),
            key="company_export_columns_select",
        )
        export_cols = st.session_state["company_export_columns"] or list(DISPLAY_COLUMNS.values())
        export_df = view_df[export_cols] if (not view_df.empty and all(c in view_df.columns for c in export_cols)) else view_df
        if current_selected and not raw_df.empty:
            export_df = export_df.loc[raw_df["company_id"].isin(set(current_selected))].reset_index(drop=True)
        st.download_button(
            "선택자료 엑셀 다운로드",
            data=dataframe_to_excel_bytes(export_df, "업체관리"),
            file_name=f"업체관리_{_ts()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
            key="company_selected_excel_download",
        )

    form = st.session_state["company_form"]
    with left_col:
        st.markdown("### 업체 입력")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("신규", width="stretch", key="company_btn_new"):
                _reset_form(conn)
                st.rerun()
        with b2:
            if st.button("저장", width="stretch", key="company_btn_save"):
                try:
                    new_id = save_company_data(conn, st.session_state["company_form"], _actor())
                    conn.commit()
                    st.session_state["company_selected_ids"] = [new_id]
                    st.success("저장되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        with b3:
            if st.button("삭제", width="stretch", key="company_btn_delete"):
                deleted = remove_companies(conn, st.session_state.get("company_selected_ids", []))
                conn.commit()
                _reset_form(conn)
                st.success(f"{deleted}건 삭제되었습니다.")
                st.rerun()

        st.text_input("업체ID", value=str(form.get("company_id") or ""), disabled=True, placeholder="1100001부터 자동부여", key="company_form_company_id_view")
        st.text_input("업체코드", value=str(form.get("company_code") or ""), disabled=True, placeholder="저장 시 JS-C-001 자동부여", key="company_form_company_code_view")

        tabs = st.tabs(["기본정보", "거래정보", "연락처/주소"])
        with tabs[0]:
            form["company_name"] = st.text_input("업체명", value=form["company_name"], key="company_form_company_name")
            c1, c2 = st.columns(2)
            with c1:
                form["company_type_code_id"] = _select(opts.get("company_type"), "업체유형", form["company_type_code_id"], "company_form_company_type")
            with c2:
                form["company_status_code_id"] = _select(opts.get("company_status"), "거래상태", form["company_status_code_id"], "company_form_company_status")
            c3, c4 = st.columns(2)
            with c3:
                form["company_class_code_id"] = _select(opts.get("company_class"), "법인/개인구분", form["company_class_code_id"], "company_form_company_class")
            with c4:
                form["use_yn_code_id"] = _select(opts.get("use_yn"), "사용여부", form["use_yn_code_id"], "company_form_use_yn")
            form["ceo_name"] = st.text_input("대표자명", value=form["ceo_name"], key="company_form_ceo_name")
            form["business_no"] = st.text_input("사업자등록번호", value=form["business_no"], key="company_form_business_no")
            form["corporation_no"] = st.text_input("법인등록번호", value=form["corporation_no"], key="company_form_corporation_no")
        with tabs[1]:
            c5, c6 = st.columns(2)
            with c5:
                form["business_type_code_id"] = _select(opts.get("business_type"), "업태", form["business_type_code_id"], "company_form_business_type")
            with c6:
                form["business_item_code_id"] = _select(opts.get("business_item"), "종목", form["business_item_code_id"], "company_form_business_item")
            form["note"] = st.text_area("비고", value=form["note"], height=120, key="company_form_note")
        with tabs[2]:
            c7, c8 = st.columns(2)
            with c7:
                form["phone"] = st.text_input("전화번호", value=form["phone"], key="company_form_phone")
            with c8:
                form["fax"] = st.text_input("팩스번호", value=form["fax"], key="company_form_fax")
            form["email"] = st.text_input("이메일", value=form["email"], key="company_form_email")
            c9, c10 = st.columns([1, 2])
            with c9:
                form["zip_code"] = st.text_input("우편번호", value=form["zip_code"], key="company_form_zip_code")
            with c10:
                form["address"] = st.text_input("주소", value=form["address"], key="company_form_address")
            form["address_detail"] = st.text_input("상세주소", value=form["address_detail"], key="company_form_address_detail")

        st.session_state["company_form"] = form

        st.markdown("#### 엑셀 기능")
        sample_df = pd.DataFrame(columns=list(DISPLAY_COLUMNS.keys()))
        d1, d2 = st.columns(2)
        with d1:
            st.download_button("샘플엑셀 다운로드", data=dataframe_to_excel_bytes(sample_df, "업체관리샘플"), file_name="업체관리_샘플.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch", key="company_sample_download")
        with d2:
            uploaded = st.file_uploader("엑셀 업로드", type=["xlsx", "xls", "csv"], key="company_excel_upload")
            if uploaded is not None and st.button("업로드 반영", width="stretch", key="company_excel_apply"):
                try:
                    upload_df = _excel_upload_to_df(uploaded)
                    count = 0
                    for row in upload_df.fillna("").to_dict(orient="records"):
                        save_company_data(conn, row, _actor())
                        count += 1
                    conn.commit()
                    st.success(f"{count}건 업로드되었습니다.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))


def run_company_manage_page(conn) -> None:
    render_company_manage_page(conn)
