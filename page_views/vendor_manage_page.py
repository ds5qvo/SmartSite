# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 업체관리 화면
파일명     : vendor_manage_page.py
설명       : companies 테이블 기준 업체관리 입력/조회/수정/삭제 화면.
사용 테이블 :
    - companies
    - mst_code
    - dtl_code
작성일시   : 2026-03-18
변경이력   :
주의사항   :
    - 임의 테이블 생성 금지
    - 전체 테이블 정의 기준 컬럼만 사용
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from repositories.common_schema_repository import require_table
from repositories.reference_lookup_repository import (
    get_code_label,
    get_code_options_by_mst_candidates,
)
from services.vendor_service import get_vendor_detail, remove_vendors, save_vendor_data, search_vendors


def _init_state() -> None:
    defaults = {
        "vendor_search_text": "",
        "vendor_search_company_type_code_id": 0,
        "vendor_search_use_yn_code_id": 0,
        "vendor_selected_ids": [],
        "vendor_form_company_id": 0,
        "vendor_form_company_code": "",
        "vendor_form_company_name": "",
        "vendor_form_company_type_code_id": 0,
        "vendor_form_company_status_code_id": 0,
        "vendor_form_company_class_code_id": 0,
        "vendor_form_business_no": "",
        "vendor_form_corporation_no": "",
        "vendor_form_ceo_name": "",
        "vendor_form_business_type_code_id": 0,
        "vendor_form_business_item_code_id": 0,
        "vendor_form_zip_code": "",
        "vendor_form_address": "",
        "vendor_form_address_detail": "",
        "vendor_form_phone": "",
        "vendor_form_fax": "",
        "vendor_form_email": "",
        "vendor_form_use_yn_code_id": 0,
        "vendor_form_note": "",
        "vendor_multi_message": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _get_login_user_name() -> str:
    for key in ["login_user_name", "user_name", "login_id"]:
        if st.session_state.get(key):
            return str(st.session_state.get(key))
    return "system"


def _reset_form(clear_selection: bool = True) -> None:
    for key in [
        "vendor_form_company_id",
        "vendor_form_company_type_code_id",
        "vendor_form_company_status_code_id",
        "vendor_form_company_class_code_id",
        "vendor_form_business_type_code_id",
        "vendor_form_business_item_code_id",
        "vendor_form_use_yn_code_id",
    ]:
        st.session_state[key] = 0
    for key in [
        "vendor_form_company_code",
        "vendor_form_company_name",
        "vendor_form_business_no",
        "vendor_form_corporation_no",
        "vendor_form_ceo_name",
        "vendor_form_zip_code",
        "vendor_form_address",
        "vendor_form_address_detail",
        "vendor_form_phone",
        "vendor_form_fax",
        "vendor_form_email",
        "vendor_form_note",
    ]:
        st.session_state[key] = ""
    st.session_state["vendor_multi_message"] = ""
    if clear_selection:
        st.session_state["vendor_selected_ids"] = []


def _fill_form(detail: Dict[str, Any]) -> None:
    st.session_state["vendor_form_company_id"] = int(detail.get("company_id") or 0)
    st.session_state["vendor_form_company_code"] = str(detail.get("company_code") or "")
    st.session_state["vendor_form_company_name"] = str(detail.get("company_name") or "")
    st.session_state["vendor_form_company_type_code_id"] = int(detail.get("company_type_code_id") or 0)
    st.session_state["vendor_form_company_status_code_id"] = int(detail.get("company_status_code_id") or 0)
    st.session_state["vendor_form_company_class_code_id"] = int(detail.get("company_class_code_id") or 0)
    st.session_state["vendor_form_business_no"] = str(detail.get("business_no") or "")
    st.session_state["vendor_form_corporation_no"] = str(detail.get("corporation_no") or "")
    st.session_state["vendor_form_ceo_name"] = str(detail.get("ceo_name") or "")
    st.session_state["vendor_form_business_type_code_id"] = int(detail.get("business_type_code_id") or 0)
    st.session_state["vendor_form_business_item_code_id"] = int(detail.get("business_item_code_id") or 0)
    st.session_state["vendor_form_zip_code"] = str(detail.get("zip_code") or "")
    st.session_state["vendor_form_address"] = str(detail.get("address") or "")
    st.session_state["vendor_form_address_detail"] = str(detail.get("address_detail") or "")
    st.session_state["vendor_form_phone"] = str(detail.get("phone") or "")
    st.session_state["vendor_form_fax"] = str(detail.get("fax") or "")
    st.session_state["vendor_form_email"] = str(detail.get("email") or "")
    st.session_state["vendor_form_use_yn_code_id"] = int(detail.get("use_yn_code_id") or 0)
    st.session_state["vendor_form_note"] = str(detail.get("note") or "")
    st.session_state["vendor_multi_message"] = ""


def _code_selectbox(label: str, key: str, options: List[Dict[str, Any]], allow_all: bool = False) -> int:
    option_items: List[tuple[int, str]] = []
    option_items.append((0, "전체" if allow_all else "선택"))
    for item in options:
        option_items.append((int(item.get("dtl_code_id") or 0), f"{item.get('dtl_code', '')} {item.get('code_name', '')}".strip()))

    current_value = int(st.session_state.get(key) or 0)
    ids = [item[0] for item in option_items]
    if current_value not in ids:
        st.session_state[key] = 0
        current_value = 0

    index = ids.index(current_value)
    selected_label = st.selectbox(label, [item[1] for item in option_items], index=index, key=f"{key}_widget")
    selected_index = [item[1] for item in option_items].index(selected_label)
    selected_value = option_items[selected_index][0]
    st.session_state[key] = selected_value
    return selected_value


def render_vendor_manage_page(conn) -> None:
    _init_state()
    st.title("업체관리")

    try:
        require_table(conn, "companies")
    except Exception as exc:
        st.error(str(exc))
        return

    company_type_options = get_code_options_by_mst_candidates(conn, ["COMPANY_TYPE", "업체유형"])
    company_status_options = get_code_options_by_mst_candidates(conn, ["COMPANY_STATUS", "거래상태"])
    company_class_options = get_code_options_by_mst_candidates(conn, ["COMPANY_CLASS", "법인개인구분"])
    business_type_options = get_code_options_by_mst_candidates(conn, ["BUSINESS_TYPE", "업태"])
    business_item_options = get_code_options_by_mst_candidates(conn, ["BUSINESS_ITEM", "종목"])
    use_yn_options = get_code_options_by_mst_candidates(conn, ["USE_YN", "사용여부"])

    left_col, right_col = st.columns([4, 6], gap="medium")

    with left_col:
        st.subheader("업체 입력")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("신규", width="stretch", key="vendor_btn_new"):
                _reset_form(clear_selection=True)
                st.rerun()
        with b2:
            if st.button("저장", width="stretch", key="vendor_btn_save"):
                try:
                    data = {
                        "company_id": st.session_state.get("vendor_form_company_id", 0),
                        "company_code": str(st.session_state.get("vendor_form_company_code") or "").strip(),
                        "company_name": str(st.session_state.get("vendor_form_company_name") or "").strip(),
                        "company_type_code_id": st.session_state.get("vendor_form_company_type_code_id", 0) or None,
                        "company_status_code_id": st.session_state.get("vendor_form_company_status_code_id", 0) or None,
                        "company_class_code_id": st.session_state.get("vendor_form_company_class_code_id", 0) or None,
                        "business_no": str(st.session_state.get("vendor_form_business_no") or "").strip(),
                        "corporation_no": str(st.session_state.get("vendor_form_corporation_no") or "").strip(),
                        "ceo_name": str(st.session_state.get("vendor_form_ceo_name") or "").strip(),
                        "business_type_code_id": st.session_state.get("vendor_form_business_type_code_id", 0) or None,
                        "business_item_code_id": st.session_state.get("vendor_form_business_item_code_id", 0) or None,
                        "zip_code": str(st.session_state.get("vendor_form_zip_code") or "").strip(),
                        "address": str(st.session_state.get("vendor_form_address") or "").strip(),
                        "address_detail": str(st.session_state.get("vendor_form_address_detail") or "").strip(),
                        "phone": str(st.session_state.get("vendor_form_phone") or "").strip(),
                        "fax": str(st.session_state.get("vendor_form_fax") or "").strip(),
                        "email": str(st.session_state.get("vendor_form_email") or "").strip(),
                        "use_yn_code_id": st.session_state.get("vendor_form_use_yn_code_id", 0) or None,
                        "note": str(st.session_state.get("vendor_form_note") or "").strip(),
                    }
                    saved_id = save_vendor_data(conn, data, actor=_get_login_user_name())
                    detail = get_vendor_detail(conn, saved_id) or {}
                    _fill_form(detail)
                    st.success("업체자료가 저장되었습니다.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        with b3:
            if st.button("삭제", width="stretch", key="vendor_btn_delete"):
                try:
                    selected_ids = st.session_state.get("vendor_selected_ids", [])
                    remove_vendors(conn, selected_ids)
                    _reset_form(clear_selection=True)
                    st.success("선택한 업체자료가 삭제되었습니다.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

        c1, c2 = st.columns(2)
        with c1:
            st.text_input("업체코드", key="vendor_form_company_code")
        with c2:
            st.text_input("업체명", key="vendor_form_company_name")
        c3, c4 = st.columns(2)
        with c3:
            _code_selectbox("업체유형", "vendor_form_company_type_code_id", company_type_options)
        with c4:
            _code_selectbox("거래상태", "vendor_form_company_status_code_id", company_status_options)
        c5, c6 = st.columns(2)
        with c5:
            _code_selectbox("법인/개인구분", "vendor_form_company_class_code_id", company_class_options)
        with c6:
            _code_selectbox("사용여부", "vendor_form_use_yn_code_id", use_yn_options)
        st.text_input("사업자등록번호", key="vendor_form_business_no")
        st.text_input("법인등록번호", key="vendor_form_corporation_no")
        st.text_input("대표자명", key="vendor_form_ceo_name")
        c7, c8 = st.columns(2)
        with c7:
            _code_selectbox("업태", "vendor_form_business_type_code_id", business_type_options)
        with c8:
            _code_selectbox("종목", "vendor_form_business_item_code_id", business_item_options)
        st.text_input("우편번호", key="vendor_form_zip_code")
        st.text_input("주소", key="vendor_form_address")
        st.text_input("상세주소", key="vendor_form_address_detail")
        c9, c10 = st.columns(2)
        with c9:
            st.text_input("대표전화", key="vendor_form_phone")
        with c10:
            st.text_input("팩스번호", key="vendor_form_fax")
        st.text_input("이메일", key="vendor_form_email")
        st.text_area("비고", key="vendor_form_note", height=120)

        if st.session_state.get("vendor_multi_message"):
            st.warning(st.session_state.get("vendor_multi_message"))

    with right_col:
        st.subheader("업체 목록")
        s1, s2, s3 = st.columns([3, 2, 2])
        with s1:
            st.text_input("검색", key="vendor_search_text", placeholder="업체코드 / 업체명 / 사업자번호 / 대표자명")
        with s2:
            _code_selectbox("업체유형", "vendor_search_company_type_code_id", company_type_options, allow_all=True)
        with s3:
            _code_selectbox("사용여부", "vendor_search_use_yn_code_id", use_yn_options, allow_all=True)

        a1, a2, a3 = st.columns(3)
        with a1:
            st.button("조회", width="stretch", key="vendor_btn_search")
        with a2:
            if st.button("전체선택", width="stretch", key="vendor_btn_select_all"):
                current_df = search_vendors(
                    conn,
                    search_text=st.session_state.get("vendor_search_text", ""),
                    company_type_code_id=st.session_state.get("vendor_search_company_type_code_id", 0) or None,
                    use_yn_code_id=st.session_state.get("vendor_search_use_yn_code_id", 0) or None,
                )
                st.session_state["vendor_selected_ids"] = current_df["company_id"].astype(int).tolist() if not current_df.empty else []
                st.rerun()
        with a3:
            if st.button("선택해제", width="stretch", key="vendor_btn_clear_select"):
                st.session_state["vendor_selected_ids"] = []
                st.rerun()

        df = search_vendors(
            conn,
            search_text=st.session_state.get("vendor_search_text", ""),
            company_type_code_id=st.session_state.get("vendor_search_company_type_code_id", 0) or None,
            use_yn_code_id=st.session_state.get("vendor_search_use_yn_code_id", 0) or None,
        )

        if df.empty:
            st.info("조회된 업체자료가 없습니다.")
        else:
            display_df = df.copy()
            display_df.insert(0, "선택", display_df["company_id"].astype(int).isin(st.session_state.get("vendor_selected_ids", [])))
            display_df["업체유형"] = display_df["company_type_code_id"].apply(lambda x: get_code_label(company_type_options, int(x) if pd.notna(x) else None))
            display_df["거래상태"] = display_df["company_status_code_id"].apply(lambda x: get_code_label(company_status_options, int(x) if pd.notna(x) else None))
            display_df["사용여부"] = display_df["use_yn_code_id"].apply(lambda x: get_code_label(use_yn_options, int(x) if pd.notna(x) else None))
            display_df = display_df[
                ["선택", "company_id", "company_code", "company_name", "업체유형", "거래상태", "business_no", "ceo_name", "phone", "email", "사용여부"]
            ].rename(columns={"company_id": "업체ID", "company_code": "업체코드", "company_name": "업체명", "business_no": "사업자등록번호", "ceo_name": "대표자명", "phone": "대표전화", "email": "이메일"})

            edited_df = st.data_editor(
                display_df,
                hide_index=True,
                width="stretch",
                disabled=["업체ID", "업체코드", "업체명", "업체유형", "거래상태", "사업자등록번호", "대표자명", "대표전화", "이메일", "사용여부"],
                column_config={"선택": st.column_config.CheckboxColumn("선택")},
                key="vendor_list_editor",
            )

            selected_ids = display_df.loc[edited_df["선택"], "업체ID"].astype(int).tolist()
            previous_ids = st.session_state.get("vendor_selected_ids", [])
            if selected_ids != previous_ids:
                st.session_state["vendor_selected_ids"] = selected_ids
                if len(selected_ids) == 1:
                    detail = get_vendor_detail(conn, selected_ids[0]) or {}
                    _fill_form(detail)
                elif len(selected_ids) > 1:
                    _reset_form(clear_selection=False)
                    st.session_state["vendor_multi_message"] = "입력 화면에 데이타가 다중 선택되었습니다."
                else:
                    st.session_state["vendor_multi_message"] = ""
                st.rerun()
