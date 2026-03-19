# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 마스터코드관리 화면
파일명     : mst_code_manage_page.py
설명       : 마스터코드 입력/조회/수정/삭제 화면을 구성한다.
사용 테이블 : mst_code, hist_code
주요 기능   :
    1. 4:6 비율 화면 구성
    2. 마스터코드 검색
    3. 리스트 체크박스 선택 / 전체선택 / 선택해제
    4. 단일 선택 시 상세 표시
    5. 다중 선택 시 입력 초기화 및 안내 문구 표시
    6. 초기화 시 체크박스 전체 해제
작성일시   : 2026-03-15
변경이력   :
    - 2026-03-15 : session_state 충돌 방지 구조 적용
주의사항   :
    - 신규 등록 시 mst_code_id는 자동 채번
    - 직접 수정하지 않음
"""

import pandas as pd
import streamlit as st

from services.code_service import CodeService

service = CodeService()


def _init_session_state() -> None:
    defaults = {
        "mst_form_id": None,
        "mst_form_mst_code_value": "",
        "mst_form_code_name_value": "",
        "mst_form_use_yn_value": "Y",
        "mst_form_sort_order_value": 0,
        "mst_search_keyword": "",
        "mst_selected_ids": [],
        "mst_multi_message": "",
        "mst_last_loaded_id": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _clear_form(clear_checks: bool = True) -> None:
    st.session_state["mst_form_id"] = None
    st.session_state["mst_form_mst_code_value"] = ""
    st.session_state["mst_form_code_name_value"] = ""
    st.session_state["mst_form_use_yn_value"] = "Y"
    st.session_state["mst_form_sort_order_value"] = 0
    st.session_state["mst_multi_message"] = ""
    st.session_state["mst_last_loaded_id"] = None
    if clear_checks:
        st.session_state["mst_selected_ids"] = []


def _load_detail_to_form(mst_code_id: int) -> None:
    detail = service.get_mst_code_detail(mst_code_id)
    if not detail:
        return
    st.session_state["mst_form_id"] = detail["mst_code_id"]
    st.session_state["mst_form_mst_code_value"] = detail["mst_code"] or ""
    st.session_state["mst_form_code_name_value"] = detail["code_name"] or ""
    st.session_state["mst_form_use_yn_value"] = detail["use_yn"] or "Y"
    st.session_state["mst_form_sort_order_value"] = int(detail["sort_order"] or 0)
    st.session_state["mst_multi_message"] = ""
    st.session_state["mst_last_loaded_id"] = mst_code_id


def render_mst_code_manage_page() -> None:
    _init_session_state()
    st.subheader("마스터코드관리")

    data = service.get_mst_code_list(st.session_state["mst_search_keyword"])
    df = pd.DataFrame(data)

    selected_ids = st.session_state.get("mst_selected_ids", [])
    if len(selected_ids) == 1:
        selected_id = selected_ids[0]
        if st.session_state.get("mst_last_loaded_id") != selected_id:
            _load_detail_to_form(selected_id)
    elif len(selected_ids) > 1:
        _clear_form(clear_checks=False)
        st.session_state["mst_multi_message"] = "입력 화면에 데이타가 다중 선택되었습니다."
    else:
        st.session_state["mst_multi_message"] = ""

    left_col, right_col = st.columns([4, 6])
    with left_col:
        st.markdown("### 입력 화면")
        if st.session_state["mst_multi_message"]:
            st.warning(st.session_state["mst_multi_message"])
        preview_id = st.session_state["mst_form_id"] or service.get_next_mst_code_id()
        st.text_input("마스터코드ID", value=str(preview_id), disabled=True)
        mst_code = st.text_input("마스터코드", value=st.session_state["mst_form_mst_code_value"], key="mst_input_mst_code")
        code_name = st.text_input("코드명", value=st.session_state["mst_form_code_name_value"], key="mst_input_code_name")
        use_yn = st.selectbox("사용여부", ["Y", "N"], index=0 if st.session_state["mst_form_use_yn_value"] == "Y" else 1, key="mst_input_use_yn")
        sort_order = st.number_input("정렬순서", min_value=0, step=1, value=int(st.session_state["mst_form_sort_order_value"]), key="mst_input_sort_order")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("저장", width="stretch"):
                try:
                    message = service.save_mst_code({
                        "mst_code_id": st.session_state["mst_form_id"],
                        "mst_code": mst_code,
                        "code_name": code_name,
                        "use_yn": use_yn,
                        "sort_order": int(sort_order),
                        "created_by": "system",
                        "updated_by": "system",
                        "ip_address": "",
                        "mac_address": "",
                    })
                    st.success(message)
                    _clear_form(True)
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        with b2:
            if st.button("초기화", width="stretch"):
                _clear_form(True)
                st.rerun()
        with b3:
            if st.button("선택삭제", width="stretch"):
                try:
                    st.success(service.delete_mst_codes(st.session_state["mst_selected_ids"]))
                    _clear_form(True)
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    with right_col:
        st.markdown("### 리스트 화면")
        s1, s2, s3 = st.columns([5,1,1])
        with s1:
            search_value = st.text_input("검색", value=st.session_state["mst_search_keyword"], placeholder="마스터코드 또는 코드명 검색", key="mst_search_input")
        with s2:
            if st.button("조회", width="stretch"):
                st.session_state["mst_search_keyword"] = search_value
                _clear_form(True)
                st.rerun()
        with s3:
            if st.button("선택해제", width="stretch"):
                _clear_form(True)
                st.rerun()

        if not df.empty:
            display_df = df[["mst_code_id", "mst_code", "code_name", "use_yn", "sort_order"]].copy()
            display_df.insert(0, "선택", display_df["mst_code_id"].isin(st.session_state["mst_selected_ids"]))
            t1, t2 = st.columns([1,1])
            with t1:
                if st.button("전체선택", width="stretch"):
                    st.session_state["mst_selected_ids"] = display_df["mst_code_id"].tolist()
                    st.session_state["mst_last_loaded_id"] = None
                    st.rerun()
            with t2:
                if st.button("입력초기화", width="stretch"):
                    _clear_form(True)
                    st.rerun()
            edited_df = st.data_editor(
                display_df,
                width='stretch',
                hide_index=True,
                disabled=["mst_code_id", "mst_code", "code_name", "use_yn", "sort_order"],
                column_config={
                    "선택": st.column_config.CheckboxColumn("선택"),
                    "mst_code_id": st.column_config.NumberColumn("마스터코드ID", format="%d"),
                    "mst_code": "마스터코드",
                    "code_name": "코드명",
                    "use_yn": "사용여부",
                    "sort_order": st.column_config.NumberColumn("정렬순서", format="%d"),
                },
                key="mst_code_editor",
            )
            current_selected_ids = edited_df.loc[edited_df["선택"] == True, "mst_code_id"].tolist()
            if current_selected_ids != st.session_state["mst_selected_ids"]:
                st.session_state["mst_selected_ids"] = current_selected_ids
                st.session_state["mst_last_loaded_id"] = None
                st.rerun()
        else:
            st.info("조회된 데이터가 없습니다.")
