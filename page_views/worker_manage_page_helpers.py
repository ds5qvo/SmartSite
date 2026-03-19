# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 작업자 등록관리 화면 Helper
파일경로 : page_views/worker_manage_page_helpers.py
기능설명 : 작업자 등록관리 화면의 목록, 입력영역, 위젯, 다운로드 렌더링과 버튼 이벤트 처리를 담당한다.
작성일시 : 2026-03-17 16:40:00
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from page_views.worker_manage_page_state import (
    apply_widget_sync_before_render,
    clear_worker_data_state,
    collect_worker_bundle_from_data_state,
    copy_widget_number_to_data,
    copy_widget_to_data,
    data_key,
    load_worker_bundle_to_data_state,
    schedule_search_reset,
    set_message,
    widget_key,
)
from services.worker_manage_service import (
    delete_worker_bundle,
    get_dtl_code_options,
    get_next_worker_code,
    get_worker_bundle,
    get_worker_list,
    normalize_birth_date_text,
    normalize_date_text,
    normalize_phone_text,
    normalize_resident_no_text,
    save_worker_bundle,
)


WORKER_CODE_MST_MAP: Dict[str, str] = {
    "gender_code_id": "GENDER",
    "department_code_id": "DEPARTMENT",
    "position_code_id": "POSITION",
    "employment_type_code_id": "EMPLOYMENT_TYPE",
    "use_yn_code_id": "USE_YN",
    "nationality_code_id": "NATIONALITY",
    "blood_type_code_id": "BLOOD_TYPE",
    "bank_code_id": "BANK",
    "account_owner_code_id": "ACCOUNT_OWNER",
    "exam_institution_code_id": "MEDICAL_INSTITUTION",
    "exam_request_company_code_id": "MEDICAL_COMPANY",
    "exam_result_code_id": "MEDICAL_RESULT",
}

DEFAULT_SORT1_FIELDS = [
    "department_code_id",
    "position_code_id",
    "employment_type_code_id",
    "gender_code_id",
    "use_yn_code_id",
    "nationality_code_id",
    "blood_type_code_id",
    "bank_code_id",
    "account_owner_code_id",
    "exam_institution_code_id",
    "exam_request_company_code_id",
    "exam_result_code_id",
]

DOWNLOAD_COLUMN_NAME_MAP: Dict[str, str] = {
    "worker_id": "작업자ID",
    "worker_code": "사번",
    "worker_name": "성명",
    "birth_date": "생년월일",
    "gender_name": "성별",
    "phone": "연락처",
    "hire_date": "입사일자",
    "department_name": "부서",
    "position_name": "직급",
    "employment_type_name": "고용형태",
    "use_yn_name": "사용여부",
    "resident_no": "주민등록번호",
    "nationality_name": "국적",
    "blood_type_name": "혈액형",
    "safety_shoes_size": "안전화사이즈",
    "vehicle_no": "차량번호",
    "zip_code": "우편번호",
    "emergency_contact": "비상연락처",
    "address": "주소",
    "bank_name": "은행명",
    "account_no": "계좌번호",
    "account_owner_name": "예금주구분",
    "account_holder_name": "예금주",
    "exam_date": "검진일",
    "exam_institution_name": "검진기관",
    "exam_request_company_name": "의뢰업체",
    "exam_result_name": "검진결과",
    "file_name": "파일명",
    "file_path": "파일경로",
    "file_type": "파일유형",
    "created_by": "등록자",
    "created_at": "등록일시",
    "updated_by": "수정자",
    "updated_at": "수정일시",
}


@st.cache_data(show_spinner=False)
def _cached_get_dtl_code_options(cache_token: str, mst_code: str, _conn) -> List[Dict[str, Any]]:
    return get_dtl_code_options(_conn, mst_code)


def load_code_options_map(conn) -> Dict[str, List[Dict[str, Any]]]:
    options_map: Dict[str, List[Dict[str, Any]]] = {}
    cache_token = "smartsite_worker_manage_code_options_v6"

    for field_name, mst_code in WORKER_CODE_MST_MAP.items():
        options_map[field_name] = _cached_get_dtl_code_options(cache_token, mst_code, conn) or []

    return options_map


def render_worker_list_area(conn, code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    st.markdown("### 작업자 목록")

    _render_list_search_area(code_options_map)

    btn_col1, btn_col2, btn_col3, _ = st.columns([1, 1, 1, 3])

    with btn_col1:
        if st.button("조회", width="stretch"):
            set_message("")
            st.rerun()

    with btn_col2:
        if st.button("전체선택", width="stretch"):
            df_all = _get_worker_list_df(conn, code_options_map)
            st.session_state["wm_selected_worker_ids"] = df_all["worker_id"].tolist() if not df_all.empty else []
            set_message("")
            st.rerun()

    with btn_col3:
        if st.button("선택해제", width="stretch"):
            st.session_state["wm_selected_worker_ids"] = []
            st.session_state["wm_selected_worker_id"] = None
            set_message("")
            st.rerun()

    worker_df = _get_worker_list_df(conn, code_options_map)

    if worker_df.empty:
        st.info("조회된 작업자 데이터가 없습니다.")
        _render_download_area(worker_df)
        return

    edited_df = st.data_editor(
        worker_df,
        key="wm_list_editor",
        hide_index=True,
        width="stretch",
        height=480,
        disabled=[c for c in worker_df.columns if c != "__selected"],
        column_config={
            "__selected": st.column_config.CheckboxColumn("선택"),
            "worker_id": st.column_config.NumberColumn("ID"),
            "worker_code": st.column_config.TextColumn("사번"),
            "worker_name": st.column_config.TextColumn("성명"),
            "birth_date": st.column_config.TextColumn("생년월일"),
            "gender_name": st.column_config.TextColumn("성별"),
            "phone": st.column_config.TextColumn("연락처"),
            "hire_date": st.column_config.TextColumn("입사일자"),
            "department_name": st.column_config.TextColumn("부서"),
            "position_name": st.column_config.TextColumn("직급"),
            "employment_type_name": st.column_config.TextColumn("고용형태"),
            "use_yn_name": st.column_config.TextColumn("사용여부"),
            "resident_no": st.column_config.TextColumn("주민등록번호"),
            "nationality_name": st.column_config.TextColumn("국적"),
            "blood_type_name": st.column_config.TextColumn("혈액형"),
            "safety_shoes_size": st.column_config.NumberColumn("안전화사이즈"),
            "vehicle_no": st.column_config.TextColumn("차량번호"),
            "zip_code": st.column_config.TextColumn("우편번호"),
            "emergency_contact": st.column_config.TextColumn("비상연락처"),
            "address": st.column_config.TextColumn("주소"),
            "bank_name": st.column_config.TextColumn("은행명"),
            "account_no": st.column_config.TextColumn("계좌번호"),
            "account_owner_name": st.column_config.TextColumn("예금주구분"),
            "account_holder_name": st.column_config.TextColumn("예금주"),
            "exam_date": st.column_config.TextColumn("검진일"),
            "exam_institution_name": st.column_config.TextColumn("검진기관"),
            "exam_request_company_name": st.column_config.TextColumn("의뢰업체"),
            "exam_result_name": st.column_config.TextColumn("검진결과"),
            "file_name": st.column_config.TextColumn("파일명"),
            "file_path": st.column_config.TextColumn("파일경로"),
            "file_type": st.column_config.TextColumn("파일유형"),
            "created_by": st.column_config.TextColumn("등록자"),
            "created_at": st.column_config.TextColumn("등록일시"),
            "updated_by": st.column_config.TextColumn("수정자"),
            "updated_at": st.column_config.TextColumn("수정일시"),
        },
    )

    current_selected_ids = []
    if isinstance(edited_df, pd.DataFrame) and not edited_df.empty:
        current_selected_ids = edited_df.loc[edited_df["__selected"] == True, "worker_id"].tolist()

    previous_selected_ids = st.session_state.get("wm_selected_worker_ids", [])

    if current_selected_ids != previous_selected_ids:
        st.session_state["wm_selected_worker_ids"] = current_selected_ids

        if len(current_selected_ids) == 1:
            selected_worker_id = current_selected_ids[0]
            bundle = get_worker_bundle(conn, selected_worker_id) or {}
            load_worker_bundle_to_data_state(bundle)
            st.session_state["wm_mode"] = "edit"
            st.session_state["wm_selected_worker_id"] = selected_worker_id
            st.session_state["wm_need_widget_sync"] = True
            st.session_state["wm_pending_apply_new_defaults"] = False
            set_message("")
            st.rerun()

        if len(current_selected_ids) == 0:
            st.session_state["wm_selected_worker_id"] = None
            set_message("")
            st.rerun()

        if len(current_selected_ids) >= 2:
            st.session_state["wm_selected_worker_id"] = None
            set_message("입력 화면에 데이타가 다중 선택되었습니다.")
            st.rerun()

    if st.session_state.get("wm_message"):
        st.warning(st.session_state["wm_message"])

    _render_download_area(worker_df)


def render_worker_editor_area(conn, code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    st.markdown("### 작업자 입력")

    btn_col1, btn_col2, btn_col3, _ = st.columns([1, 1, 1, 2])

    with btn_col1:
        if st.button("신규", width="stretch"):
            _handle_new_action()
            st.rerun()

    with btn_col2:
        if st.button("저장", width="stretch"):
            _handle_save_action(conn, code_options_map)
            st.rerun()

    with btn_col3:
        if st.button("삭제", width="stretch"):
            _handle_delete_action(conn)
            st.rerun()

    selected_ids = st.session_state.get("wm_selected_worker_ids", [])
    if len(selected_ids) >= 2:
        st.warning("입력 화면에 데이타가 다중 선택되었습니다.")
        return

    _apply_new_defaults_if_needed(conn, code_options_map)
    _apply_account_holder_default_if_needed(code_options_map)
    apply_widget_sync_before_render()

    tab1, tab2, tab3, tab4 = st.tabs(["기본정보", "상세정보", "급여계좌", "특수검진"])

    with tab1:
        _render_basic_tab(code_options_map)
    with tab2:
        _render_detail_tab(code_options_map)
    with tab3:
        _render_account_tab(code_options_map)
    with tab4:
        _render_medical_tab(code_options_map)


def _handle_new_action() -> None:
    clear_worker_data_state()
    st.session_state["wm_mode"] = "new"
    st.session_state["wm_selected_worker_id"] = None
    st.session_state["wm_selected_worker_ids"] = []
    st.session_state["wm_download_expanded"] = False
    st.session_state["wm_download_selected_fields"] = []
    st.session_state["wm_need_widget_sync"] = True
    st.session_state["wm_pending_apply_new_defaults"] = True

    schedule_search_reset()
    set_message("신규 입력 상태로 전환되었습니다.")


def _handle_save_action(conn, code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    payload = collect_worker_bundle_from_data_state()

    owner_options = code_options_map.get("account_owner_code_id", [])
    owner_name = _option_name_by_id(owner_options, payload.get("account_owner_code_id"))
    if owner_name == "본인" and str(payload.get("worker_name") or "").strip():
        payload["account_holder_name"] = str(payload.get("worker_name") or "").strip()

    if not str(payload.get("worker_code", "")).strip():
        set_message("사번은 필수입니다.")
        return

    if not str(payload.get("worker_name", "")).strip():
        set_message("성명은 필수입니다.")
        return

    try:
        save_result = save_worker_bundle(conn, payload)
        saved_worker_id = save_result.get("worker_id") if isinstance(save_result, dict) else save_result

        if not saved_worker_id:
            set_message("저장 처리 결과에서 작업자 ID를 확인할 수 없습니다.")
            return

        bundle = get_worker_bundle(conn, saved_worker_id) or {}
        load_worker_bundle_to_data_state(bundle)

        st.session_state["wm_mode"] = "edit"
        st.session_state["wm_selected_worker_id"] = saved_worker_id
        st.session_state["wm_selected_worker_ids"] = [saved_worker_id]
        st.session_state["wm_need_widget_sync"] = True
        st.session_state["wm_pending_apply_new_defaults"] = False
        set_message("저장되었습니다.")

    except Exception as exc:
        set_message(f"저장 중 오류가 발생했습니다: {str(exc)}")


def _handle_delete_action(conn) -> None:
    selected_worker_id = st.session_state.get("wm_selected_worker_id")

    if selected_worker_id in (None, "", 0):
        set_message("삭제할 작업자를 먼저 1건 선택하세요.")
        return

    try:
        delete_worker_bundle(conn, int(selected_worker_id))

        clear_worker_data_state()
        st.session_state["wm_mode"] = "new"
        st.session_state["wm_selected_worker_id"] = None
        st.session_state["wm_selected_worker_ids"] = []
        st.session_state["wm_need_widget_sync"] = True
        st.session_state["wm_pending_apply_new_defaults"] = True

        schedule_search_reset()
        set_message("삭제되었습니다.")

    except Exception as exc:
        set_message(f"삭제 중 오류가 발생했습니다: {str(exc)}")


def _apply_new_defaults_if_needed(conn, code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    if st.session_state.get("wm_mode") != "new":
        return

    if not st.session_state.get("wm_pending_apply_new_defaults", False):
        return

    worker_code = st.session_state.get(data_key("worker_code"))
    if not str(worker_code or "").strip():
        st.session_state[data_key("worker_code")] = get_next_worker_code(conn)

    for field_name in DEFAULT_SORT1_FIELDS:
        current_value = st.session_state.get(data_key(field_name))
        options = code_options_map.get(field_name, [])
        if current_value in (None, "", 0) and options:
            st.session_state[data_key(field_name)] = options[0].get("dtl_code_id")

    st.session_state["wm_pending_apply_new_defaults"] = False
    st.session_state["wm_need_widget_sync"] = True


def _apply_account_holder_default_if_needed(code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    owner_code_id = st.session_state.get(data_key("account_owner_code_id"))
    worker_name = st.session_state.get(data_key("worker_name"), "")
    current_holder = st.session_state.get(data_key("account_holder_name"), "")

    owner_options = code_options_map.get("account_owner_code_id", [])
    owner_name = _option_name_by_id(owner_options, owner_code_id)

    if owner_name == "본인" and str(worker_name).strip():
        if current_holder != worker_name:
            st.session_state[data_key("account_holder_name")] = worker_name
            st.session_state["wm_need_widget_sync"] = True


def _render_list_search_area(code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.text_input("사번", key="wm_search_worker_code", placeholder="사번 검색")

    with col2:
        st.text_input("성명", key="wm_search_worker_name", placeholder="성명 검색")

    with col3:
        options = code_options_map.get("department_code_id", [])
        option_ids = [None] + [opt.get("dtl_code_id") for opt in options]
        option_labels = ["전체"] + [str(opt.get("code_name", "")) for opt in options]

        current_code_id = st.session_state.get("wm_search_department_code_id")
        current_index = option_ids.index(current_code_id) if current_code_id in option_ids else 0

        selected_label = st.selectbox(
            "부서",
            options=option_labels,
            index=current_index,
            key="wm_search_department_label",
        )
        selected_index = option_labels.index(selected_label)
        st.session_state["wm_search_department_code_id"] = option_ids[selected_index]


def _get_worker_list_df(conn, code_options_map: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    search_params = {
        "worker_code": st.session_state.get("wm_search_worker_code", "").strip(),
        "worker_name": st.session_state.get("wm_search_worker_name", "").strip(),
        "department_code_id": st.session_state.get("wm_search_department_code_id"),
    }

    rows = get_worker_list(conn, search_params) or []
    df = pd.DataFrame(rows)

    list_columns = [
        "__selected",
        "worker_id",
        "worker_code",
        "worker_name",
        "birth_date",
        "gender_name",
        "phone",
        "hire_date",
        "department_name",
        "position_name",
        "employment_type_name",
        "use_yn_name",
        "resident_no",
        "nationality_name",
        "blood_type_name",
        "safety_shoes_size",
        "vehicle_no",
        "zip_code",
        "emergency_contact",
        "address",
        "bank_name",
        "account_no",
        "account_owner_name",
        "account_holder_name",
        "exam_date",
        "exam_institution_name",
        "exam_request_company_name",
        "exam_result_name",
        "file_name",
        "file_path",
        "file_type",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
    ]

    if df.empty:
        return pd.DataFrame(columns=list_columns)

    for field_name in [
        "gender_code_id",
        "department_code_id",
        "position_code_id",
        "employment_type_code_id",
        "use_yn_code_id",
        "nationality_code_id",
        "blood_type_code_id",
        "bank_code_id",
        "account_owner_code_id",
        "exam_institution_code_id",
        "exam_request_company_code_id",
        "exam_result_code_id",
    ]:
        if field_name in df.columns:
            options = code_options_map.get(field_name, [])
            name_col = {
                "bank_code_id": "bank_name",
                "account_owner_code_id": "account_owner_name",
                "exam_institution_code_id": "exam_institution_name",
                "exam_request_company_code_id": "exam_request_company_name",
                "exam_result_code_id": "exam_result_name",
            }.get(field_name, field_name.replace("_code_id", "_name"))
            df[name_col] = df[field_name].apply(lambda x: _option_name_by_id(options, x))

    if "account_owner_name" in df.columns and "worker_name" in df.columns:
        df["account_holder_name"] = df.apply(
            lambda row: row.get("worker_name", "")
            if str(row.get("account_owner_name", "")).strip() == "본인" and not str(row.get("account_holder_name", "") or "").strip()
            else row.get("account_holder_name", ""),
            axis=1,
        )

    selected_ids = set(st.session_state.get("wm_selected_worker_ids", []))
    df["__selected"] = df["worker_id"].apply(lambda x: x in selected_ids)

    for col in list_columns:
        if col not in df.columns:
            df[col] = False if col == "__selected" else ""

    return df[[c for c in list_columns if c in df.columns]].copy()


def _option_name_by_id(options: List[Dict[str, Any]], dtl_code_id: Any) -> str:
    if dtl_code_id in (None, "", 0):
        return ""
    for option in options:
        if option.get("dtl_code_id") == dtl_code_id:
            return str(option.get("code_name", ""))
    return ""


def _render_basic_tab(code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    col1, col2 = st.columns(2)
    with col1:
        _render_text_input("사번", "worker_code")
        _render_text_input("성명", "worker_name")
        _render_birth_date_input("생년월일", "birth_date")
        _render_selectbox("성별", "gender_code_id", code_options_map.get("gender_code_id", []))
        _render_phone_input("연락처", "phone")
    with col2:
        _render_date_text_input("입사일자", "hire_date")
        _render_selectbox("부서", "department_code_id", code_options_map.get("department_code_id", []))
        _render_selectbox("직급", "position_code_id", code_options_map.get("position_code_id", []))
        _render_selectbox("고용형태", "employment_type_code_id", code_options_map.get("employment_type_code_id", []))
        _render_selectbox("사용여부", "use_yn_code_id", code_options_map.get("use_yn_code_id", []))


def _render_detail_tab(code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    col1, col2 = st.columns(2)
    with col1:
        _render_resident_no_input("주민등록번호", "resident_no")
        _render_selectbox("국적", "nationality_code_id", code_options_map.get("nationality_code_id", []))
        _render_selectbox("혈액형", "blood_type_code_id", code_options_map.get("blood_type_code_id", []))
        _render_number_input("안전화사이즈", "safety_shoes_size")
    with col2:
        _render_text_input("차량번호", "vehicle_no")
        _render_text_input("우편번호", "zip_code")
        _render_phone_input("비상연락처", "emergency_contact")
        _render_text_area("주소", "address", height=100)


def _render_account_tab(code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    col1, col2 = st.columns(2)
    with col1:
        _render_selectbox("은행명", "bank_code_id", code_options_map.get("bank_code_id", []))
        _render_text_input("계좌번호", "account_no")
    with col2:
        _render_selectbox("예금주구분", "account_owner_code_id", code_options_map.get("account_owner_code_id", []))
        _render_account_holder_input("예금주", "account_holder_name", code_options_map)


def _render_medical_tab(code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    col1, col2 = st.columns(2)
    with col1:
        _render_medical_date_input("검진일", "exam_date")
        _render_selectbox("검진기관", "exam_institution_code_id", code_options_map.get("exam_institution_code_id", []))
        _render_selectbox("의뢰업체", "exam_request_company_code_id", code_options_map.get("exam_request_company_code_id", []))
        _render_selectbox("검진결과", "exam_result_code_id", code_options_map.get("exam_result_code_id", []))
        _render_medical_upload("검진표 업로드")
    with col2:
        _render_text_input("파일명", "file_name")
        _render_text_input("파일경로", "file_path")
        _render_text_input("파일유형", "file_type")


def _render_text_input(label: str, field_name: str, placeholder: str | None = None) -> None:
    st.text_input(
        label,
        key=widget_key(field_name),
        placeholder=placeholder,
        on_change=copy_widget_to_data,
        args=(field_name,),
    )


def _render_text_area(label: str, field_name: str, height: int = 100) -> None:
    st.text_area(
        label,
        key=widget_key(field_name),
        height=height,
        on_change=copy_widget_to_data,
        args=(field_name,),
    )


def _render_date_text_input(label: str, field_name: str) -> None:
    st.text_input(
        label,
        key=widget_key(field_name),
        placeholder="YYYY-MM-DD",
        on_change=_normalize_general_date_widget,
        args=(field_name,),
    )


def _render_birth_date_input(label: str, field_name: str) -> None:
    st.text_input(
        label,
        key=widget_key(field_name),
        placeholder="YYYY-MM-DD 또는 YYYYMMDD",
        on_change=_normalize_birth_date_widget,
        args=(field_name,),
    )


def _render_medical_date_input(label: str, field_name: str) -> None:
    st.text_input(
        label,
        key=widget_key(field_name),
        placeholder="YYYY-MM-DD 또는 YYYYMMDD",
        on_change=_normalize_general_date_widget,
        args=(field_name,),
    )


def _normalize_birth_date_widget(field_name: str) -> None:
    raw_value = st.session_state.get(widget_key(field_name))
    normalized = normalize_birth_date_text(raw_value)

    if normalized is None:
        normalized = ""

    st.session_state[widget_key(field_name)] = normalized
    st.session_state[data_key(field_name)] = normalized


def _normalize_general_date_widget(field_name: str) -> None:
    raw_value = st.session_state.get(widget_key(field_name))
    normalized = normalize_date_text(raw_value)

    if normalized is None:
        normalized = ""

    st.session_state[widget_key(field_name)] = normalized
    st.session_state[data_key(field_name)] = normalized


def _render_phone_input(label: str, field_name: str) -> None:
    st.text_input(
        label,
        key=widget_key(field_name),
        placeholder="숫자만 입력 가능",
        on_change=_normalize_phone_widget,
        args=(field_name,),
    )


def _normalize_phone_widget(field_name: str) -> None:
    raw_value = st.session_state.get(widget_key(field_name))
    normalized = normalize_phone_text(raw_value)

    if normalized is None:
        normalized = ""

    st.session_state[widget_key(field_name)] = normalized
    st.session_state[data_key(field_name)] = normalized


def _render_resident_no_input(label: str, field_name: str) -> None:
    st.text_input(
        label,
        key=widget_key(field_name),
        placeholder="13자리 숫자 입력",
        on_change=_normalize_resident_no_widget,
        args=(field_name,),
    )


def _normalize_resident_no_widget(field_name: str) -> None:
    raw_value = st.session_state.get(widget_key(field_name))
    normalized = normalize_resident_no_text(raw_value)

    if normalized is None:
        normalized = ""

    st.session_state[widget_key(field_name)] = normalized
    st.session_state[data_key(field_name)] = normalized


def _render_account_holder_input(label: str, field_name: str, code_options_map: Dict[str, List[Dict[str, Any]]]) -> None:
    owner_code_id = st.session_state.get(data_key("account_owner_code_id"))
    owner_options = code_options_map.get("account_owner_code_id", [])
    owner_name = _option_name_by_id(owner_options, owner_code_id)
    disabled_flag = owner_name == "본인"

    st.text_input(
        label,
        key=widget_key(field_name),
        disabled=disabled_flag,
        on_change=copy_widget_to_data,
        args=(field_name,),
    )


def _render_medical_upload(label: str) -> None:
    uploaded_file = st.file_uploader(label, key="wm_medical_upload_file")

    if uploaded_file is None:
        return

    base_dir = Path("G:/내 드라이브/SmartSite/uploads/worker_medical")
    base_dir.mkdir(parents=True, exist_ok=True)

    save_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uploaded_file.name}"
    save_path = base_dir / save_name

    with open(save_path, "wb") as fp:
        fp.write(uploaded_file.getbuffer())

    st.session_state[data_key("file_name")] = uploaded_file.name
    st.session_state[data_key("file_path")] = str(save_path).replace("\\", "/")
    suffix = Path(uploaded_file.name).suffix.replace(".", "").lower()
    st.session_state[data_key("file_type")] = suffix

    st.session_state["wm_need_widget_sync"] = True
    st.success("검진표 파일이 업로드되었습니다.")


def _render_number_input(label: str, field_name: str) -> None:
    if st.session_state.get(widget_key(field_name)) in (None, "", "None"):
        st.session_state[widget_key(field_name)] = 0

    st.number_input(
        label,
        min_value=0,
        step=1,
        key=widget_key(field_name),
        on_change=copy_widget_number_to_data,
        args=(field_name,),
    )


def _render_selectbox(label: str, field_name: str, options: List[Dict[str, Any]]) -> None:
    current_value = st.session_state.get(widget_key(field_name))
    option_ids = [None] + [opt.get("dtl_code_id") for opt in options]

    if current_value not in option_ids:
        st.session_state[widget_key(field_name)] = None

    st.selectbox(
        label,
        options=option_ids,
        key=widget_key(field_name),
        format_func=lambda x: "선택" if x in (None, "") else _option_name_by_id(options, x),
        on_change=copy_widget_to_data,
        args=(field_name,),
    )


def _render_download_area(worker_df: pd.DataFrame) -> None:
    expanded = st.session_state.get("wm_download_expanded", False)

    with st.expander("엑셀 저장", expanded=expanded):
        st.session_state["wm_download_expanded"] = True

        available_columns = [col for col in worker_df.columns if col != "__selected" and col in DOWNLOAD_COLUMN_NAME_MAP]
        default_fields = available_columns[:]
        selected_saved = st.session_state.get("wm_download_selected_fields", []) or default_fields

        selected_fields = st.multiselect(
            "다운로드 필드",
            options=available_columns,
            default=[col for col in selected_saved if col in available_columns],
            format_func=lambda x: DOWNLOAD_COLUMN_NAME_MAP.get(x, x),
            key="wm_download_fields_multiselect",
        )
        st.session_state["wm_download_selected_fields"] = selected_fields

        if worker_df.empty or not selected_fields:
            return

        download_df = worker_df[selected_fields].copy()
        download_df.rename(columns=DOWNLOAD_COLUMN_NAME_MAP, inplace=True)

        file_name = f"작업자등록관리_{datetime.now().strftime('%Y%m%d %H%M%S')}.xlsx"
        excel_bytes = _convert_df_to_excel_bytes(download_df)

        st.download_button(
            label="엑셀 저장",
            data=excel_bytes,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
        )


def _convert_df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="작업자목록")
    return output.getvalue()