# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 작업자 등록관리 화면 상태관리
파일경로 : page_views/worker_manage_page_state.py
기능설명 : 작업자 등록관리 화면의 session_state 초기화, reset 예약, widget/data 상태 동기화를 담당한다.
작성일시 : 2026-03-17 16:10:00
"""

from __future__ import annotations

from typing import Any, Dict

import streamlit as st


def empty_worker_bundle() -> Dict[str, Any]:
    return {
        "worker_id": None,
        "worker_code": "",
        "worker_name": "",
        "birth_date": "",
        "gender_code_id": None,
        "phone": "",
        "hire_date": "",
        "department_code_id": None,
        "position_code_id": None,
        "employment_type_code_id": None,
        "use_yn_code_id": None,
        "worker_detail_id": None,
        "resident_no": "",
        "nationality_code_id": None,
        "blood_type_code_id": None,
        "safety_shoes_size": None,
        "vehicle_no": "",
        "address": "",
        "zip_code": "",
        "emergency_contact": "",
        "worker_account_id": None,
        "bank_code_id": None,
        "account_no": "",
        "account_holder_name": "",
        "account_owner_code_id": None,
        "medical_file_id": None,
        "exam_date": "",
        "exam_institution_code_id": None,
        "exam_request_company_code_id": None,
        "exam_result_code_id": None,
        "file_name": "",
        "file_path": "",
        "file_type": "",
    }


def data_key(field_name: str) -> str:
    return f"wm_data_{field_name}"


def widget_key(field_name: str) -> str:
    return f"wm_widget_{field_name}"


def initialize_worker_manage_state() -> None:
    defaults: Dict[str, Any] = {
        "wm_mode": "new",
        "wm_selected_worker_id": None,
        "wm_selected_worker_ids": [],
        "wm_search_worker_code": "",
        "wm_search_worker_name": "",
        "wm_search_department_code_id": None,
        "wm_search_department_label": "전체",
        "wm_download_expanded": False,
        "wm_download_selected_fields": [],
        "wm_need_widget_sync": True,
        "wm_message": "",
        "wm_pending_reset_search": False,
        "wm_pending_apply_new_defaults": True,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    for field_name, default_value in empty_worker_bundle().items():
        key = data_key(field_name)
        if key not in st.session_state:
            st.session_state[key] = default_value


def clear_worker_data_state() -> None:
    for field_name, default_value in empty_worker_bundle().items():
        st.session_state[data_key(field_name)] = default_value


def collect_worker_bundle_from_data_state() -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for field_name in empty_worker_bundle().keys():
        payload[field_name] = st.session_state.get(data_key(field_name))
    return payload


def load_worker_bundle_to_data_state(bundle: Dict[str, Any]) -> None:
    clear_worker_data_state()

    for field_name, default_value in empty_worker_bundle().items():
        value = bundle.get(field_name, default_value)
        if value is None and field_name in {"birth_date", "hire_date", "exam_date"}:
            value = ""
        st.session_state[data_key(field_name)] = value


def apply_widget_sync_before_render() -> None:
    if not st.session_state.get("wm_need_widget_sync", False):
        return

    for field_name in empty_worker_bundle().keys():
        st.session_state[widget_key(field_name)] = st.session_state.get(data_key(field_name))

    st.session_state["wm_need_widget_sync"] = False


def copy_widget_to_data(field_name: str) -> None:
    st.session_state[data_key(field_name)] = st.session_state.get(widget_key(field_name))


def copy_widget_number_to_data(field_name: str) -> None:
    value = st.session_state.get(widget_key(field_name))
    if value in (None, "", "None"):
        st.session_state[data_key(field_name)] = None
        return

    try:
        numeric_value = int(value)
        st.session_state[data_key(field_name)] = None if numeric_value == 0 else numeric_value
    except (TypeError, ValueError):
        st.session_state[data_key(field_name)] = None


def set_message(message: str) -> None:
    st.session_state["wm_message"] = message


def schedule_search_reset() -> None:
    st.session_state["wm_pending_reset_search"] = True


def apply_pending_resets() -> None:
    if st.session_state.get("wm_pending_reset_search", False):
        st.session_state["wm_search_worker_code"] = ""
        st.session_state["wm_search_worker_name"] = ""
        st.session_state["wm_search_department_code_id"] = None
        st.session_state["wm_search_department_label"] = "전체"
        st.session_state["wm_pending_reset_search"] = False