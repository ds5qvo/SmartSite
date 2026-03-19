# -*- coding: utf-8 -*-
"""
프로그램명 : code_manage_page.py
파일경로   : page_views/code_manage_page.py
기능설명   : SmartSite 코드관리 화면
화면설명   :
    - 상위코드 / 하위코드 관리 화면
    - 첫 클릭 즉시 반응 구조
    - widget key 와 실제 data key 분리로 Streamlit session_state 충돌 방지
    - dtl_code.remark 절대 사용 금지
작성일시   : 2026-03-17
작성자     : ChatGPT
변경이력   :
    - 2026-03-17 : widget state 충돌 오류 수정
주의사항   :
    - DB 연결은 app.py 에서만 수행
    - conn 은 page -> service -> repository 로 전달
"""

from __future__ import annotations

from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Sequence

import pandas as pd
import streamlit as st

try:
    import services.code_manage_service as code_service
except Exception as import_error:
    code_service = None
    _IMPORT_ERROR = import_error
else:
    _IMPORT_ERROR = None


# ============================================================
# 상수
# ============================================================
MASTER_FIELD_LABELS: Dict[str, str] = {
    "mst_code_id": "상위코드ID",
    "mst_code": "상위코드",
    "code_name": "상위코드명",
    "use_yn": "사용여부",
    "sort_order": "정렬순서",
    "ip_address": "IP주소",
    "mac_address": "MAC주소",
    "created_by": "등록자",
    "created_at": "등록일시",
    "updated_by": "수정자",
    "updated_at": "수정일시",
}

DETAIL_FIELD_LABELS: Dict[str, str] = {
    "dtl_code_id": "하위코드ID",
    "mst_code_id": "상위코드ID",
    "mst_code": "상위코드",
    "mst_code_name": "상위코드명",
    "dtl_code": "하위코드",
    "code_name": "하위코드명",
    "use_yn": "사용여부",
    "sort_order": "정렬순서",
    "ip_address": "IP주소",
    "mac_address": "MAC주소",
    "created_by": "등록자",
    "created_at": "등록일시",
    "updated_by": "수정자",
    "updated_at": "수정일시",
}

MASTER_GRID_COLUMNS: List[str] = [
    "mst_code_id",
    "mst_code",
    "code_name",
    "use_yn",
    "sort_order",
    "ip_address",
    "mac_address",
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
]

DETAIL_GRID_COLUMNS: List[str] = [
    "dtl_code_id",
    "mst_code_id",
    "mst_code",
    "mst_code_name",
    "dtl_code",
    "code_name",
    "use_yn",
    "sort_order",
    "ip_address",
    "mac_address",
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
]


# ============================================================
# 공통 유틸
# ============================================================
def _ensure_service_loaded() -> None:
    if code_service is None:
        raise RuntimeError(f"services.code_manage_service import 실패: {_IMPORT_ERROR}")


def _resolve_service_function(candidates: Sequence[str]) -> Callable[..., Any]:
    _ensure_service_loaded()
    for func_name in candidates:
        func = getattr(code_service, func_name, None)
        if callable(func):
            return func
    raise AttributeError(f"code_manage_service 함수 없음: {', '.join(candidates)}")


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(value)
    except Exception:
        return default


def _normalize_yes_no(value: Any, default: str = "Y") -> str:
    value_str = _to_str(value).strip().upper()
    return value_str if value_str in {"Y", "N"} else default


def _normalize_records(data: Any) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient="records")
    if isinstance(data, list):
        result: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                result.append(item)
            else:
                result.append(dict(item))
        return result
    try:
        return pd.DataFrame(data).to_dict(orient="records")
    except Exception:
        return []


def _make_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def _rename_for_download(df: pd.DataFrame, label_map: Dict[str, str], selected_labels: List[str]) -> pd.DataFrame:
    reverse_map = {v: k for k, v in label_map.items()}
    selected_columns = [
        reverse_map[label]
        for label in selected_labels
        if label in reverse_map and reverse_map[label] in df.columns
    ]
    if not selected_columns:
        selected_columns = [col for col in label_map.keys() if col in df.columns]
    result_df = df[selected_columns].copy()
    return result_df.rename(columns=label_map)


def _build_master_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=MASTER_GRID_COLUMNS)
    for col in MASTER_GRID_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[MASTER_GRID_COLUMNS].copy()
    df.insert(0, "_checked", False)
    return df


def _build_detail_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=DETAIL_GRID_COLUMNS)
    for col in DETAIL_GRID_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[DETAIL_GRID_COLUMNS].copy()
    df.insert(0, "_checked", False)
    return df


def _show_message(message: str, message_type: str = "info") -> None:
    if not message:
        return
    if message_type == "success":
        st.success(message)
    elif message_type == "warning":
        st.warning(message)
    elif message_type == "error":
        st.error(message)
    else:
        st.info(message)


# ============================================================
# 서비스 호출
# ============================================================
def _get_master_code_list(conn: Any, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    func = _resolve_service_function(
        ["get_master_code_list", "search_master_code_list", "get_master_codes", "select_master_code_list"]
    )
    return _normalize_records(func(conn, search_params))


def _get_detail_code_list(conn: Any, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    func = _resolve_service_function(
        ["get_detail_code_list", "search_detail_code_list", "get_detail_codes", "select_detail_code_list"]
    )
    return _normalize_records(func(conn, search_params))


def _save_master_code(conn: Any, save_data: Dict[str, Any]) -> Any:
    func = _resolve_service_function(
        ["save_master_code", "save_mst_code", "upsert_master_code"]
    )
    return func(conn, save_data)


def _save_detail_code(conn: Any, save_data: Dict[str, Any]) -> Any:
    func = _resolve_service_function(
        ["save_detail_code", "save_dtl_code", "upsert_detail_code"]
    )
    return func(conn, save_data)


# ============================================================
# 세션 초기화
# ============================================================
def _init_state() -> None:
    defaults: Dict[str, Any] = {
        "cm_message": "",
        "cm_message_type": "info",

        "cm_master_mode": "view",
        "cm_detail_mode": "view",

        "cm_master_selected_ids": [],
        "cm_detail_selected_ids": [],
        "cm_detail_parent_selected_mst_ids": [],

        "cm_master_download_open": False,
        "cm_detail_download_open": False,

        "cm_master_search_mst_code": "",
        "cm_master_search_code_name": "",
        "cm_master_search_use_yn": "전체",

        "cm_detail_search_parent_mst_code": "",
        "cm_detail_search_parent_code_name": "",
        "cm_detail_search_dtl_code": "",
        "cm_detail_search_code_name": "",
        "cm_detail_search_use_yn": "전체",

        # master 실제 data
        "cm_master_data_mst_code_id": "",
        "cm_master_data_mst_code": "",
        "cm_master_data_code_name": "",
        "cm_master_data_use_yn": "Y",
        "cm_master_data_sort_order": 0,
        "cm_master_data_ip_address": "",
        "cm_master_data_mac_address": "",

        # master widget
        "cm_master_widget_mst_code_id": "",
        "cm_master_widget_mst_code": "",
        "cm_master_widget_code_name": "",
        "cm_master_widget_use_yn": "Y",
        "cm_master_widget_sort_order": 0,
        "cm_master_widget_ip_address": "",
        "cm_master_widget_mac_address": "",

        # detail parent 실제 data
        "cm_detail_parent_data_mst_code_id": "",
        "cm_detail_parent_data_mst_code": "",
        "cm_detail_parent_data_code_name": "",

        # detail parent widget
        "cm_detail_parent_widget_mst_code_id": "",
        "cm_detail_parent_widget_mst_code": "",
        "cm_detail_parent_widget_code_name": "",

        # detail 실제 data
        "cm_detail_data_dtl_code_id": "",
        "cm_detail_data_mst_code_id": "",
        "cm_detail_data_mst_code": "",
        "cm_detail_data_dtl_code": "",
        "cm_detail_data_code_name": "",
        "cm_detail_data_use_yn": "Y",
        "cm_detail_data_sort_order": 0,
        "cm_detail_data_ip_address": "",
        "cm_detail_data_mac_address": "",

        # detail widget
        "cm_detail_widget_dtl_code_id": "",
        "cm_detail_widget_mst_code_id": "",
        "cm_detail_widget_mst_code": "",
        "cm_detail_widget_dtl_code": "",
        "cm_detail_widget_code_name": "",
        "cm_detail_widget_use_yn": "Y",
        "cm_detail_widget_sort_order": 0,
        "cm_detail_widget_ip_address": "",
        "cm_detail_widget_mac_address": "",

        "cm_master_download_labels": list(MASTER_FIELD_LABELS.values()),
        "cm_detail_download_labels": list(DETAIL_FIELD_LABELS.values()),
        "cm_active_tab": "상위코드",
        "cm_tab_selector": "상위코드",
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ============================================================
# 메시지
# ============================================================
def _set_message(message: str, message_type: str = "info") -> None:
    st.session_state["cm_message"] = message
    st.session_state["cm_message_type"] = message_type


def _clear_message() -> None:
    st.session_state["cm_message"] = ""
    st.session_state["cm_message_type"] = "info"


# ============================================================
# data 상태 제어
# ============================================================
def _clear_master_data() -> None:
    st.session_state["cm_master_data_mst_code_id"] = ""
    st.session_state["cm_master_data_mst_code"] = ""
    st.session_state["cm_master_data_code_name"] = ""
    st.session_state["cm_master_data_use_yn"] = "Y"
    st.session_state["cm_master_data_sort_order"] = 0
    st.session_state["cm_master_data_ip_address"] = ""
    st.session_state["cm_master_data_mac_address"] = ""


def _clear_detail_parent_data() -> None:
    st.session_state["cm_detail_parent_data_mst_code_id"] = ""
    st.session_state["cm_detail_parent_data_mst_code"] = ""
    st.session_state["cm_detail_parent_data_code_name"] = ""


def _clear_detail_data() -> None:
    st.session_state["cm_detail_data_dtl_code_id"] = ""
    st.session_state["cm_detail_data_mst_code_id"] = ""
    st.session_state["cm_detail_data_mst_code"] = ""
    st.session_state["cm_detail_data_dtl_code"] = ""
    st.session_state["cm_detail_data_code_name"] = ""
    st.session_state["cm_detail_data_use_yn"] = "Y"
    st.session_state["cm_detail_data_sort_order"] = 0
    st.session_state["cm_detail_data_ip_address"] = ""
    st.session_state["cm_detail_data_mac_address"] = ""


def _sync_master_data_from_row(row: Dict[str, Any]) -> None:
    st.session_state["cm_master_data_mst_code_id"] = _to_str(row.get("mst_code_id"))
    st.session_state["cm_master_data_mst_code"] = _to_str(row.get("mst_code"))
    st.session_state["cm_master_data_code_name"] = _to_str(row.get("code_name"))
    st.session_state["cm_master_data_use_yn"] = _normalize_yes_no(row.get("use_yn"), "Y")
    st.session_state["cm_master_data_sort_order"] = _safe_int(row.get("sort_order"), 0)
    st.session_state["cm_master_data_ip_address"] = _to_str(row.get("ip_address"))
    st.session_state["cm_master_data_mac_address"] = _to_str(row.get("mac_address"))


def _sync_detail_parent_data_from_row(row: Dict[str, Any]) -> None:
    st.session_state["cm_detail_parent_data_mst_code_id"] = _to_str(row.get("mst_code_id"))
    st.session_state["cm_detail_parent_data_mst_code"] = _to_str(row.get("mst_code"))
    st.session_state["cm_detail_parent_data_code_name"] = _to_str(row.get("code_name"))


def _sync_detail_data_from_row(row: Dict[str, Any]) -> None:
    st.session_state["cm_detail_data_dtl_code_id"] = _to_str(row.get("dtl_code_id"))
    st.session_state["cm_detail_data_mst_code_id"] = _to_str(row.get("mst_code_id"))
    st.session_state["cm_detail_data_mst_code"] = _to_str(row.get("mst_code"))
    st.session_state["cm_detail_data_dtl_code"] = _to_str(row.get("dtl_code"))
    st.session_state["cm_detail_data_code_name"] = _to_str(row.get("code_name"))
    st.session_state["cm_detail_data_use_yn"] = _normalize_yes_no(row.get("use_yn"), "Y")
    st.session_state["cm_detail_data_sort_order"] = _safe_int(row.get("sort_order"), 0)
    st.session_state["cm_detail_data_ip_address"] = _to_str(row.get("ip_address"))
    st.session_state["cm_detail_data_mac_address"] = _to_str(row.get("mac_address"))


# ============================================================
# data -> widget 복사
# 반드시 widget 생성 전에만 호출
# ============================================================
def _load_master_widgets_from_data() -> None:
    st.session_state["cm_master_widget_mst_code_id"] = st.session_state["cm_master_data_mst_code_id"]
    st.session_state["cm_master_widget_mst_code"] = st.session_state["cm_master_data_mst_code"]
    st.session_state["cm_master_widget_code_name"] = st.session_state["cm_master_data_code_name"]
    st.session_state["cm_master_widget_use_yn"] = st.session_state["cm_master_data_use_yn"]
    st.session_state["cm_master_widget_sort_order"] = st.session_state["cm_master_data_sort_order"]
    st.session_state["cm_master_widget_ip_address"] = st.session_state["cm_master_data_ip_address"]
    st.session_state["cm_master_widget_mac_address"] = st.session_state["cm_master_data_mac_address"]


def _load_detail_parent_widgets_from_data() -> None:
    st.session_state["cm_detail_parent_widget_mst_code_id"] = st.session_state["cm_detail_parent_data_mst_code_id"]
    st.session_state["cm_detail_parent_widget_mst_code"] = st.session_state["cm_detail_parent_data_mst_code"]
    st.session_state["cm_detail_parent_widget_code_name"] = st.session_state["cm_detail_parent_data_code_name"]


def _load_detail_widgets_from_data() -> None:
    st.session_state["cm_detail_widget_dtl_code_id"] = st.session_state["cm_detail_data_dtl_code_id"]
    st.session_state["cm_detail_widget_mst_code_id"] = st.session_state["cm_detail_data_mst_code_id"]
    st.session_state["cm_detail_widget_mst_code"] = st.session_state["cm_detail_data_mst_code"]
    st.session_state["cm_detail_widget_dtl_code"] = st.session_state["cm_detail_data_dtl_code"]
    st.session_state["cm_detail_widget_code_name"] = st.session_state["cm_detail_data_code_name"]
    st.session_state["cm_detail_widget_use_yn"] = st.session_state["cm_detail_data_use_yn"]
    st.session_state["cm_detail_widget_sort_order"] = st.session_state["cm_detail_data_sort_order"]
    st.session_state["cm_detail_widget_ip_address"] = st.session_state["cm_detail_data_ip_address"]
    st.session_state["cm_detail_widget_mac_address"] = st.session_state["cm_detail_data_mac_address"]


# ============================================================
# 검색 초기화
# ============================================================
def _reset_master_search() -> None:
    st.session_state["cm_master_search_mst_code"] = ""
    st.session_state["cm_master_search_code_name"] = ""
    st.session_state["cm_master_search_use_yn"] = "전체"


def _reset_detail_search() -> None:
    st.session_state["cm_detail_search_parent_mst_code"] = ""
    st.session_state["cm_detail_search_parent_code_name"] = ""
    st.session_state["cm_detail_search_dtl_code"] = ""
    st.session_state["cm_detail_search_code_name"] = ""
    st.session_state["cm_detail_search_use_yn"] = "전체"


# ============================================================
# 모드 전환
# ============================================================
def _enter_master_new_mode() -> None:
    st.session_state["cm_master_mode"] = "new"
    st.session_state["cm_master_selected_ids"] = []
    st.session_state["cm_master_download_open"] = False
    _reset_master_search()
    _clear_master_data()
    _set_message("상위코드 신규 입력 모드입니다.", "info")


def _enter_detail_new_mode() -> None:
    st.session_state["cm_detail_mode"] = "new"
    st.session_state["cm_detail_selected_ids"] = []
    st.session_state["cm_detail_download_open"] = False
    _reset_detail_search()
    _clear_detail_data()
    _set_message("하위코드 신규 입력 모드입니다.", "info")


# ============================================================
# 검색조건
# ============================================================
def _get_master_search_params() -> Dict[str, Any]:
    use_yn = st.session_state["cm_master_search_use_yn"]
    return {
        "mst_code": st.session_state["cm_master_search_mst_code"].strip(),
        "code_name": st.session_state["cm_master_search_code_name"].strip(),
        "use_yn": "" if use_yn == "전체" else use_yn,
    }


def _get_detail_search_params() -> Dict[str, Any]:
    use_yn = st.session_state["cm_detail_search_use_yn"]
    parent_ids = st.session_state["cm_detail_parent_selected_mst_ids"]
    return {
        "mst_code_id": parent_ids[0] if len(parent_ids) == 1 else "",
        "mst_code": st.session_state["cm_detail_search_parent_mst_code"].strip(),
        "mst_code_name": st.session_state["cm_detail_search_parent_code_name"].strip(),
        "dtl_code": st.session_state["cm_detail_search_dtl_code"].strip(),
        "code_name": st.session_state["cm_detail_search_code_name"].strip(),
        "use_yn": "" if use_yn == "전체" else use_yn,
    }


# ============================================================
# 선택 반영
# ============================================================
def _apply_master_selection(master_rows: List[Dict[str, Any]], edited_df: pd.DataFrame) -> None:
    selected_ids = edited_df.loc[edited_df["_checked"] == True, "mst_code_id"].astype(str).tolist()
    current_ids = [str(x) for x in st.session_state["cm_master_selected_ids"]]

    if selected_ids != current_ids:
        st.session_state["cm_master_selected_ids"] = selected_ids
        selected_rows = [row for row in master_rows if str(row.get("mst_code_id")) in selected_ids]

        if len(selected_rows) == 1 and st.session_state["cm_master_mode"] == "view":
            _sync_master_data_from_row(selected_rows[0])
            _set_message("상위코드 1건이 선택되었습니다.", "info")
        elif len(selected_rows) > 1:
            _clear_master_data()
            _set_message("입력 화면에 데이타가 다중 선택되었습니다.", "warning")
        elif len(selected_rows) == 0 and st.session_state["cm_master_mode"] == "view":
            _clear_master_data()

        st.rerun()


def _apply_detail_parent_selection(parent_rows: List[Dict[str, Any]], edited_df: pd.DataFrame) -> None:
    selected_ids = edited_df.loc[edited_df["_checked"] == True, "mst_code_id"].astype(str).tolist()
    current_ids = [str(x) for x in st.session_state["cm_detail_parent_selected_mst_ids"]]

    if selected_ids != current_ids:
        st.session_state["cm_detail_parent_selected_mst_ids"] = selected_ids
        st.session_state["cm_detail_selected_ids"] = []
        _clear_detail_data()

        selected_rows = [row for row in parent_rows if str(row.get("mst_code_id")) in selected_ids]

        if len(selected_rows) == 1:
            row = selected_rows[0]
            _sync_detail_parent_data_from_row(row)
            if st.session_state["cm_detail_mode"] == "new":
                st.session_state["cm_detail_data_mst_code_id"] = _to_str(row.get("mst_code_id"))
                st.session_state["cm_detail_data_mst_code"] = _to_str(row.get("mst_code"))
            _set_message("부모 상위코드 1건이 선택되었습니다.", "info")
        elif len(selected_rows) > 1:
            _clear_detail_parent_data()
            _set_message("입력 화면에 데이타가 다중 선택되었습니다.", "warning")
        else:
            _clear_detail_parent_data()

        st.rerun()


def _apply_detail_selection(detail_rows: List[Dict[str, Any]], edited_df: pd.DataFrame) -> None:
    selected_ids = edited_df.loc[edited_df["_checked"] == True, "dtl_code_id"].astype(str).tolist()
    current_ids = [str(x) for x in st.session_state["cm_detail_selected_ids"]]

    if selected_ids != current_ids:
        st.session_state["cm_detail_selected_ids"] = selected_ids
        selected_rows = [row for row in detail_rows if str(row.get("dtl_code_id")) in selected_ids]

        if len(selected_rows) == 1 and st.session_state["cm_detail_mode"] == "view":
            row = selected_rows[0]
            _sync_detail_data_from_row(row)
            _sync_detail_parent_data_from_row(
                {
                    "mst_code_id": row.get("mst_code_id"),
                    "mst_code": row.get("mst_code"),
                    "code_name": row.get("mst_code_name"),
                }
            )
            st.session_state["cm_detail_parent_selected_mst_ids"] = [_to_str(row.get("mst_code_id"))]
            _set_message("하위코드 1건이 선택되었습니다.", "info")
        elif len(selected_rows) > 1:
            _clear_detail_data()
            _set_message("입력 화면에 데이타가 다중 선택되었습니다.", "warning")
        elif len(selected_rows) == 0 and st.session_state["cm_detail_mode"] == "view":
            _clear_detail_data()

        st.rerun()


# ============================================================
# 저장
# ============================================================
def _validate_master_widget_values() -> Optional[str]:
    if not st.session_state["cm_master_widget_mst_code"].strip():
        return "상위코드를 입력해 주세요."
    if not st.session_state["cm_master_widget_code_name"].strip():
        return "상위코드명을 입력해 주세요."
    return None


def _validate_detail_widget_values() -> Optional[str]:
    if not st.session_state["cm_detail_widget_mst_code_id"]:
        return "부모 상위코드를 먼저 1건 선택해 주세요."
    if not st.session_state["cm_detail_widget_dtl_code"].strip():
        return "하위코드를 입력해 주세요."
    if not st.session_state["cm_detail_widget_code_name"].strip():
        return "하위코드명을 입력해 주세요."
    return None


def _save_master(conn: Any) -> None:
    error_message = _validate_master_widget_values()
    if error_message:
        _set_message(error_message, "warning")
        st.rerun()

    save_data = {
        "mst_code_id": st.session_state["cm_master_widget_mst_code_id"],
        "mst_code": st.session_state["cm_master_widget_mst_code"].strip(),
        "code_name": st.session_state["cm_master_widget_code_name"].strip(),
        "use_yn": _normalize_yes_no(st.session_state["cm_master_widget_use_yn"], "Y"),
        "sort_order": _safe_int(st.session_state["cm_master_widget_sort_order"], 0),
        "ip_address": st.session_state["cm_master_widget_ip_address"].strip(),
        "mac_address": st.session_state["cm_master_widget_mac_address"].strip(),
    }

    try:
        result = _save_master_code(conn, save_data)
        saved_row = result.get("row") if isinstance(result, dict) else None

        if isinstance(saved_row, dict):
            _sync_master_data_from_row(saved_row)
            st.session_state["cm_master_selected_ids"] = [_to_str(saved_row.get("mst_code_id"))]

        st.session_state["cm_master_mode"] = "view"
        _set_message("상위코드가 저장되었습니다.", "success")
    except Exception as exc:
        _set_message(f"상위코드 저장 중 오류가 발생했습니다: {exc}", "error")

    st.rerun()


def _save_detail(conn: Any) -> None:
    error_message = _validate_detail_widget_values()
    if error_message:
        _set_message(error_message, "warning")
        st.rerun()

    save_data = {
        "dtl_code_id": st.session_state["cm_detail_widget_dtl_code_id"],
        "mst_code_id": st.session_state["cm_detail_widget_mst_code_id"],
        "mst_code": st.session_state["cm_detail_widget_mst_code"].strip(),
        "dtl_code": st.session_state["cm_detail_widget_dtl_code"].strip(),
        "code_name": st.session_state["cm_detail_widget_code_name"].strip(),
        "use_yn": _normalize_yes_no(st.session_state["cm_detail_widget_use_yn"], "Y"),
        "sort_order": _safe_int(st.session_state["cm_detail_widget_sort_order"], 0),
        "ip_address": st.session_state["cm_detail_widget_ip_address"].strip(),
        "mac_address": st.session_state["cm_detail_widget_mac_address"].strip(),
    }

    try:
        result = _save_detail_code(conn, save_data)
        saved_row = result.get("row") if isinstance(result, dict) else None

        if isinstance(saved_row, dict):
            _sync_detail_data_from_row(saved_row)
            _sync_detail_parent_data_from_row(
                {
                    "mst_code_id": saved_row.get("mst_code_id"),
                    "mst_code": saved_row.get("mst_code"),
                    "code_name": saved_row.get("mst_code_name"),
                }
            )
            st.session_state["cm_detail_selected_ids"] = [_to_str(saved_row.get("dtl_code_id"))]
            st.session_state["cm_detail_parent_selected_mst_ids"] = [_to_str(saved_row.get("mst_code_id"))]

        st.session_state["cm_detail_mode"] = "view"
        _set_message("하위코드가 저장되었습니다.", "success")
    except Exception as exc:
        _set_message(f"하위코드 저장 중 오류가 발생했습니다: {exc}", "error")

    st.rerun()


# ============================================================
# 그리드
# ============================================================
def _render_master_grid(master_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    grid_df = _build_master_df(master_rows)
    selected_ids = set(st.session_state["cm_master_selected_ids"])
    grid_df["_checked"] = grid_df["mst_code_id"].astype(str).isin({str(x) for x in selected_ids})

    return st.data_editor(
        grid_df,
        key="cm_master_grid_editor",
        hide_index=True,
        width="stretch",
        height=420,
        num_rows="fixed",
        column_config={
            "_checked": st.column_config.CheckboxColumn("선택", default=False),
            "mst_code_id": st.column_config.TextColumn("상위코드ID", disabled=True),
            "mst_code": st.column_config.TextColumn("상위코드", disabled=True),
            "code_name": st.column_config.TextColumn("상위코드명", disabled=True),
            "use_yn": st.column_config.TextColumn("사용여부", disabled=True),
            "sort_order": st.column_config.NumberColumn("정렬순서", disabled=True),
            "ip_address": st.column_config.TextColumn("IP주소", disabled=True),
            "mac_address": st.column_config.TextColumn("MAC주소", disabled=True),
            "created_by": st.column_config.TextColumn("등록자", disabled=True),
            "created_at": st.column_config.TextColumn("등록일시", disabled=True),
            "updated_by": st.column_config.TextColumn("수정자", disabled=True),
            "updated_at": st.column_config.TextColumn("수정일시", disabled=True),
        },
        disabled=[col for col in MASTER_GRID_COLUMNS],
    )


def _render_detail_parent_grid(parent_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    grid_df = _build_master_df(parent_rows)
    selected_ids = set(st.session_state["cm_detail_parent_selected_mst_ids"])
    grid_df["_checked"] = grid_df["mst_code_id"].astype(str).isin({str(x) for x in selected_ids})

    return st.data_editor(
        grid_df,
        key="cm_detail_parent_grid_editor",
        hide_index=True,
        width="stretch",
        height=240,
        num_rows="fixed",
        column_config={
            "_checked": st.column_config.CheckboxColumn("선택", default=False),
            "mst_code_id": st.column_config.TextColumn("상위코드ID", disabled=True),
            "mst_code": st.column_config.TextColumn("상위코드", disabled=True),
            "code_name": st.column_config.TextColumn("상위코드명", disabled=True),
            "use_yn": st.column_config.TextColumn("사용여부", disabled=True),
            "sort_order": st.column_config.NumberColumn("정렬순서", disabled=True),
            "ip_address": st.column_config.TextColumn("IP주소", disabled=True),
            "mac_address": st.column_config.TextColumn("MAC주소", disabled=True),
            "created_by": st.column_config.TextColumn("등록자", disabled=True),
            "created_at": st.column_config.TextColumn("등록일시", disabled=True),
            "updated_by": st.column_config.TextColumn("수정자", disabled=True),
            "updated_at": st.column_config.TextColumn("수정일시", disabled=True),
        },
        disabled=[col for col in MASTER_GRID_COLUMNS],
    )


def _render_detail_grid(detail_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    grid_df = _build_detail_df(detail_rows)
    selected_ids = set(st.session_state["cm_detail_selected_ids"])
    grid_df["_checked"] = grid_df["dtl_code_id"].astype(str).isin({str(x) for x in selected_ids})

    return st.data_editor(
        grid_df,
        key="cm_detail_grid_editor",
        hide_index=True,
        width="stretch",
        height=330,
        num_rows="fixed",
        column_config={
            "_checked": st.column_config.CheckboxColumn("선택", default=False),
            "dtl_code_id": st.column_config.TextColumn("하위코드ID", disabled=True),
            "mst_code_id": st.column_config.TextColumn("상위코드ID", disabled=True),
            "mst_code": st.column_config.TextColumn("상위코드", disabled=True),
            "mst_code_name": st.column_config.TextColumn("상위코드명", disabled=True),
            "dtl_code": st.column_config.TextColumn("하위코드", disabled=True),
            "code_name": st.column_config.TextColumn("하위코드명", disabled=True),
            "use_yn": st.column_config.TextColumn("사용여부", disabled=True),
            "sort_order": st.column_config.NumberColumn("정렬순서", disabled=True),
            "ip_address": st.column_config.TextColumn("IP주소", disabled=True),
            "mac_address": st.column_config.TextColumn("MAC주소", disabled=True),
            "created_by": st.column_config.TextColumn("등록자", disabled=True),
            "created_at": st.column_config.TextColumn("등록일시", disabled=True),
            "updated_by": st.column_config.TextColumn("수정자", disabled=True),
            "updated_at": st.column_config.TextColumn("수정일시", disabled=True),
        },
        disabled=[col for col in DETAIL_GRID_COLUMNS],
    )


# ============================================================
# 상위코드 탭
# ============================================================
def _render_master_left_panel(conn: Any, master_rows: List[Dict[str, Any]], master_df: pd.DataFrame) -> None:
    with st.container(border=True):
        st.markdown("#### 상위코드 관리")

        btn1, btn2, btn3, btn4, btn5 = st.columns(5)

        with btn1:
            if st.button("신규", key="cm_master_btn_new", width="stretch"):
                _enter_master_new_mode()
                st.rerun()

        with btn2:
            if st.button("수정", key="cm_master_btn_edit", width="stretch"):
                selected_rows = [row for row in master_rows if str(row.get("mst_code_id")) in [str(x) for x in st.session_state["cm_master_selected_ids"]]]
                if len(selected_rows) != 1:
                    _set_message("수정은 상위코드 1건 선택 시만 가능합니다.", "warning")
                else:
                    st.session_state["cm_master_mode"] = "edit"
                    _sync_master_data_from_row(selected_rows[0])
                    _set_message("상위코드 수정 모드입니다.", "info")
                st.rerun()

        with btn3:
            if st.button("저장", key="cm_master_btn_save", width="stretch"):
                _save_master(conn)

        with btn4:
            if st.button("전체선택", key="cm_master_btn_select_all", width="stretch"):
                st.session_state["cm_master_selected_ids"] = master_df["mst_code_id"].astype(str).tolist()
                if len(master_rows) == 1:
                    _sync_master_data_from_row(master_rows[0])
                else:
                    _clear_master_data()
                    _set_message("입력 화면에 데이타가 다중 선택되었습니다.", "warning")
                st.rerun()

        with btn5:
            if st.button("선택해제", key="cm_master_btn_clear_select", width="stretch"):
                st.session_state["cm_master_selected_ids"] = []
                if st.session_state["cm_master_mode"] == "view":
                    _clear_master_data()
                _set_message("상위코드 선택이 해제되었습니다.", "info")
                st.rerun()

        if len(st.session_state["cm_master_selected_ids"]) > 1:
            st.warning("입력 화면에 데이타가 다중 선택되었습니다.")

        _load_master_widgets_from_data()

        col1, col2 = st.columns(2)
        with col1:
            st.text_input("상위코드ID", key="cm_master_widget_mst_code_id", disabled=True)
            st.text_input("상위코드", key="cm_master_widget_mst_code")
            st.selectbox("사용여부", ["Y", "N"], key="cm_master_widget_use_yn")

        with col2:
            st.text_input("상위코드명", key="cm_master_widget_code_name")
            st.number_input("정렬순서", min_value=0, step=1, key="cm_master_widget_sort_order")
            st.text_input("IP주소", key="cm_master_widget_ip_address")

        st.text_input("MAC주소", key="cm_master_widget_mac_address")

        st.divider()

        t1, t2 = st.columns([1, 2])
        with t1:
            if st.button("엑셀 저장 옵션", key="cm_master_btn_download_toggle", width="stretch"):
                st.session_state["cm_master_download_open"] = not st.session_state["cm_master_download_open"]
                st.rerun()
        with t2:
            st.caption("다운로드 필드명은 한글명으로만 표시됩니다.")

        if st.session_state["cm_master_download_open"]:
            all_labels = list(MASTER_FIELD_LABELS.values())
            st.multiselect(
                "다운로드 항목 선택",
                options=all_labels,
                default=st.session_state["cm_master_download_labels"],
                key="cm_master_download_labels",
            )
            download_df = _rename_for_download(
                master_df.drop(columns=["_checked"], errors="ignore"),
                MASTER_FIELD_LABELS,
                st.session_state["cm_master_download_labels"],
            )
            st.download_button(
                "엑셀 저장",
                data=_make_excel_bytes(download_df, "상위코드"),
                file_name="상위코드_관리.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="cm_master_btn_download",
                width="stretch",
            )


def _render_master_right_panel(master_rows: List[Dict[str, Any]]) -> None:
    with st.container(border=True):
        st.markdown("#### 상위코드 목록")

        c1, c2, c3, c4 = st.columns([1.2, 1.2, 0.8, 0.8])
        with c1:
            st.text_input("상위코드 검색", key="cm_master_search_mst_code")
        with c2:
            st.text_input("상위코드명 검색", key="cm_master_search_code_name")
        with c3:
            st.selectbox("사용여부", ["전체", "Y", "N"], key="cm_master_search_use_yn")
        with c4:
            if st.button("검색초기화", key="cm_master_btn_reset_search", width="stretch"):
                _reset_master_search()
                st.rerun()

        st.caption(f"조회건수 : {len(master_rows)} 건")
        edited_df = _render_master_grid(master_rows)
        _apply_master_selection(master_rows, edited_df)


def _render_master_tab(conn: Any) -> None:
    master_rows = _get_master_code_list(conn, _get_master_search_params())

    if st.session_state["cm_master_mode"] == "view":
        selected_rows = [
            row for row in master_rows
            if str(row.get("mst_code_id")) in [str(x) for x in st.session_state["cm_master_selected_ids"]]
        ]
        if len(selected_rows) == 1:
            _sync_master_data_from_row(selected_rows[0])

    master_df = _build_master_df(master_rows)

    left_col, right_col = st.columns([4, 6])
    with left_col:
        _render_master_left_panel(conn, master_rows, master_df)
    with right_col:
        _render_master_right_panel(master_rows)


# ============================================================
# 하위코드 탭
# ============================================================
def _render_detail_left_panel(conn: Any, parent_rows: List[Dict[str, Any]], detail_rows: List[Dict[str, Any]], detail_df: pd.DataFrame) -> None:
    with st.container(border=True):
        st.markdown("#### 상위코드 관리 영역")

        _load_detail_parent_widgets_from_data()

        st.text_input("상위코드ID", key="cm_detail_parent_widget_mst_code_id", disabled=True)
        st.text_input("상위코드", key="cm_detail_parent_widget_mst_code", disabled=True)
        st.text_input("상위코드명", key="cm_detail_parent_widget_code_name", disabled=True)

        st.divider()
        st.markdown("#### 하위코드 관리")

        btn1, btn2, btn3, btn4, btn5 = st.columns(5)

        with btn1:
            if st.button("신규", key="cm_detail_btn_new", width="stretch"):
                selected_parent_rows = [
                    row for row in parent_rows
                    if str(row.get("mst_code_id")) in [str(x) for x in st.session_state["cm_detail_parent_selected_mst_ids"]]
                ]
                _enter_detail_new_mode()
                if len(selected_parent_rows) == 1:
                    _sync_detail_parent_data_from_row(selected_parent_rows[0])
                    st.session_state["cm_detail_data_mst_code_id"] = _to_str(selected_parent_rows[0].get("mst_code_id"))
                    st.session_state["cm_detail_data_mst_code"] = _to_str(selected_parent_rows[0].get("mst_code"))
                st.rerun()

        with btn2:
            if st.button("수정", key="cm_detail_btn_edit", width="stretch"):
                selected_rows = [row for row in detail_rows if str(row.get("dtl_code_id")) in [str(x) for x in st.session_state["cm_detail_selected_ids"]]]
                if len(selected_rows) != 1:
                    _set_message("수정은 하위코드 1건 선택 시만 가능합니다.", "warning")
                else:
                    row = selected_rows[0]
                    st.session_state["cm_detail_mode"] = "edit"
                    _sync_detail_data_from_row(row)
                    _sync_detail_parent_data_from_row(
                        {
                            "mst_code_id": row.get("mst_code_id"),
                            "mst_code": row.get("mst_code"),
                            "code_name": row.get("mst_code_name"),
                        }
                    )
                    _set_message("하위코드 수정 모드입니다.", "info")
                st.rerun()

        with btn3:
            if st.button("저장", key="cm_detail_btn_save", width="stretch"):
                _save_detail(conn)

        with btn4:
            if st.button("전체선택", key="cm_detail_btn_select_all", width="stretch"):
                st.session_state["cm_detail_selected_ids"] = detail_df["dtl_code_id"].astype(str).tolist()
                if len(detail_rows) == 1:
                    _sync_detail_data_from_row(detail_rows[0])
                else:
                    _clear_detail_data()
                    _set_message("입력 화면에 데이타가 다중 선택되었습니다.", "warning")
                st.rerun()

        with btn5:
            if st.button("선택해제", key="cm_detail_btn_clear_select", width="stretch"):
                st.session_state["cm_detail_selected_ids"] = []
                if st.session_state["cm_detail_mode"] == "view":
                    _clear_detail_data()
                _set_message("하위코드 선택이 해제되었습니다.", "info")
                st.rerun()

        if len(st.session_state["cm_detail_selected_ids"]) > 1:
            st.warning("입력 화면에 데이타가 다중 선택되었습니다.")

        _load_detail_widgets_from_data()

        col1, col2 = st.columns(2)
        with col1:
            st.text_input("하위코드ID", key="cm_detail_widget_dtl_code_id", disabled=True)
            st.text_input("상위코드ID", key="cm_detail_widget_mst_code_id", disabled=True)
            st.text_input("상위코드", key="cm_detail_widget_mst_code", disabled=True)
            st.selectbox("사용여부", ["Y", "N"], key="cm_detail_widget_use_yn")

        with col2:
            st.text_input("하위코드", key="cm_detail_widget_dtl_code")
            st.text_input("하위코드명", key="cm_detail_widget_code_name")
            st.number_input("정렬순서", min_value=0, step=1, key="cm_detail_widget_sort_order")

        c3, c4 = st.columns(2)
        with c3:
            st.text_input("IP주소", key="cm_detail_widget_ip_address")
        with c4:
            st.text_input("MAC주소", key="cm_detail_widget_mac_address")

        st.divider()

        t1, t2 = st.columns([1, 2])
        with t1:
            if st.button("엑셀 저장 옵션", key="cm_detail_btn_download_toggle", width="stretch"):
                st.session_state["cm_detail_download_open"] = not st.session_state["cm_detail_download_open"]
                st.rerun()
        with t2:
            st.caption("다운로드 필드명은 한글명으로만 표시됩니다.")

        if st.session_state["cm_detail_download_open"]:
            all_labels = list(DETAIL_FIELD_LABELS.values())
            st.multiselect(
                "다운로드 항목 선택",
                options=all_labels,
                default=st.session_state["cm_detail_download_labels"],
                key="cm_detail_download_labels",
            )
            download_df = _rename_for_download(
                detail_df.drop(columns=["_checked"], errors="ignore"),
                DETAIL_FIELD_LABELS,
                st.session_state["cm_detail_download_labels"],
            )
            st.download_button(
                "엑셀 저장",
                data=_make_excel_bytes(download_df, "하위코드"),
                file_name="하위코드_관리.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="cm_detail_btn_download",
                width="stretch",
            )


def _render_detail_right_panel(parent_rows: List[Dict[str, Any]], detail_rows: List[Dict[str, Any]]) -> None:
    with st.container(border=True):
        st.markdown("#### 상위코드 목록")

        c1, c2, c3 = st.columns([1.3, 1.3, 0.8])
        with c1:
            st.text_input("상위코드 검색", key="cm_detail_search_parent_mst_code")
        with c2:
            st.text_input("상위코드명 검색", key="cm_detail_search_parent_code_name")
        with c3:
            if st.button("상위검색초기화", key="cm_detail_btn_reset_parent_search", width="stretch"):
                st.session_state["cm_detail_search_parent_mst_code"] = ""
                st.session_state["cm_detail_search_parent_code_name"] = ""
                st.rerun()

        st.caption(f"상위코드 조회건수 : {len(parent_rows)} 건")
        edited_parent_df = _render_detail_parent_grid(parent_rows)
        _apply_detail_parent_selection(parent_rows, edited_parent_df)

        st.divider()
        st.markdown("#### 하위코드 목록")

        d1, d2, d3, d4 = st.columns([1.1, 1.2, 0.8, 0.8])
        with d1:
            st.text_input("하위코드 검색", key="cm_detail_search_dtl_code")
        with d2:
            st.text_input("하위코드명 검색", key="cm_detail_search_code_name")
        with d3:
            st.selectbox("사용여부", ["전체", "Y", "N"], key="cm_detail_search_use_yn")
        with d4:
            if st.button("하위검색초기화", key="cm_detail_btn_reset_detail_search", width="stretch"):
                st.session_state["cm_detail_search_dtl_code"] = ""
                st.session_state["cm_detail_search_code_name"] = ""
                st.session_state["cm_detail_search_use_yn"] = "전체"
                st.rerun()

        st.caption(f"하위코드 조회건수 : {len(detail_rows)} 건")
        edited_detail_df = _render_detail_grid(detail_rows)
        _apply_detail_selection(detail_rows, edited_detail_df)


def _render_detail_tab(conn: Any) -> None:
    parent_rows = _get_master_code_list(
        conn,
        {
            "mst_code": st.session_state["cm_detail_search_parent_mst_code"].strip(),
            "code_name": st.session_state["cm_detail_search_parent_code_name"].strip(),
            "use_yn": "",
        },
    )

    detail_rows = _get_detail_code_list(conn, _get_detail_search_params())

    if st.session_state["cm_detail_mode"] == "view":
        selected_rows = [
            row for row in detail_rows
            if str(row.get("dtl_code_id")) in [str(x) for x in st.session_state["cm_detail_selected_ids"]]
        ]
        if len(selected_rows) == 1:
            row = selected_rows[0]
            _sync_detail_data_from_row(row)
            _sync_detail_parent_data_from_row(
                {
                    "mst_code_id": row.get("mst_code_id"),
                    "mst_code": row.get("mst_code"),
                    "code_name": row.get("mst_code_name"),
                }
            )

    detail_df = _build_detail_df(detail_rows)

    left_col, right_col = st.columns([4, 6])
    with left_col:
        _render_detail_left_panel(conn, parent_rows, detail_rows, detail_df)
    with right_col:
        _render_detail_right_panel(parent_rows, detail_rows)


# ============================================================
# 외부 공개 함수
# ============================================================
def render_code_manage_page(conn: Any) -> None:
    _init_state()

    if st.session_state["cm_message"]:
        _show_message(st.session_state["cm_message"], st.session_state["cm_message_type"])
        _clear_message()

    try:
        active_tab = st.radio(
            "코드관리 구분",
            options=["상위코드", "하위코드"],
            key="cm_tab_selector",
            horizontal=True,
            label_visibility="collapsed",
        )
        st.session_state["cm_active_tab"] = active_tab

        if active_tab == "상위코드":
            _render_master_tab(conn)
        else:
            _render_detail_tab(conn)

    except Exception as exc:
        st.error(f"코드관리 화면 처리 중 오류가 발생했습니다: {exc}")