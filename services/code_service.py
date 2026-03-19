# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 코드관리 Service
파일경로   : services/code_service.py
기능설명   : SmartSite 코드관리 Service
작성일시   : 2026-03-18 18:10
주요기능   :
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

from repositories.code_manage_repository import (
    delete_dtl_code_by_id,
    delete_mst_code_by_id,
    get_dtl_code_by_id,
    get_mst_code_by_id,
    insert_dtl_code,
    insert_mst_code,
    select_detail_code_list,
    select_master_code_list,
    update_dtl_code,
    update_mst_code,
)


def get_master_code_list(conn: sqlite3.Connection, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return select_master_code_list(conn, search_params)


def search_master_code_list(conn: sqlite3.Connection, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return select_master_code_list(conn, search_params)


def select_master_code_list_service(conn: sqlite3.Connection, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return select_master_code_list(conn, search_params)


def get_detail_code_list(conn: sqlite3.Connection, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return select_detail_code_list(conn, search_params)


def search_detail_code_list(conn: sqlite3.Connection, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return select_detail_code_list(conn, search_params)


def save_master_code(conn: sqlite3.Connection, data: Dict[str, Any]) -> Dict[str, Any]:
    mst_code_id = data.get("mst_code_id")
    mst_code = (data.get("mst_code") or "").strip()
    code_name = (data.get("code_name") or data.get("mst_code_name") or "").strip()
    use_yn = (data.get("use_yn") or "Y").strip() or "Y"
    sort_order = int(data.get("sort_order") or 0)
    remark = (data.get("remark") or "").strip()

    if not mst_code:
        raise ValueError("상위코드를 입력해 주세요.")
    if not code_name:
        raise ValueError("상위코드명을 입력해 주세요.")

    if mst_code_id in (None, ""):
        new_id = insert_mst_code(conn, mst_code, code_name, sort_order, use_yn, remark)
        row = get_mst_code_by_id(conn, new_id)
    else:
        update_mst_code(conn, int(mst_code_id), mst_code, code_name, sort_order, use_yn, remark)
        row = get_mst_code_by_id(conn, int(mst_code_id))

    return {"success": True, "row": row}


def save_detail_code(conn: sqlite3.Connection, data: Dict[str, Any]) -> Dict[str, Any]:
    dtl_code_id = data.get("dtl_code_id")
    mst_code_id = data.get("mst_code_id")
    dtl_code = (data.get("dtl_code") or "").strip()
    code_name = (data.get("code_name") or data.get("dtl_code_name") or "").strip()
    use_yn = (data.get("use_yn") or "Y").strip() or "Y"
    sort_order = int(data.get("sort_order") or 0)

    if mst_code_id in (None, ""):
        raise ValueError("부모 상위코드를 먼저 1건 선택해 주세요.")
    if not dtl_code:
        raise ValueError("하위코드를 입력해 주세요.")
    if not code_name:
        raise ValueError("하위코드명을 입력해 주세요.")

    if dtl_code_id in (None, ""):
        new_id = insert_dtl_code(conn, int(mst_code_id), dtl_code, code_name, sort_order, use_yn)
        row = get_dtl_code_by_id(conn, new_id)
    else:
        update_dtl_code(conn, int(dtl_code_id), int(mst_code_id), dtl_code, code_name, sort_order, use_yn)
        row = get_dtl_code_by_id(conn, int(dtl_code_id))

    return {"success": True, "row": row}


def remove_master_codes(conn: sqlite3.Connection, mst_code_ids: List[int]) -> int:
    deleted_count = 0
    for mst_code_id in mst_code_ids:
        success, message = delete_mst_code_by_id(conn, int(mst_code_id))
        if not success:
            raise ValueError(message)
        deleted_count += 1
    return deleted_count


def remove_detail_codes(conn: sqlite3.Connection, dtl_code_ids: List[int]) -> int:
    deleted_count = 0
    for dtl_code_id in dtl_code_ids:
        success, message = delete_dtl_code_by_id(conn, int(dtl_code_id))
        if not success:
            raise ValueError(message)
        deleted_count += 1
    return deleted_count
