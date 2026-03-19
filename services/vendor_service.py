# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 업체관리 Service
파일명     : vendor_service.py
설명       : companies 기준 업체관리 업무 로직 처리.
사용 테이블 :
    - companies
작성일시   : 2026-03-18
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.vendor_repository import delete_vendors, get_vendor, list_vendors, save_vendor


def search_vendors(
    conn: sqlite3.Connection,
    search_text: str = "",
    company_type_code_id: Optional[int] = None,
    use_yn_code_id: Optional[int] = None,
) -> pd.DataFrame:
    return list_vendors(
        conn,
        search_text=search_text,
        company_type_code_id=company_type_code_id,
        use_yn_code_id=use_yn_code_id,
    )


def get_vendor_detail(conn: sqlite3.Connection, company_id: int) -> Optional[Dict[str, Any]]:
    return get_vendor(conn, company_id)


def save_vendor_data(conn: sqlite3.Connection, data: Dict[str, Any], actor: str = "system") -> int:
    if not str(data.get("company_code") or "").strip():
        raise ValueError("업체코드를 입력해 주세요.")
    if not str(data.get("company_name") or "").strip():
        raise ValueError("업체명을 입력해 주세요.")
    return save_vendor(conn, data, actor=actor)


def remove_vendors(conn: sqlite3.Connection, company_ids: List[int]) -> int:
    if not company_ids:
        raise ValueError("삭제할 업체를 선택해 주세요.")
    return delete_vendors(conn, company_ids)
