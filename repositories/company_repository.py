# -*- coding: utf-8 -*-
"""
프로그램명 : company_repository.py
파일경로   : repositories/company_repository.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.common_reference_repository import (
    get_code_options_by_mst_code,
    get_code_options_by_mst_code_id,
    get_use_yn_options,
    label_by_value,
    table_exists,
)


def _dict_row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _digits(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))


def _format_phone(value: Any) -> str:
    digits = _digits(value)
    if not digits:
        return ""
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:]}"
    if digits.startswith("02"):
        if len(digits) == 9:
            return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
        if len(digits) == 10:
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11:
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    return digits


def _format_business_no(value: Any) -> str:
    digits = _digits(value)
    if len(digits) >= 10:
        digits = digits[:10]
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:10]}"
    return digits


def _next_company_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(company_id), 1100000) + 1 FROM companies").fetchone()
    return int(row[0]) if row and row[0] else 1100001


def _next_company_code(conn: sqlite3.Connection) -> str:
    rows = conn.execute("SELECT company_code FROM companies WHERE company_code LIKE 'JS-C-%'").fetchall()
    max_no = 0
    for row in rows:
        code = str(row[0] or "")
        m = re.search(r"JS-C-(\d+)$", code)
        if m:
            max_no = max(max_no, int(m.group(1)))
    return f"JS-C-{max_no + 1:03d}"


def get_company_options(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "company_type": get_code_options_by_mst_code_id(conn, 100018, "업체유형") or get_code_options_by_mst_code(conn, "COMPANY_TYPE", "업체유형"),
        "company_status": get_code_options_by_mst_code_id(conn, 100019, "거래상태") or get_code_options_by_mst_code(conn, "COMPANY_STATUS", "거래상태"),
        "company_class": get_code_options_by_mst_code_id(conn, 100020, "법인/개인구분") or get_code_options_by_mst_code(conn, "COMPANY_CLASS", "법인/개인구분"),
        "business_type": get_code_options_by_mst_code_id(conn, 100021, "업태") or get_code_options_by_mst_code(conn, "BUSINESS_TYPE", "업태"),
        "business_item": get_code_options_by_mst_code_id(conn, 100022, "종목") or get_code_options_by_mst_code(conn, "BUSINESS_ITEM", "종목"),
        "use_yn": get_use_yn_options(conn),
    }


def list_companies(conn: sqlite3.Connection, search_text: str = "", use_yn_code_id: Any = None) -> pd.DataFrame:
    if not table_exists(conn, "companies"):
        return pd.DataFrame()

    options = get_company_options(conn)
    sql = """
        SELECT company_id, company_code, company_name, company_type_code_id, company_status_code_id,
               company_class_code_id, business_no, corporation_no, ceo_name, business_type_code_id,
               business_item_code_id, zip_code, address, address_detail, phone, fax, email,
               use_yn_code_id, note, created_at, updated_at
          FROM companies
         WHERE 1 = 1
    """
    params: List[Any] = []
    if str(search_text).strip():
        keyword = f"%{str(search_text).strip()}%"
        sql += """ AND (
                COALESCE(company_code, '') LIKE ?
             OR COALESCE(company_name, '') LIKE ?
             OR COALESCE(business_no, '') LIKE ?
             OR COALESCE(ceo_name, '') LIKE ?
             OR COALESCE(phone, '') LIKE ?
             OR COALESCE(email, '') LIKE ?
             OR COALESCE(address, '') LIKE ?
        ) """
        params.extend([keyword] * 7)
    if use_yn_code_id not in (None, "", "전체"):
        sql += " AND COALESCE(use_yn_code_id, 0) = ? "
        params.append(use_yn_code_id)
    sql += " ORDER BY company_id DESC"

    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df

    df["business_no"] = df["business_no"].apply(_format_business_no)
    df["phone"] = df["phone"].apply(_format_phone)
    df["fax"] = df["fax"].apply(_format_phone)
    df["업체유형"] = df["company_type_code_id"].apply(lambda x: label_by_value(options["company_type"], x))
    df["거래상태"] = df["company_status_code_id"].apply(lambda x: label_by_value(options["company_status"], x))
    df["법인/개인구분"] = df["company_class_code_id"].apply(lambda x: label_by_value(options["company_class"], x))
    df["업태"] = df["business_type_code_id"].apply(lambda x: label_by_value(options["business_type"], x))
    df["종목"] = df["business_item_code_id"].apply(lambda x: label_by_value(options["business_item"], x))
    df["사용여부"] = df["use_yn_code_id"].apply(lambda x: label_by_value(options["use_yn"], x))
    return df


def get_company(conn: sqlite3.Connection, company_id: int) -> Optional[Dict[str, Any]]:
    if not table_exists(conn, "companies"):
        return None
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM companies WHERE company_id = ?", (company_id,)).fetchone()
    conn.row_factory = None
    return _dict_row(row)


def save_company(conn: sqlite3.Connection, payload: Dict[str, Any], actor: str) -> int:
    if not table_exists(conn, "companies"):
        raise ValueError("companies 테이블이 없습니다.")

    pk = int(payload.get("company_id") or 0)
    values = {
        "company_code": str(payload.get("company_code") or "").strip(),
        "company_name": str(payload.get("company_name") or "").strip(),
        "company_type_code_id": payload.get("company_type_code_id") or None,
        "company_status_code_id": payload.get("company_status_code_id") or None,
        "company_class_code_id": payload.get("company_class_code_id") or None,
        "business_no": _digits(payload.get("business_no")),
        "corporation_no": _digits(payload.get("corporation_no")),
        "ceo_name": str(payload.get("ceo_name") or "").strip(),
        "business_type_code_id": payload.get("business_type_code_id") or None,
        "business_item_code_id": payload.get("business_item_code_id") or None,
        "zip_code": _digits(payload.get("zip_code")),
        "address": str(payload.get("address") or "").strip(),
        "address_detail": str(payload.get("address_detail") or "").strip(),
        "phone": _format_phone(payload.get("phone")),
        "fax": _format_phone(payload.get("fax")),
        "email": str(payload.get("email") or "").strip(),
        "use_yn_code_id": payload.get("use_yn_code_id") or None,
        "note": str(payload.get("note") or "").strip(),
    }
    if not values["company_code"]:
        values["company_code"] = _next_company_code(conn)
    if not values["company_name"]:
        raise ValueError("업체명을 입력해 주세요.")

    dup = conn.execute("SELECT company_id FROM companies WHERE company_code = ? AND company_id != ?", (values["company_code"], pk)).fetchone()
    if dup:
        raise ValueError("동일한 업체코드가 이미 존재합니다.")

    if pk:
        conn.execute(
            """
            UPDATE companies
               SET company_code=:company_code, company_name=:company_name, company_type_code_id=:company_type_code_id,
                   company_status_code_id=:company_status_code_id, company_class_code_id=:company_class_code_id,
                   business_no=:business_no, corporation_no=:corporation_no, ceo_name=:ceo_name,
                   business_type_code_id=:business_type_code_id, business_item_code_id=:business_item_code_id,
                   zip_code=:zip_code, address=:address, address_detail=:address_detail, phone=:phone, fax=:fax,
                   email=:email, use_yn_code_id=:use_yn_code_id, note=:note, updated_at=:updated_at, updated_by=:updated_by
             WHERE company_id=:company_id
            """,
            {**values, "updated_at": _now_str(), "updated_by": actor, "company_id": pk},
        )
        return pk

    new_id = _next_company_id(conn)
    conn.execute(
        """
        INSERT INTO companies (
            company_id, company_code, company_name, company_type_code_id, company_status_code_id, company_class_code_id,
            business_no, corporation_no, ceo_name, business_type_code_id, business_item_code_id, zip_code, address,
            address_detail, phone, fax, email, use_yn_code_id, note, created_at, created_by
        ) VALUES (
            :company_id, :company_code, :company_name, :company_type_code_id, :company_status_code_id, :company_class_code_id,
            :business_no, :corporation_no, :ceo_name, :business_type_code_id, :business_item_code_id, :zip_code, :address,
            :address_detail, :phone, :fax, :email, :use_yn_code_id, :note, :created_at, :created_by
        )
        """,
        {**values, "company_id": new_id, "created_at": _now_str(), "created_by": actor},
    )
    return new_id


def delete_companies(conn: sqlite3.Connection, ids: List[int]) -> int:
    target_ids = [int(x) for x in ids if str(x).strip()]
    if not target_ids:
        return 0
    marks = ",".join(["?"] * len(target_ids))
    cur = conn.execute(f"DELETE FROM companies WHERE company_id IN ({marks})", target_ids)
    return int(cur.rowcount or 0)
