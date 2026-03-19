# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 업체관리 Repository
파일명     : vendor_repository.py
설명       : companies 테이블 기준 업체관리 CRUD 처리.
사용 테이블 :
    - companies
    - mst_code
    - dtl_code
작성일시   : 2026-03-18
변경이력   :
주의사항   :
    - 임의 테이블 생성 금지
    - 업로드된 전체 테이블 정의 기준만 사용
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.common_schema_repository import require_table, row_to_dict


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def list_vendors(
    conn: sqlite3.Connection,
    search_text: str = "",
    company_type_code_id: Optional[int] = None,
    use_yn_code_id: Optional[int] = None,
) -> pd.DataFrame:
    require_table(conn, "companies")

    sql = """
        SELECT c.company_id,
               c.company_code,
               c.company_name,
               c.company_type_code_id,
               c.company_status_code_id,
               c.company_class_code_id,
               c.business_no,
               c.corporation_no,
               c.ceo_name,
               c.business_type_code_id,
               c.business_item_code_id,
               c.zip_code,
               c.address,
               c.address_detail,
               c.phone,
               c.fax,
               c.email,
               c.use_yn_code_id,
               c.note,
               c.created_at,
               c.updated_at
          FROM companies c
         WHERE 1=1
    """
    params: List[Any] = []

    if search_text:
        like = f"%{search_text}%"
        sql += """
           AND (
                COALESCE(c.company_code, '') LIKE ?
                OR COALESCE(c.company_name, '') LIKE ?
                OR COALESCE(c.business_no, '') LIKE ?
                OR COALESCE(c.ceo_name, '') LIKE ?
           )
        """
        params.extend([like, like, like, like])

    if company_type_code_id:
        sql += " AND c.company_type_code_id = ?"
        params.append(company_type_code_id)

    if use_yn_code_id:
        sql += " AND c.use_yn_code_id = ?"
        params.append(use_yn_code_id)

    sql += " ORDER BY c.company_id DESC"
    return pd.read_sql_query(sql, conn, params=params)


def get_vendor(conn: sqlite3.Connection, company_id: int) -> Optional[Dict[str, Any]]:
    require_table(conn, "companies")
    row = conn.execute(
        """
        SELECT *
          FROM companies
         WHERE company_id = ?
        """,
        (company_id,),
    ).fetchone()
    result = row_to_dict(row)
    return result or None


def save_vendor(conn: sqlite3.Connection, data: Dict[str, Any], actor: str = "system") -> int:
    require_table(conn, "companies")
    now = _now_str()
    company_id = int(data.get("company_id") or 0)

    update_values = (
        data.get("company_code"),
        data.get("company_name"),
        data.get("company_type_code_id"),
        data.get("company_status_code_id"),
        data.get("company_class_code_id"),
        data.get("business_no"),
        data.get("corporation_no"),
        data.get("ceo_name"),
        data.get("business_type_code_id"),
        data.get("business_item_code_id"),
        data.get("zip_code"),
        data.get("address"),
        data.get("address_detail"),
        data.get("phone"),
        data.get("fax"),
        data.get("email"),
        data.get("use_yn_code_id"),
        data.get("note"),
        actor,
        now,
    )

    if company_id > 0:
        conn.execute(
            """
            UPDATE companies
               SET company_code = ?,
                   company_name = ?,
                   company_type_code_id = ?,
                   company_status_code_id = ?,
                   company_class_code_id = ?,
                   business_no = ?,
                   corporation_no = ?,
                   ceo_name = ?,
                   business_type_code_id = ?,
                   business_item_code_id = ?,
                   zip_code = ?,
                   address = ?,
                   address_detail = ?,
                   phone = ?,
                   fax = ?,
                   email = ?,
                   use_yn_code_id = ?,
                   note = ?,
                   updated_by = ?,
                   updated_at = ?
             WHERE company_id = ?
            """,
            update_values + (company_id,),
        )
        conn.commit()
        return company_id

    row = conn.execute("SELECT COALESCE(MAX(company_id), 0) + 1 AS next_id FROM companies").fetchone()
    next_id = int(row["next_id"] if isinstance(row, sqlite3.Row) else row[0])

    conn.execute(
        """
        INSERT INTO companies (
            company_id,
            company_code,
            company_name,
            company_type_code_id,
            company_status_code_id,
            company_class_code_id,
            business_no,
            corporation_no,
            ceo_name,
            business_type_code_id,
            business_item_code_id,
            zip_code,
            address,
            address_detail,
            phone,
            fax,
            email,
            use_yn_code_id,
            note,
            created_by,
            created_at,
            updated_by,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            next_id,
            data.get("company_code"),
            data.get("company_name"),
            data.get("company_type_code_id"),
            data.get("company_status_code_id"),
            data.get("company_class_code_id"),
            data.get("business_no"),
            data.get("corporation_no"),
            data.get("ceo_name"),
            data.get("business_type_code_id"),
            data.get("business_item_code_id"),
            data.get("zip_code"),
            data.get("address"),
            data.get("address_detail"),
            data.get("phone"),
            data.get("fax"),
            data.get("email"),
            data.get("use_yn_code_id"),
            data.get("note"),
            actor,
            now,
            actor,
            now,
        ),
    )
    conn.commit()
    return next_id


def delete_vendors(conn: sqlite3.Connection, company_ids: List[int]) -> int:
    require_table(conn, "companies")
    if not company_ids:
        return 0
    placeholders = ",".join(["?"] * len(company_ids))
    cur = conn.execute(
        f"DELETE FROM companies WHERE company_id IN ({placeholders})",
        tuple(company_ids),
    )
    conn.commit()
    return int(cur.rowcount or 0)
