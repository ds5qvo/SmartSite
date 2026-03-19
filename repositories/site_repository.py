# -*- coding: utf-8 -*-
"""
프로그램명 : site_repository.py
파일경로   : repositories/site_repository.py
기능설명   :
주요기능   :
변경이력   :
주의사항   :
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.common_reference_repository import table_exists


def list_sites(conn: sqlite3.Connection, search_text: str = "") -> pd.DataFrame:
    if not table_exists(conn, "projects"):
        return pd.DataFrame()
    sql = """
        SELECT
            project_id,
            project_code,
            project_name,
            site_name,
            zip_code,
            address,
            address_detail,
            start_date,
            end_date,
            note
          FROM projects
         WHERE 1 = 1
    """
    params: List[Any] = []
    if str(search_text).strip():
        keyword = f"%{str(search_text).strip()}%"
        sql += """
          AND (
                COALESCE(project_code, '') LIKE ?
             OR COALESCE(project_name, '') LIKE ?
             OR COALESCE(site_name, '') LIKE ?
             OR COALESCE(address, '') LIKE ?
          )
        """
        params.extend([keyword, keyword, keyword, keyword])

    sql += " ORDER BY project_id DESC"
    return pd.read_sql_query(sql, conn, params=params)


def get_site_project(conn: sqlite3.Connection, project_id: int) -> Optional[Dict[str, Any]]:
    if not table_exists(conn, "projects"):
        return None
    row = conn.execute(
        """
        SELECT project_id, project_code, project_name, site_name, zip_code, address, address_detail,
               start_date, end_date, note
          FROM projects
         WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()
    return dict(row) if row else None


def save_site_fields(conn: sqlite3.Connection, payload: Dict[str, Any], actor: str) -> int:
    if not table_exists(conn, "projects"):
        raise ValueError("projects 테이블이 없습니다.")
    project_id = int(payload.get("project_id") or 0)
    if not project_id:
        raise ValueError("프로젝트를 선택해 주세요.")
    conn.execute(
        """
        UPDATE projects
           SET site_name = ?,
               zip_code = ?,
               address = ?,
               address_detail = ?,
               start_date = ?,
               end_date = ?,
               note = ?,
               updated_at = CURRENT_TIMESTAMP,
               updated_by = ?
         WHERE project_id = ?
        """,
        (
            payload.get("site_name") or "",
            payload.get("zip_code") or "",
            payload.get("address") or "",
            payload.get("address_detail") or "",
            payload.get("start_date") or None,
            payload.get("end_date") or None,
            payload.get("note") or "",
            actor,
            project_id,
        ),
    )
    return project_id
