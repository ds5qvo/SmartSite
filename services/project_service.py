# -*- coding: utf-8 -*-
"""
프로그램명 : project_service.py
파일경로   : services/project_service.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import sqlite3
from io import BytesIO
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.project_repository import (
    delete_projects,
    get_project,
    get_project_options,
    list_projects,
    save_project,
)


def get_project_form_options(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    return get_project_options(conn)


def get_project_list(conn: sqlite3.Connection, search_text: str = "") -> pd.DataFrame:
    return list_projects(conn, search_text=search_text)


def get_project_detail(conn: sqlite3.Connection, project_id: int) -> Optional[Dict[str, Any]]:
    return get_project(conn, project_id)


def save_project_data(conn: sqlite3.Connection, payload: Dict[str, Any], actor: str) -> int:
    return save_project(conn, payload, actor)


def remove_project_rows(conn: sqlite3.Connection, ids: List[int]) -> int:
    return delete_projects(conn, ids)


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    output.seek(0)
    return output.read()
