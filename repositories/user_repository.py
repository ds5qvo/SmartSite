# -*- coding: utf-8 -*-
"""
프로그램명 : user_repository.py
파일경로   : repositories/user_repository.py
기능설명   : SmartSite 사용자관리 Repository
화면설명   :
    - mst_user 표준 테이블 조회 / 등록 / 수정 / 삭제
    - ROLE / DEPT 코드 한글명 조회
    - 코드 테이블이 없어도 죽지 않도록 방어 처리
    - legacy mst_user 구조와 충돌하지 않도록 표준 컬럼 기준 사용
사용테이블 :
    - mst_user
    - mst_code (있을 때 사용)
    - dtl_code (있을 때 사용)
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : 최초 작성
주의사항   :
    - mst_user 표준 컬럼 기준
    - password_hash 저장, 평문 저장 금지
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


# =============================================================================
# 공통 유틸
# =============================================================================
def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
          FROM sqlite_master
         WHERE type='table'
           AND name=?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _get_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    if not _table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def _add_column_if_missing(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_type_sql: str,
) -> None:
    columns = _get_columns(conn, table_name)
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}")
        conn.commit()


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


# =============================================================================
# 테이블 준비
# =============================================================================
def ensure_user_table(conn: sqlite3.Connection) -> None:
    """
    mst_user 표준 구조 보장
    표준 컬럼 :
      user_id, login_id, user_name, password_hash, role_code, dept_code,
      position_name, phone_no, email, use_yn, sort_order, remark,
      ip_address, mac_address, created_by, created_at, updated_by, updated_at
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mst_user (
            user_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            login_id        TEXT NOT NULL UNIQUE,
            user_name       TEXT NOT NULL,
            password_hash   TEXT NOT NULL,
            role_code       TEXT,
            dept_code       TEXT,
            position_name   TEXT,
            phone_no        TEXT,
            email           TEXT,
            use_yn          TEXT DEFAULT 'Y',
            sort_order      INTEGER DEFAULT 0,
            remark          TEXT,
            ip_address      TEXT,
            mac_address     TEXT,
            created_by      TEXT,
            created_at      TEXT,
            updated_by      TEXT,
            updated_at      TEXT
        )
        """
    )
    conn.commit()

    required_columns = {
        "user_id": "INTEGER",
        "login_id": "TEXT",
        "user_name": "TEXT",
        "password_hash": "TEXT",
        "role_code": "TEXT",
        "dept_code": "TEXT",
        "position_name": "TEXT",
        "phone_no": "TEXT",
        "email": "TEXT",
        "use_yn": "TEXT DEFAULT 'Y'",
        "sort_order": "INTEGER DEFAULT 0",
        "remark": "TEXT",
        "ip_address": "TEXT",
        "mac_address": "TEXT",
        "created_by": "TEXT",
        "created_at": "TEXT",
        "updated_by": "TEXT",
        "updated_at": "TEXT",
    }

    for col_name, col_type in required_columns.items():
        _add_column_if_missing(conn, "mst_user", col_name, col_type)


# =============================================================================
# 코드 옵션 조회
# =============================================================================
def get_role_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    ROLE 코드 목록 조회
    코드테이블 없으면 빈 목록 반환
    """
    if not _table_exists(conn, "mst_code") or not _table_exists(conn, "dtl_code"):
        return []

    sql = """
        SELECT
            d.dtl_code AS code_value,
            d.code_name AS code_name,
            d.sort_order AS sort_order
        FROM mst_code m
        JOIN dtl_code d
          ON d.mst_code_id = m.mst_code_id
        WHERE m.mst_code = 'ROLE'
        ORDER BY d.sort_order, d.dtl_code
    """
    try:
        rows = conn.execute(sql).fetchall()
        return [
            {
                "code_value": row[0],
                "code_name": row[1],
                "sort_order": row[2],
            }
            for row in rows
        ]
    except Exception:
        return []


def get_dept_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    DEPT 코드 목록 조회
    코드테이블 없으면 빈 목록 반환
    """
    if not _table_exists(conn, "mst_code") or not _table_exists(conn, "dtl_code"):
        return []

    sql = """
        SELECT
            d.dtl_code AS code_value,
            d.code_name AS code_name,
            d.sort_order AS sort_order
        FROM mst_code m
        JOIN dtl_code d
          ON d.mst_code_id = m.mst_code_id
        WHERE m.mst_code = 'DEPT'
        ORDER BY d.sort_order, d.dtl_code
    """
    try:
        rows = conn.execute(sql).fetchall()
        return [
            {
                "code_value": row[0],
                "code_name": row[1],
                "sort_order": row[2],
            }
            for row in rows
        ]
    except Exception:
        return []


# =============================================================================
# 조회
# =============================================================================
def list_users(
    conn: sqlite3.Connection,
    search_text: str = "",
    use_yn: str = "",
    role_code: str = "",
    dept_code: str = "",
) -> pd.DataFrame:
    """
    사용자 목록 조회
    코드테이블이 있으면 role_name, dept_name 한글명 조회
    없으면 원코드 표시
    """
    ensure_user_table(conn)

    has_code_tables = _table_exists(conn, "mst_code") and _table_exists(conn, "dtl_code")

    if has_code_tables:
        sql = """
            SELECT
                u.user_id,
                u.login_id,
                u.user_name,
                COALESCE(u.role_code, '') AS role_code,
                COALESCE(dr.code_name, u.role_code) AS role_name,
                COALESCE(u.dept_code, '') AS dept_code,
                COALESCE(dd.code_name, u.dept_code) AS dept_name,
                COALESCE(u.position_name, '') AS position_name,
                COALESCE(u.phone_no, '') AS phone_no,
                COALESCE(u.email, '') AS email,
                COALESCE(u.use_yn, 'Y') AS use_yn,
                COALESCE(u.sort_order, 0) AS sort_order,
                COALESCE(u.remark, '') AS remark,
                COALESCE(u.created_by, '') AS created_by,
                COALESCE(u.created_at, '') AS created_at,
                COALESCE(u.updated_by, '') AS updated_by,
                COALESCE(u.updated_at, '') AS updated_at
            FROM mst_user u
            LEFT JOIN mst_code mr
                   ON mr.mst_code = 'ROLE'
            LEFT JOIN dtl_code dr
                   ON dr.mst_code_id = mr.mst_code_id
                  AND dr.dtl_code = u.role_code
            LEFT JOIN mst_code md
                   ON md.mst_code = 'DEPT'
            LEFT JOIN dtl_code dd
                   ON dd.mst_code_id = md.mst_code_id
                  AND dd.dtl_code = u.dept_code
            WHERE 1=1
        """
    else:
        sql = """
            SELECT
                u.user_id,
                u.login_id,
                u.user_name,
                COALESCE(u.role_code, '') AS role_code,
                COALESCE(u.role_code, '') AS role_name,
                COALESCE(u.dept_code, '') AS dept_code,
                COALESCE(u.dept_code, '') AS dept_name,
                COALESCE(u.position_name, '') AS position_name,
                COALESCE(u.phone_no, '') AS phone_no,
                COALESCE(u.email, '') AS email,
                COALESCE(u.use_yn, 'Y') AS use_yn,
                COALESCE(u.sort_order, 0) AS sort_order,
                COALESCE(u.remark, '') AS remark,
                COALESCE(u.created_by, '') AS created_by,
                COALESCE(u.created_at, '') AS created_at,
                COALESCE(u.updated_by, '') AS updated_by,
                COALESCE(u.updated_at, '') AS updated_at
            FROM mst_user u
            WHERE 1=1
        """

    params: List[Any] = []

    if search_text.strip():
        keyword = f"%{search_text.strip()}%"
        sql += """
            AND (
                u.login_id LIKE ?
                OR u.user_name LIKE ?
                OR COALESCE(u.role_code, '') LIKE ?
                OR COALESCE(u.dept_code, '') LIKE ?
                OR COALESCE(u.position_name, '') LIKE ?
                OR COALESCE(u.phone_no, '') LIKE ?
                OR COALESCE(u.email, '') LIKE ?
                OR COALESCE(u.remark, '') LIKE ?
            )
        """
        params.extend([keyword, keyword, keyword, keyword, keyword, keyword, keyword, keyword])

    if use_yn in ("Y", "N"):
        sql += " AND COALESCE(u.use_yn, 'Y') = ? "
        params.append(use_yn)

    if role_code.strip():
        sql += " AND COALESCE(u.role_code, '') = ? "
        params.append(role_code.strip())

    if dept_code.strip():
        sql += " AND COALESCE(u.dept_code, '') = ? "
        params.append(dept_code.strip())

    sql += " ORDER BY COALESCE(u.sort_order, 0), u.login_id, u.user_id "
    return pd.read_sql_query(sql, conn, params=params)


def get_user(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    """
    사용자 단건 조회
    """
    ensure_user_table(conn)
    conn.row_factory = sqlite3.Row

    row = conn.execute(
        """
        SELECT
            user_id,
            login_id,
            user_name,
            password_hash,
            role_code,
            dept_code,
            position_name,
            phone_no,
            email,
            use_yn,
            sort_order,
            remark,
            ip_address,
            mac_address,
            created_by,
            created_at,
            updated_by,
            updated_at
        FROM mst_user
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()
    return _row_to_dict(row)


# =============================================================================
# 저장
# =============================================================================
def save_user(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    actor: str = "system",
) -> int:
    """
    사용자 등록/수정
    - 등록 시 비밀번호 필수
    - 수정 시 비밀번호 입력 없으면 기존 해시 유지
    """
    ensure_user_table(conn)

    user_id = int(data.get("user_id") or 0)
    login_id = str(data.get("login_id") or "").strip()
    user_name = str(data.get("user_name") or "").strip()
    password = str(data.get("password") or "").strip()
    role_code = str(data.get("role_code") or "").strip()
    dept_code = str(data.get("dept_code") or "").strip()
    position_name = str(data.get("position_name") or "").strip()
    phone_no = str(data.get("phone_no") or "").strip()
    email = str(data.get("email") or "").strip()
    use_yn = str(data.get("use_yn") or "Y").strip() or "Y"
    sort_order = int(data.get("sort_order") or 0)
    remark = str(data.get("remark") or "").strip()
    ip_address = str(data.get("ip_address") or "").strip()
    mac_address = str(data.get("mac_address") or "").strip()
    now = _now_str()

    if not login_id:
        raise ValueError("로그인ID를 입력하세요.")
    if not user_name:
        raise ValueError("사용자명을 입력하세요.")
    if use_yn not in ("Y", "N"):
        use_yn = "Y"

    dup_row = conn.execute(
        """
        SELECT user_id
          FROM mst_user
         WHERE login_id = ?
           AND user_id != ?
        """,
        (login_id, user_id),
    ).fetchone()
    if dup_row:
        raise ValueError("동일한 로그인ID가 이미 존재합니다.")

    if user_id > 0:
        old_row = conn.execute(
            "SELECT password_hash FROM mst_user WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if old_row is None:
            raise ValueError("수정 대상 사용자가 존재하지 않습니다.")

        password_hash = old_row[0]
        if password:
            password_hash = _hash_password(password)

        conn.execute(
            """
            UPDATE mst_user
               SET login_id      = ?,
                   user_name     = ?,
                   password_hash = ?,
                   role_code     = ?,
                   dept_code     = ?,
                   position_name = ?,
                   phone_no      = ?,
                   email         = ?,
                   use_yn        = ?,
                   sort_order    = ?,
                   remark        = ?,
                   ip_address    = ?,
                   mac_address   = ?,
                   updated_by    = ?,
                   updated_at    = ?
             WHERE user_id = ?
            """,
            (
                login_id,
                user_name,
                password_hash,
                role_code,
                dept_code,
                position_name,
                phone_no,
                email,
                use_yn,
                sort_order,
                remark,
                ip_address,
                mac_address,
                actor,
                now,
                user_id,
            ),
        )
        conn.commit()
        return user_id

    if not password:
        raise ValueError("신규 등록 시 비밀번호를 입력하세요.")

    password_hash = _hash_password(password)

    conn.execute(
        """
        INSERT INTO mst_user (
            login_id, user_name, password_hash, role_code, dept_code,
            position_name, phone_no, email, use_yn, sort_order, remark,
            ip_address, mac_address, created_by, created_at, updated_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            login_id,
            user_name,
            password_hash,
            role_code,
            dept_code,
            position_name,
            phone_no,
            email,
            use_yn,
            sort_order,
            remark,
            ip_address,
            mac_address,
            actor,
            now,
            actor,
            now,
        ),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# =============================================================================
# 삭제
# =============================================================================
def delete_users(conn: sqlite3.Connection, user_ids: List[int]) -> int:
    """
    사용자 삭제
    """
    ensure_user_table(conn)

    delete_count = 0
    for user_id in user_ids:
        row = conn.execute("SELECT user_id FROM mst_user WHERE user_id = ?", (user_id,)).fetchone()
        if row is None:
            continue
        conn.execute("DELETE FROM mst_user WHERE user_id = ?", (user_id,))
        delete_count += 1

    conn.commit()
    return delete_count