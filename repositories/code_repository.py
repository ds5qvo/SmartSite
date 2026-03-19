# -*- coding: utf-8 -*-
"""
프로그램명 : code_repository.py
파일경로   : repositories/code_repository.py
기능설명   : SmartSite 코드관리 Repository
화면설명   :
    - 상위코드(mst_code) / 상세코드(dtl_code) / 코드이력(hist_code) 관리
    - mst_code 표준 컬럼 재정비
    - 누락 컬럼(ip_address, mac_address) 추가
    - 불필요 컬럼(mst_code_name, remark) 제거를 위해 재구성 수행
    - 기본코드 자동 입력
    - 코드 CRUD 및 hist_code 저장
사용테이블 :
    - mst_code
    - dtl_code
    - hist_code
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : mst_code 컬럼 구조 재정비
    - 2026-03-16 : ip_address / mac_address 추가
    - 2026-03-16 : mst_code_name / remark 제거 반영
주의사항   :
    - mst_code 는 재생성 방식으로 컬럼 정리
    - 백업 테이블은 삭제하지 않음
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


# =============================================================================
# 공통 유틸
# =============================================================================
def _now_str() -> str:
    """현재 일시 문자열 반환"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_suffix() -> str:
    """백업용 suffix 반환"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """테이블 존재 여부 확인"""
    sql = """
        SELECT name
          FROM sqlite_master
         WHERE type='table'
           AND name=?
    """
    row = conn.execute(sql, (table_name,)).fetchone()
    return row is not None


def _get_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """테이블 컬럼 목록 조회"""
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
    """누락 컬럼 자동 추가"""
    columns = _get_columns(conn, table_name)
    if column_name not in columns:
        conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}"
        )
        conn.commit()


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    """sqlite row -> dict 변환"""
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _safe_json(data: Any) -> str:
    """JSON 직렬화"""
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


# =============================================================================
# mst_code 재구성
# =============================================================================
def _rebuild_mst_code_table(conn: sqlite3.Connection) -> None:
    """
    mst_code 표준 구조로 재구성
    최종 컬럼:
      mst_code_id, mst_code, code_name, use_yn, sort_order,
      ip_address, mac_address, created_by, created_at, updated_by, updated_at

    제거 대상:
      mst_code_name, remark
    """
    if not _table_exists(conn, "mst_code"):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mst_code (
                mst_code_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                mst_code      TEXT NOT NULL UNIQUE,
                code_name     TEXT NOT NULL,
                use_yn        TEXT DEFAULT 'Y',
                sort_order    INTEGER DEFAULT 0,
                ip_address    TEXT,
                mac_address   TEXT,
                created_by    TEXT,
                created_at    TEXT,
                updated_by    TEXT,
                updated_at    TEXT
            )
            """
        )
        conn.commit()
        return

    old_columns = _get_columns(conn, "mst_code")
    target_columns = [
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

    if old_columns == target_columns:
        return

    backup_table_name = f"mst_code_backup_{_now_suffix()}"
    conn.execute(f"ALTER TABLE mst_code RENAME TO {backup_table_name}")
    conn.commit()

    conn.execute(
        """
        CREATE TABLE mst_code (
            mst_code_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            mst_code      TEXT NOT NULL UNIQUE,
            code_name     TEXT NOT NULL,
            use_yn        TEXT DEFAULT 'Y',
            sort_order    INTEGER DEFAULT 0,
            ip_address    TEXT,
            mac_address   TEXT,
            created_by    TEXT,
            created_at    TEXT,
            updated_by    TEXT,
            updated_at    TEXT
        )
        """
    )
    conn.commit()

    backup_columns = _get_columns(conn, backup_table_name)

    now = _now_str()
    select_expr_map = {
        "mst_code_id": "mst_code_id" if "mst_code_id" in backup_columns else "NULL",
        "mst_code": "mst_code" if "mst_code" in backup_columns else "''",
        "code_name": "code_name" if "code_name" in backup_columns else "''",
        "use_yn": "use_yn" if "use_yn" in backup_columns else "'Y'",
        "sort_order": "sort_order" if "sort_order" in backup_columns else "0",
        "ip_address": "ip_address" if "ip_address" in backup_columns else "NULL",
        "mac_address": "mac_address" if "mac_address" in backup_columns else "NULL",
        "created_by": "created_by" if "created_by" in backup_columns else "'system'",
        "created_at": "created_at" if "created_at" in backup_columns else f"'{now}'",
        "updated_by": "updated_by" if "updated_by" in backup_columns else "'system'",
        "updated_at": "updated_at" if "updated_at" in backup_columns else f"'{now}'",
    }

    insert_columns = list(select_expr_map.keys())
    select_sql = ", ".join(select_expr_map[col] for col in insert_columns)

    conn.execute(
        f"""
        INSERT INTO mst_code ({", ".join(insert_columns)})
        SELECT {select_sql}
          FROM {backup_table_name}
        """
    )
    conn.commit()


# =============================================================================
# 테이블 자동 생성 / 구조 보정
# =============================================================================
def ensure_code_tables(conn: sqlite3.Connection) -> None:
    """
    코드관리 관련 테이블 자동 생성 및 보정
    - mst_code
    - dtl_code
    - hist_code
    """
    conn.row_factory = sqlite3.Row

    # mst_code 재구성
    _rebuild_mst_code_table(conn)

    # dtl_code
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dtl_code (
            dtl_code_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            mst_code_id   INTEGER NOT NULL,
            dtl_code      TEXT NOT NULL,
            code_name     TEXT NOT NULL,
            use_yn        TEXT DEFAULT 'Y',
            sort_order    INTEGER DEFAULT 0,
            remark        TEXT,
            created_by    TEXT,
            created_at    TEXT,
            updated_by    TEXT,
            updated_at    TEXT,
            UNIQUE(mst_code_id, dtl_code)
        )
        """
    )

    # hist_code
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hist_code (
            hist_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            target_table  TEXT NOT NULL,
            action_type   TEXT NOT NULL,
            mst_code_id   INTEGER,
            dtl_code_id   INTEGER,
            old_data      TEXT,
            new_data      TEXT,
            action_by     TEXT,
            action_at     TEXT
        )
        """
    )
    conn.commit()

    # dtl_code 보정
    dtl_required = {
        "dtl_code_id": "INTEGER",
        "mst_code_id": "INTEGER",
        "dtl_code": "TEXT",
        "code_name": "TEXT",
        "use_yn": "TEXT DEFAULT 'Y'",
        "sort_order": "INTEGER DEFAULT 0",
        "remark": "TEXT",
        "created_by": "TEXT",
        "created_at": "TEXT",
        "updated_by": "TEXT",
        "updated_at": "TEXT",
    }
    for col_name, col_type in dtl_required.items():
        _add_column_if_missing(conn, "dtl_code", col_name, col_type)

    # hist_code 보정
    hist_required = {
        "hist_id": "INTEGER",
        "target_table": "TEXT",
        "action_type": "TEXT",
        "mst_code_id": "INTEGER",
        "dtl_code_id": "INTEGER",
        "old_data": "TEXT",
        "new_data": "TEXT",
        "action_by": "TEXT",
        "action_at": "TEXT",
    }
    for col_name, col_type in hist_required.items():
        _add_column_if_missing(conn, "hist_code", col_name, col_type)

    conn.commit()


# =============================================================================
# 기본 코드 자동 입력
# =============================================================================
def seed_default_codes(conn: sqlite3.Connection, actor: str = "system") -> None:
    """기본 상위코드 / 상세코드 자동 입력"""
    ensure_code_tables(conn)

    default_master_codes = [
        {"mst_code": "ROLE", "code_name": "권한", "sort_order": 1},
        {"mst_code": "DEPT", "code_name": "부서", "sort_order": 2},
        {"mst_code": "NATIONALITY", "code_name": "국적", "sort_order": 3},
        {"mst_code": "COMPANY", "code_name": "업체", "sort_order": 4},
        {"mst_code": "JOB_TYPE", "code_name": "직종", "sort_order": 5},
        {"mst_code": "SKILL_LEVEL", "code_name": "숙련도", "sort_order": 6},
    ]

    default_detail_codes = {
        "ROLE": [
            ("SYS_ADMIN", "시스템관리자", 1),
            ("SUPER_ADMIN", "최고관리자", 2),
            ("TOP_ADMIN", "최상위자", 3),
            ("ADMIN", "관리자", 4),
            ("USER", "일반사용자", 5),
        ],
        "DEPT": [
            ("HQ", "본사", 1),
            ("SALES", "영업", 2),
            ("SITE", "현장", 3),
            ("ADMIN", "관리", 4),
        ],
        "NATIONALITY": [
            ("KR", "대한민국", 1),
            ("VN", "베트남", 2),
            ("TH", "태국", 3),
            ("CN", "중국", 4),
        ],
        "COMPANY": [
            ("CMP001", "기본업체1", 1),
            ("CMP002", "기본업체2", 2),
        ],
        "JOB_TYPE": [
            ("ASBESTOS", "석면해체", 1),
            ("SAFETY", "안전관리", 2),
            ("MANAGER", "현장관리", 3),
            ("ETC", "기타", 99),
        ],
        "SKILL_LEVEL": [
            ("A", "상", 1),
            ("B", "중", 2),
            ("C", "하", 3),
        ],
    }

    now = _now_str()

    for mst in default_master_codes:
        row = conn.execute(
            "SELECT mst_code_id FROM mst_code WHERE mst_code = ?",
            (mst["mst_code"],),
        ).fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO mst_code (
                    mst_code, code_name, use_yn, sort_order,
                    ip_address, mac_address,
                    created_by, created_at, updated_by, updated_at
                )
                VALUES (?, ?, 'Y', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mst["mst_code"],
                    mst["code_name"],
                    mst["sort_order"],
                    "",
                    "",
                    actor,
                    now,
                    actor,
                    now,
                ),
            )
            conn.commit()

    for mst_code, details in default_detail_codes.items():
        mst_row = conn.execute(
            "SELECT mst_code_id FROM mst_code WHERE mst_code = ?",
            (mst_code,),
        ).fetchone()
        if mst_row is None:
            continue

        mst_code_id = mst_row["mst_code_id"]

        for dtl_code, code_name, sort_order in details:
            row = conn.execute(
                """
                SELECT dtl_code_id
                  FROM dtl_code
                 WHERE mst_code_id = ?
                   AND dtl_code = ?
                """,
                (mst_code_id, dtl_code),
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO dtl_code (
                        mst_code_id, dtl_code, code_name, use_yn, sort_order, remark,
                        created_by, created_at, updated_by, updated_at
                    )
                    VALUES (?, ?, ?, 'Y', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mst_code_id,
                        dtl_code,
                        code_name,
                        sort_order,
                        "기본 자동 생성 코드",
                        actor,
                        now,
                        actor,
                        now,
                    ),
                )
                conn.commit()


def initialize_code_setup(conn: sqlite3.Connection, actor: str = "system") -> None:
    """코드관리 초기 설정 일괄 수행"""
    ensure_code_tables(conn)
    seed_default_codes(conn, actor=actor)


# =============================================================================
# 이력 저장
# =============================================================================
def insert_hist_code(
    conn: sqlite3.Connection,
    target_table: str,
    action_type: str,
    mst_code_id: Optional[int] = None,
    dtl_code_id: Optional[int] = None,
    old_data: Optional[Dict[str, Any]] = None,
    new_data: Optional[Dict[str, Any]] = None,
    action_by: str = "system",
) -> None:
    """코드 이력(hist_code) 저장"""
    ensure_code_tables(conn)

    conn.execute(
        """
        INSERT INTO hist_code (
            target_table, action_type, mst_code_id, dtl_code_id,
            old_data, new_data, action_by, action_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            target_table,
            action_type,
            mst_code_id,
            dtl_code_id,
            _safe_json(old_data),
            _safe_json(new_data),
            action_by,
            _now_str(),
        ),
    )
    conn.commit()


# =============================================================================
# 조회
# =============================================================================
def get_mst_code_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """상위코드 콤보용 조회"""
    ensure_code_tables(conn)
    rows = conn.execute(
        """
        SELECT mst_code_id, mst_code, code_name, use_yn, sort_order
          FROM mst_code
         ORDER BY sort_order, mst_code, mst_code_id
        """
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def list_mst_codes(
    conn: sqlite3.Connection,
    search_text: str = "",
    use_yn: str = "",
) -> pd.DataFrame:
    """상위코드 목록 조회"""
    ensure_code_tables(conn)

    sql = """
        SELECT
            m.mst_code_id,
            m.mst_code,
            m.code_name,
            m.use_yn,
            m.sort_order,
            COALESCE(m.ip_address, '') AS ip_address,
            COALESCE(m.mac_address, '') AS mac_address,
            COALESCE(m.created_by, '') AS created_by,
            COALESCE(m.created_at, '') AS created_at,
            COALESCE(m.updated_by, '') AS updated_by,
            COALESCE(m.updated_at, '') AS updated_at,
            (
                SELECT COUNT(*)
                  FROM dtl_code d
                 WHERE d.mst_code_id = m.mst_code_id
            ) AS dtl_count
        FROM mst_code m
        WHERE 1=1
    """
    params: List[Any] = []

    if search_text.strip():
        keyword = f"%{search_text.strip()}%"
        sql += """
            AND (
                m.mst_code LIKE ?
                OR m.code_name LIKE ?
                OR COALESCE(m.ip_address, '') LIKE ?
                OR COALESCE(m.mac_address, '') LIKE ?
            )
        """
        params.extend([keyword, keyword, keyword, keyword])

    if use_yn in ("Y", "N"):
        sql += " AND m.use_yn = ? "
        params.append(use_yn)

    sql += " ORDER BY m.sort_order, m.mst_code, m.mst_code_id "

    return pd.read_sql_query(sql, conn, params=params)


def list_dtl_codes(
    conn: sqlite3.Connection,
    mst_code_id: Optional[int] = None,
    search_text: str = "",
    use_yn: str = "",
) -> pd.DataFrame:
    """상세코드 목록 조회"""
    ensure_code_tables(conn)

    sql = """
        SELECT
            d.dtl_code_id,
            d.mst_code_id,
            COALESCE(m.mst_code, '') AS mst_code,
            COALESCE(m.code_name, '') AS mst_code_name,
            d.dtl_code,
            d.code_name,
            d.use_yn,
            d.sort_order,
            COALESCE(d.remark, '') AS remark,
            COALESCE(d.created_by, '') AS created_by,
            COALESCE(d.created_at, '') AS created_at,
            COALESCE(d.updated_by, '') AS updated_by,
            COALESCE(d.updated_at, '') AS updated_at
        FROM dtl_code d
        LEFT JOIN mst_code m
               ON m.mst_code_id = d.mst_code_id
        WHERE 1=1
    """
    params: List[Any] = []

    if mst_code_id:
        sql += " AND d.mst_code_id = ? "
        params.append(mst_code_id)

    if search_text.strip():
        keyword = f"%{search_text.strip()}%"
        sql += """
            AND (
                d.dtl_code LIKE ?
                OR d.code_name LIKE ?
                OR COALESCE(d.remark, '') LIKE ?
                OR COALESCE(m.mst_code, '') LIKE ?
                OR COALESCE(m.code_name, '') LIKE ?
            )
        """
        params.extend([keyword, keyword, keyword, keyword, keyword])

    if use_yn in ("Y", "N"):
        sql += " AND d.use_yn = ? "
        params.append(use_yn)

    sql += """
        ORDER BY
            COALESCE(m.sort_order, 9999),
            COALESCE(m.mst_code, ''),
            d.sort_order,
            d.dtl_code,
            d.dtl_code_id
    """

    return pd.read_sql_query(sql, conn, params=params)


def get_mst_code(conn: sqlite3.Connection, mst_code_id: int) -> Optional[Dict[str, Any]]:
    """상위코드 단건 조회"""
    ensure_code_tables(conn)
    row = conn.execute(
        """
        SELECT
            mst_code_id, mst_code, code_name, use_yn, sort_order,
            ip_address, mac_address,
            created_by, created_at, updated_by, updated_at
        FROM mst_code
        WHERE mst_code_id = ?
        """,
        (mst_code_id,),
    ).fetchone()
    return _row_to_dict(row)


def get_dtl_code(conn: sqlite3.Connection, dtl_code_id: int) -> Optional[Dict[str, Any]]:
    """상세코드 단건 조회"""
    ensure_code_tables(conn)
    row = conn.execute(
        """
        SELECT
            d.dtl_code_id,
            d.mst_code_id,
            COALESCE(m.mst_code, '') AS mst_code,
            COALESCE(m.code_name, '') AS mst_code_name,
            d.dtl_code,
            d.code_name,
            d.use_yn,
            d.sort_order,
            d.remark,
            d.created_by,
            d.created_at,
            d.updated_by,
            d.updated_at
        FROM dtl_code d
        LEFT JOIN mst_code m
               ON m.mst_code_id = d.mst_code_id
        WHERE d.dtl_code_id = ?
        """,
        (dtl_code_id,),
    ).fetchone()
    return _row_to_dict(row)


# =============================================================================
# 등록 / 수정
# =============================================================================
def save_mst_code(conn: sqlite3.Connection, data: Dict[str, Any], actor: str = "system") -> int:
    """상위코드 등록/수정"""
    ensure_code_tables(conn)

    mst_code_id = int(data.get("mst_code_id") or 0)
    mst_code = str(data.get("mst_code") or "").strip()
    code_name = str(data.get("code_name") or "").strip()
    use_yn = str(data.get("use_yn") or "Y").strip() or "Y"
    sort_order = int(data.get("sort_order") or 0)
    ip_address = str(data.get("ip_address") or "").strip()
    mac_address = str(data.get("mac_address") or "").strip()
    now = _now_str()

    if not mst_code:
        raise ValueError("상위코드를 입력하세요.")
    if not code_name:
        raise ValueError("상위코드명을 입력하세요.")
    if use_yn not in ("Y", "N"):
        use_yn = "Y"

    dup_row = conn.execute(
        """
        SELECT mst_code_id
          FROM mst_code
         WHERE mst_code = ?
           AND mst_code_id != ?
        """,
        (mst_code, mst_code_id),
    ).fetchone()
    if dup_row:
        raise ValueError("동일한 상위코드가 이미 존재합니다.")

    if mst_code_id > 0:
        old_row = get_mst_code(conn, mst_code_id)
        if old_row is None:
            raise ValueError("수정 대상 상위코드를 찾을 수 없습니다.")

        conn.execute(
            """
            UPDATE mst_code
               SET mst_code   = ?,
                   code_name  = ?,
                   use_yn     = ?,
                   sort_order = ?,
                   ip_address = ?,
                   mac_address= ?,
                   updated_by = ?,
                   updated_at = ?
             WHERE mst_code_id = ?
            """,
            (
                mst_code,
                code_name,
                use_yn,
                sort_order,
                ip_address,
                mac_address,
                actor,
                now,
                mst_code_id,
            ),
        )
        conn.commit()

        new_row = get_mst_code(conn, mst_code_id)
        insert_hist_code(
            conn=conn,
            target_table="mst_code",
            action_type="UPDATE",
            mst_code_id=mst_code_id,
            old_data=old_row,
            new_data=new_row,
            action_by=actor,
        )
        return mst_code_id

    conn.execute(
        """
        INSERT INTO mst_code (
            mst_code, code_name, use_yn, sort_order,
            ip_address, mac_address,
            created_by, created_at, updated_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mst_code,
            code_name,
            use_yn,
            sort_order,
            ip_address,
            mac_address,
            actor,
            now,
            actor,
            now,
        ),
    )
    conn.commit()

    new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    new_row = get_mst_code(conn, new_id)

    insert_hist_code(
        conn=conn,
        target_table="mst_code",
        action_type="INSERT",
        mst_code_id=new_id,
        old_data=None,
        new_data=new_row,
        action_by=actor,
    )
    return new_id


def save_dtl_code(conn: sqlite3.Connection, data: Dict[str, Any], actor: str = "system") -> int:
    """상세코드 등록/수정"""
    ensure_code_tables(conn)

    dtl_code_id = int(data.get("dtl_code_id") or 0)
    mst_code_id = int(data.get("mst_code_id") or 0)
    dtl_code = str(data.get("dtl_code") or "").strip()
    code_name = str(data.get("code_name") or "").strip()
    use_yn = str(data.get("use_yn") or "Y").strip() or "Y"
    sort_order = int(data.get("sort_order") or 0)
    remark = str(data.get("remark") or "").strip()
    now = _now_str()

    if mst_code_id <= 0:
        raise ValueError("상위코드를 선택하세요.")
    if not dtl_code:
        raise ValueError("상세코드를 입력하세요.")
    if not code_name:
        raise ValueError("상세코드명을 입력하세요.")
    if use_yn not in ("Y", "N"):
        use_yn = "Y"

    mst_row = conn.execute(
        "SELECT mst_code_id FROM mst_code WHERE mst_code_id = ?",
        (mst_code_id,),
    ).fetchone()
    if mst_row is None:
        raise ValueError("선택한 상위코드가 존재하지 않습니다.")

    dup_row = conn.execute(
        """
        SELECT dtl_code_id
          FROM dtl_code
         WHERE mst_code_id = ?
           AND dtl_code = ?
           AND dtl_code_id != ?
        """,
        (mst_code_id, dtl_code, dtl_code_id),
    ).fetchone()
    if dup_row:
        raise ValueError("동일한 상세코드가 이미 존재합니다.")

    if dtl_code_id > 0:
        old_row = get_dtl_code(conn, dtl_code_id)
        if old_row is None:
            raise ValueError("수정 대상 상세코드를 찾을 수 없습니다.")

        conn.execute(
            """
            UPDATE dtl_code
               SET mst_code_id = ?,
                   dtl_code    = ?,
                   code_name   = ?,
                   use_yn      = ?,
                   sort_order  = ?,
                   remark      = ?,
                   updated_by  = ?,
                   updated_at  = ?
             WHERE dtl_code_id = ?
            """,
            (
                mst_code_id,
                dtl_code,
                code_name,
                use_yn,
                sort_order,
                remark,
                actor,
                now,
                dtl_code_id,
            ),
        )
        conn.commit()

        new_row = get_dtl_code(conn, dtl_code_id)
        insert_hist_code(
            conn=conn,
            target_table="dtl_code",
            action_type="UPDATE",
            mst_code_id=mst_code_id,
            dtl_code_id=dtl_code_id,
            old_data=old_row,
            new_data=new_row,
            action_by=actor,
        )
        return dtl_code_id

    conn.execute(
        """
        INSERT INTO dtl_code (
            mst_code_id, dtl_code, code_name, use_yn, sort_order, remark,
            created_by, created_at, updated_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mst_code_id,
            dtl_code,
            code_name,
            use_yn,
            sort_order,
            remark,
            actor,
            now,
            actor,
            now,
        ),
    )
    conn.commit()

    new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    new_row = get_dtl_code(conn, new_id)

    insert_hist_code(
        conn=conn,
        target_table="dtl_code",
        action_type="INSERT",
        mst_code_id=mst_code_id,
        dtl_code_id=new_id,
        old_data=None,
        new_data=new_row,
        action_by=actor,
    )
    return new_id


# =============================================================================
# 삭제
# =============================================================================
def delete_mst_codes(
    conn: sqlite3.Connection,
    mst_code_ids: List[int],
    actor: str = "system",
) -> int:
    """상위코드 삭제"""
    ensure_code_tables(conn)

    delete_count = 0
    for mst_code_id in mst_code_ids:
        old_row = get_mst_code(conn, mst_code_id)
        if old_row is None:
            continue

        dtl_cnt_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM dtl_code WHERE mst_code_id = ?",
            (mst_code_id,),
        ).fetchone()

        if dtl_cnt_row and int(dtl_cnt_row["cnt"]) > 0:
            raise ValueError(
                f"상위코드 [{old_row.get('mst_code')}] 에 연결된 상세코드가 있어 삭제할 수 없습니다."
            )

        conn.execute("DELETE FROM mst_code WHERE mst_code_id = ?", (mst_code_id,))
        conn.commit()

        insert_hist_code(
            conn=conn,
            target_table="mst_code",
            action_type="DELETE",
            mst_code_id=mst_code_id,
            old_data=old_row,
            new_data=None,
            action_by=actor,
        )
        delete_count += 1

    return delete_count


def delete_dtl_codes(
    conn: sqlite3.Connection,
    dtl_code_ids: List[int],
    actor: str = "system",
) -> int:
    """상세코드 삭제"""
    ensure_code_tables(conn)

    delete_count = 0
    for dtl_code_id in dtl_code_ids:
        old_row = get_dtl_code(conn, dtl_code_id)
        if old_row is None:
            continue

        conn.execute("DELETE FROM dtl_code WHERE dtl_code_id = ?", (dtl_code_id,))
        conn.commit()

        insert_hist_code(
            conn=conn,
            target_table="dtl_code",
            action_type="DELETE",
            mst_code_id=old_row.get("mst_code_id"),
            dtl_code_id=dtl_code_id,
            old_data=old_row,
            new_data=None,
            action_by=actor,
        )
        delete_count += 1

    return delete_count


# =============================================================================
# 이력 조회
# =============================================================================
def list_hist_codes(
    conn: sqlite3.Connection,
    search_text: str = "",
    limit: int = 200,
) -> pd.DataFrame:
    """코드 이력 조회"""
    ensure_code_tables(conn)

    sql = """
        SELECT
            hist_id,
            target_table,
            action_type,
            mst_code_id,
            dtl_code_id,
            COALESCE(action_by, '') AS action_by,
            COALESCE(action_at, '') AS action_at,
            COALESCE(old_data, '') AS old_data,
            COALESCE(new_data, '') AS new_data
        FROM hist_code
        WHERE 1=1
    """
    params: List[Any] = []

    if search_text.strip():
        keyword = f"%{search_text.strip()}%"
        sql += """
            AND (
                target_table LIKE ?
                OR action_type LIKE ?
                OR COALESCE(action_by, '') LIKE ?
                OR COALESCE(old_data, '') LIKE ?
                OR COALESCE(new_data, '') LIKE ?
            )
        """
        params.extend([keyword, keyword, keyword, keyword, keyword])

    sql += " ORDER BY hist_id DESC LIMIT ? "
    params.append(limit)

    return pd.read_sql_query(sql, conn, params=params)