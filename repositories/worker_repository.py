# -*- coding: utf-8 -*-
"""
프로그램명 : worker_repository.py
파일경로   : repositories/worker_repository.py
기능설명   : SmartSite 작업자관리 Repository
화면설명   :
    - 작업자 데이터의 DB 생성/보정/조회/저장/삭제 처리
    - 코드테이블(dtl_code) 연동 시 nationality/company/job_type/skill_level 한글명 조회
    - 코드테이블이 없어도 프로그램이 중단되지 않도록 방어 처리
사용테이블 :
    - mst_worker
    - mst_code (존재 시 참조)
    - dtl_code (존재 시 참조)
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : 최초 작성
주의사항   :
    - DROP 사용 금지, ALTER TABLE ADD COLUMN 방식으로 보정
    - dtl_code 참조 실패 시 코드값 원문으로 대체 표시
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd


# =============================================================================
# 상수 정의
# =============================================================================

WORKER_TABLE_NAME = "mst_worker"

WORKER_COLUMNS: List[Tuple[str, str]] = [
    ("worker_id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("worker_no", "TEXT"),
    ("worker_name", "TEXT"),
    ("resident_no", "TEXT"),
    ("nationality_code", "TEXT"),
    ("phone_no", "TEXT"),
    ("emergency_phone_no", "TEXT"),
    ("address", "TEXT"),
    ("detail_address", "TEXT"),
    ("bank_name", "TEXT"),
    ("account_no", "TEXT"),
    ("account_holder", "TEXT"),
    ("company_code", "TEXT"),
    ("job_type_code", "TEXT"),
    ("skill_level_code", "TEXT"),
    ("safety_edu_yn", "TEXT DEFAULT 'N'"),
    ("health_check_yn", "TEXT DEFAULT 'N'"),
    ("hire_date", "TEXT"),
    ("retire_date", "TEXT"),
    ("use_yn", "TEXT DEFAULT 'Y'"),
    ("sort_order", "INTEGER DEFAULT 0"),
    ("remark", "TEXT"),
    ("created_by", "TEXT"),
    ("created_at", "TEXT"),
    ("updated_by", "TEXT"),
    ("updated_at", "TEXT"),
]

WORKER_COLUMN_NAMES: List[str] = [column_name for column_name, _ in WORKER_COLUMNS]


# =============================================================================
# 공통 유틸리티
# =============================================================================

def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    지정 테이블 존재 여부 확인
    """
    sql = """
        SELECT COUNT(1)
        FROM sqlite_master
        WHERE type='table'
          AND name=?
    """
    row = conn.execute(sql, (table_name,)).fetchone()
    return bool(row and row[0] > 0)


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """
    PRAGMA table_info 로 실제 테이블 컬럼 목록 조회
    """
    if not _table_exists(conn, table_name):
        return []

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def _normalize_text(value: Any) -> str:
    """
    값 정규화
    - None -> 빈문자열
    - 문자열 앞뒤 공백 제거
    """
    if value is None:
        return ""
    return str(value).strip()


def _normalize_yn(value: Any, default_value: str = "N") -> str:
    """
    Y/N 값 정규화
    """
    text = _normalize_text(value).upper()
    if text not in {"Y", "N"}:
        return default_value
    return text


def _normalize_int(value: Any, default_value: int = 0) -> int:
    """
    정수값 정규화
    """
    try:
        if value in (None, ""):
            return default_value
        return int(value)
    except Exception:
        return default_value


def _normalize_date_text(value: Any) -> str:
    """
    날짜 문자열 정규화
    - None / NaN / 빈값은 빈문자열 반환
    - pandas Timestamp 등은 YYYY-MM-DD 형태로 보정
    """
    if value is None:
        return ""

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")

    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""

    if len(text) >= 10:
        return text[:10]

    return text


def _get_now_text() -> str:
    """
    현재시각 문자열 반환
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# 테이블 생성 / 보정
# =============================================================================

def create_worker_table(conn: sqlite3.Connection) -> None:
    """
    mst_worker 표준 테이블 생성
    """
    sql = f"""
        CREATE TABLE IF NOT EXISTS {WORKER_TABLE_NAME} (
            worker_id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_no TEXT,
            worker_name TEXT,
            resident_no TEXT,
            nationality_code TEXT,
            phone_no TEXT,
            emergency_phone_no TEXT,
            address TEXT,
            detail_address TEXT,
            bank_name TEXT,
            account_no TEXT,
            account_holder TEXT,
            company_code TEXT,
            job_type_code TEXT,
            skill_level_code TEXT,
            safety_edu_yn TEXT DEFAULT 'N',
            health_check_yn TEXT DEFAULT 'N',
            hire_date TEXT,
            retire_date TEXT,
            use_yn TEXT DEFAULT 'Y',
            sort_order INTEGER DEFAULT 0,
            remark TEXT,
            created_by TEXT,
            created_at TEXT,
            updated_by TEXT,
            updated_at TEXT
        )
    """
    conn.execute(sql)
    conn.commit()


def ensure_worker_table_schema(conn: sqlite3.Connection) -> None:
    """
    mst_worker 표준 컬럼 보정
    - 기존 테이블 유지
    - 누락 컬럼만 ALTER TABLE ADD COLUMN 처리
    """
    create_worker_table(conn)

    existing_columns = set(_get_table_columns(conn, WORKER_TABLE_NAME))

    for column_name, column_type in WORKER_COLUMNS:
        if column_name not in existing_columns:
            conn.execute(
                f"ALTER TABLE {WORKER_TABLE_NAME} ADD COLUMN {column_name} {column_type}"
            )

    conn.commit()


# =============================================================================
# 코드 옵션 조회
# =============================================================================

def get_dtl_code_options_by_mst(
    conn: sqlite3.Connection,
    mst_code_value: str,
) -> List[Dict[str, str]]:
    """
    dtl_code 에서 특정 mst_code 기준 옵션 조회
    - dtl_code 또는 mst_code 미존재 시 빈리스트 반환
    - code_name 한글명 우선 사용
    """
    if not _table_exists(conn, "dtl_code"):
        return []

    dtl_columns = set(_get_table_columns(conn, "dtl_code"))
    required_columns = {"mst_code_id", "dtl_code", "code_name", "use_yn"}
    if not required_columns.issubset(dtl_columns):
        return []

    if not _table_exists(conn, "mst_code"):
        return []

    mst_columns = set(_get_table_columns(conn, "mst_code"))
    if not {"mst_code_id", "mst_code", "code_name"}.issubset(mst_columns):
        return []

    sql = """
        SELECT
            d.dtl_code AS code,
            COALESCE(NULLIF(d.code_name, ''), d.dtl_code) AS name
        FROM dtl_code d
        INNER JOIN mst_code m
            ON m.mst_code_id = d.mst_code_id
        WHERE m.mst_code = ?
          AND COALESCE(d.use_yn, 'Y') = 'Y'
        ORDER BY COALESCE(d.sort_order, 0), d.dtl_code
    """
    rows = conn.execute(sql, (mst_code_value,)).fetchall()

    return [
        {
            "code": _normalize_text(row[0]),
            "name": _normalize_text(row[1]),
        }
        for row in rows
    ]


# =============================================================================
# 조회
# =============================================================================

def search_workers(
    conn: sqlite3.Connection,
    keyword: str = "",
    use_yn: str = "",
    company_code: str = "",
    nationality_code: str = "",
) -> List[Dict[str, Any]]:
    """
    작업자 목록 조회
    - 코드테이블 존재 시 code_name 한글명 조회
    - 코드테이블 미존재 시 원본 코드값 표시
    """
    ensure_worker_table_schema(conn)

    dtl_exists = _table_exists(conn, "dtl_code")
    dtl_columns = set(_get_table_columns(conn, "dtl_code")) if dtl_exists else set()
    can_join_dtl = {"dtl_code", "code_name"}.issubset(dtl_columns)

    nationality_name_sql = "w.nationality_code AS nationality_name"
    company_name_sql = "w.company_code AS company_name"
    job_type_name_sql = "w.job_type_code AS job_type_name"
    skill_level_name_sql = "w.skill_level_code AS skill_level_name"
    join_sql = ""

    if can_join_dtl:
        nationality_name_sql = "COALESCE(dn.code_name, w.nationality_code) AS nationality_name"
        company_name_sql = "COALESCE(dc.code_name, w.company_code) AS company_name"
        job_type_name_sql = "COALESCE(dj.code_name, w.job_type_code) AS job_type_name"
        skill_level_name_sql = "COALESCE(ds.code_name, w.skill_level_code) AS skill_level_name"
        join_sql = """
            LEFT JOIN dtl_code dn ON dn.dtl_code = w.nationality_code
            LEFT JOIN dtl_code dc ON dc.dtl_code = w.company_code
            LEFT JOIN dtl_code dj ON dj.dtl_code = w.job_type_code
            LEFT JOIN dtl_code ds ON ds.dtl_code = w.skill_level_code
        """

    sql = f"""
        SELECT
            w.worker_id,
            w.worker_no,
            w.worker_name,
            w.resident_no,
            w.nationality_code,
            {nationality_name_sql},
            w.phone_no,
            w.emergency_phone_no,
            w.address,
            w.detail_address,
            w.bank_name,
            w.account_no,
            w.account_holder,
            w.company_code,
            {company_name_sql},
            w.job_type_code,
            {job_type_name_sql},
            w.skill_level_code,
            {skill_level_name_sql},
            w.safety_edu_yn,
            w.health_check_yn,
            w.hire_date,
            w.retire_date,
            w.use_yn,
            w.sort_order,
            w.remark,
            w.created_by,
            w.created_at,
            w.updated_by,
            w.updated_at
        FROM mst_worker w
        {join_sql}
        WHERE 1=1
    """

    params: List[Any] = []

    if keyword.strip():
        sql += """
            AND (
                COALESCE(w.worker_no, '') LIKE ?
                OR COALESCE(w.worker_name, '') LIKE ?
                OR COALESCE(w.phone_no, '') LIKE ?
                OR COALESCE(w.company_code, '') LIKE ?
                OR COALESCE(w.job_type_code, '') LIKE ?
                OR COALESCE(w.skill_level_code, '') LIKE ?
                OR COALESCE(w.remark, '') LIKE ?
            )
        """
        keyword_like = f"%{keyword.strip()}%"
        params.extend([keyword_like] * 7)

    if use_yn.strip():
        sql += " AND COALESCE(w.use_yn, 'Y') = ? "
        params.append(use_yn.strip())

    if company_code.strip():
        sql += " AND COALESCE(w.company_code, '') = ? "
        params.append(company_code.strip())

    if nationality_code.strip():
        sql += " AND COALESCE(w.nationality_code, '') = ? "
        params.append(nationality_code.strip())

    sql += """
        ORDER BY
            COALESCE(w.sort_order, 0),
            COALESCE(w.worker_no, ''),
            COALESCE(w.worker_name, '')
    """

    rows = conn.execute(sql, params).fetchall()
    columns = [
        "worker_id",
        "worker_no",
        "worker_name",
        "resident_no",
        "nationality_code",
        "nationality_name",
        "phone_no",
        "emergency_phone_no",
        "address",
        "detail_address",
        "bank_name",
        "account_no",
        "account_holder",
        "company_code",
        "company_name",
        "job_type_code",
        "job_type_name",
        "skill_level_code",
        "skill_level_name",
        "safety_edu_yn",
        "health_check_yn",
        "hire_date",
        "retire_date",
        "use_yn",
        "sort_order",
        "remark",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
    ]

    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result


def get_worker_detail(conn: sqlite3.Connection, worker_id: int) -> Optional[Dict[str, Any]]:
    """
    작업자 상세 조회
    """
    workers = search_workers(conn)
    for item in workers:
        if int(item.get("worker_id", 0)) == int(worker_id):
            return item
    return None


# =============================================================================
# 저장
# =============================================================================

def save_worker(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    actor: str = "system",
) -> int:
    """
    작업자 등록/수정 저장
    - worker_id 존재 시 수정
    - worker_id 미존재 시 신규 등록
    """
    ensure_worker_table_schema(conn)

    now_text = _get_now_text()

    worker_id = data.get("worker_id")
    worker_no = _normalize_text(data.get("worker_no"))
    worker_name = _normalize_text(data.get("worker_name"))
    resident_no = _normalize_text(data.get("resident_no"))
    nationality_code = _normalize_text(data.get("nationality_code"))
    phone_no = _normalize_text(data.get("phone_no"))
    emergency_phone_no = _normalize_text(data.get("emergency_phone_no"))
    address = _normalize_text(data.get("address"))
    detail_address = _normalize_text(data.get("detail_address"))
    bank_name = _normalize_text(data.get("bank_name"))
    account_no = _normalize_text(data.get("account_no"))
    account_holder = _normalize_text(data.get("account_holder"))
    company_code = _normalize_text(data.get("company_code"))
    job_type_code = _normalize_text(data.get("job_type_code"))
    skill_level_code = _normalize_text(data.get("skill_level_code"))
    safety_edu_yn = _normalize_yn(data.get("safety_edu_yn"), "N")
    health_check_yn = _normalize_yn(data.get("health_check_yn"), "N")
    hire_date = _normalize_date_text(data.get("hire_date"))
    retire_date = _normalize_date_text(data.get("retire_date"))
    use_yn = _normalize_yn(data.get("use_yn"), "Y")
    sort_order = _normalize_int(data.get("sort_order"), 0)
    remark = _normalize_text(data.get("remark"))

    if not worker_name:
        raise ValueError("작업자명은 필수입니다.")

    if worker_id:
        sql = """
            UPDATE mst_worker
            SET
                worker_no = ?,
                worker_name = ?,
                resident_no = ?,
                nationality_code = ?,
                phone_no = ?,
                emergency_phone_no = ?,
                address = ?,
                detail_address = ?,
                bank_name = ?,
                account_no = ?,
                account_holder = ?,
                company_code = ?,
                job_type_code = ?,
                skill_level_code = ?,
                safety_edu_yn = ?,
                health_check_yn = ?,
                hire_date = ?,
                retire_date = ?,
                use_yn = ?,
                sort_order = ?,
                remark = ?,
                updated_by = ?,
                updated_at = ?
            WHERE worker_id = ?
        """
        conn.execute(
            sql,
            (
                worker_no,
                worker_name,
                resident_no,
                nationality_code,
                phone_no,
                emergency_phone_no,
                address,
                detail_address,
                bank_name,
                account_no,
                account_holder,
                company_code,
                job_type_code,
                skill_level_code,
                safety_edu_yn,
                health_check_yn,
                hire_date,
                retire_date,
                use_yn,
                sort_order,
                remark,
                actor,
                now_text,
                int(worker_id),
            ),
        )
        conn.commit()
        return int(worker_id)

    sql = """
        INSERT INTO mst_worker (
            worker_no,
            worker_name,
            resident_no,
            nationality_code,
            phone_no,
            emergency_phone_no,
            address,
            detail_address,
            bank_name,
            account_no,
            account_holder,
            company_code,
            job_type_code,
            skill_level_code,
            safety_edu_yn,
            health_check_yn,
            hire_date,
            retire_date,
            use_yn,
            sort_order,
            remark,
            created_by,
            created_at,
            updated_by,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor = conn.execute(
        sql,
        (
            worker_no,
            worker_name,
            resident_no,
            nationality_code,
            phone_no,
            emergency_phone_no,
            address,
            detail_address,
            bank_name,
            account_no,
            account_holder,
            company_code,
            job_type_code,
            skill_level_code,
            safety_edu_yn,
            health_check_yn,
            hire_date,
            retire_date,
            use_yn,
            sort_order,
            remark,
            actor,
            now_text,
            actor,
            now_text,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def delete_workers(conn: sqlite3.Connection, worker_ids: Sequence[int]) -> int:
    """
    작업자 다건 삭제
    """
    ensure_worker_table_schema(conn)

    valid_ids = [int(worker_id) for worker_id in worker_ids if str(worker_id).strip()]
    if not valid_ids:
        return 0

    placeholders = ",".join(["?"] * len(valid_ids))
    sql = f"DELETE FROM mst_worker WHERE worker_id IN ({placeholders})"
    cursor = conn.execute(sql, valid_ids)
    conn.commit()
    return int(cursor.rowcount)


# =============================================================================
# 엑셀 업로드 / 다운로드
# =============================================================================

def get_worker_export_rows(
    conn: sqlite3.Connection,
    worker_ids: Optional[Sequence[int]] = None,
) -> List[Dict[str, Any]]:
    """
    엑셀 다운로드용 작업자 데이터 조회
    """
    rows = search_workers(conn)

    if worker_ids:
        worker_id_set = {int(worker_id) for worker_id in worker_ids}
        rows = [row for row in rows if int(row.get("worker_id", 0)) in worker_id_set]

    export_rows: List[Dict[str, Any]] = []
    for row in rows:
        export_rows.append(
            {
                "worker_id": row.get("worker_id", ""),
                "worker_no": row.get("worker_no", ""),
                "worker_name": row.get("worker_name", ""),
                "resident_no": row.get("resident_no", ""),
                "nationality_code": row.get("nationality_code", ""),
                "phone_no": row.get("phone_no", ""),
                "emergency_phone_no": row.get("emergency_phone_no", ""),
                "address": row.get("address", ""),
                "detail_address": row.get("detail_address", ""),
                "bank_name": row.get("bank_name", ""),
                "account_no": row.get("account_no", ""),
                "account_holder": row.get("account_holder", ""),
                "company_code": row.get("company_code", ""),
                "job_type_code": row.get("job_type_code", ""),
                "skill_level_code": row.get("skill_level_code", ""),
                "safety_edu_yn": row.get("safety_edu_yn", ""),
                "health_check_yn": row.get("health_check_yn", ""),
                "hire_date": row.get("hire_date", ""),
                "retire_date": row.get("retire_date", ""),
                "use_yn": row.get("use_yn", ""),
                "sort_order": row.get("sort_order", 0),
                "remark": row.get("remark", ""),
                "created_by": row.get("created_by", ""),
                "created_at": row.get("created_at", ""),
                "updated_by": row.get("updated_by", ""),
                "updated_at": row.get("updated_at", ""),
            }
        )

    return export_rows


def get_worker_sample_rows() -> List[Dict[str, Any]]:
    """
    샘플 엑셀용 기본 예시 데이터 반환
    """
    return [
        {
            "worker_id": "",
            "worker_no": "WK-0001",
            "worker_name": "홍길동",
            "resident_no": "900101-1******",
            "nationality_code": "KOR",
            "phone_no": "010-1111-2222",
            "emergency_phone_no": "010-3333-4444",
            "address": "서울특별시 강남구",
            "detail_address": "스마트로 100",
            "bank_name": "국민은행",
            "account_no": "123-456-789012",
            "account_holder": "홍길동",
            "company_code": "COMP001",
            "job_type_code": "JOB001",
            "skill_level_code": "SKILL_A",
            "safety_edu_yn": "Y",
            "health_check_yn": "Y",
            "hire_date": "2026-03-01",
            "retire_date": "",
            "use_yn": "Y",
            "sort_order": 1,
            "remark": "샘플 작업자",
            "created_by": "",
            "created_at": "",
            "updated_by": "",
            "updated_at": "",
        }
    ]


def bulk_upsert_workers(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    actor: str = "system",
) -> Dict[str, int]:
    """
    엑셀 업로드 데이터 일괄 저장
    - worker_id 있으면 수정 시도
    - worker_id 없으면 신규 등록
    - worker_name 없으면 해당 행 건너뜀
    """
    ensure_worker_table_schema(conn)

    if df is None or df.empty:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    inserted = 0
    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        row_data = {column: row[column] for column in df.columns}

        worker_name = _normalize_text(row_data.get("worker_name"))
        if not worker_name:
            skipped += 1
            continue

        worker_id_value = _normalize_text(row_data.get("worker_id"))
        if worker_id_value:
            exists = conn.execute(
                "SELECT COUNT(1) FROM mst_worker WHERE worker_id = ?",
                (int(worker_id_value),),
            ).fetchone()
            if exists and int(exists[0]) > 0:
                save_worker(conn, row_data, actor=actor)
                updated += 1
            else:
                row_data["worker_id"] = None
                save_worker(conn, row_data, actor=actor)
                inserted += 1
        else:
            save_worker(conn, row_data, actor=actor)
            inserted += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}