# -*- coding: utf-8 -*-
"""
프로그램명 : worker_service.py
파일경로   : services/worker_service.py
기능설명   : SmartSite 작업자관리 Service
화면설명   :
    - 작업자 화면에서 사용하는 비즈니스 로직 제공
    - 조회/상세/저장/삭제/엑셀 업로드/다운로드 처리
    - nationality/company/job_type/skill_level 코드 옵션 제공
사용테이블 :
    - mst_worker
    - mst_code (존재 시 참조)
    - dtl_code (존재 시 참조)
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : 최초 작성
주의사항   :
    - Repository 계층 호출 전후 입력값 정규화 및 화면용 DataFrame 생성
"""

from __future__ import annotations

import io
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from repositories.worker_repository import (
    bulk_upsert_workers,
    delete_workers,
    ensure_worker_table_schema,
    get_dtl_code_options_by_mst,
    get_worker_detail,
    get_worker_export_rows,
    get_worker_sample_rows,
    save_worker,
    search_workers,
)


# =============================================================================
# 상수 정의
# =============================================================================

ADMIN_ROLES = {"ADMIN", "TOP_ADMIN", "SUPER_ADMIN", "SYS_ADMIN"}


# =============================================================================
# 초기화
# =============================================================================

def run_worker_setup(conn: sqlite3.Connection) -> None:
    """
    작업자관리 사용 전 테이블/컬럼 보정 수행
    """
    ensure_worker_table_schema(conn)


# =============================================================================
# 권한
# =============================================================================

def is_admin_role(role_code: str) -> bool:
    """
    관리자 권한 여부 확인
    """
    return str(role_code or "").strip().upper() in ADMIN_ROLES


# =============================================================================
# 코드 옵션 조회
# =============================================================================

def _prepend_empty_option(options: List[Dict[str, str]], empty_name: str = "전체") -> List[Dict[str, str]]:
    """
    옵션목록 상단에 빈 선택값 추가
    """
    return [{"code": "", "name": empty_name}] + options


def get_worker_nationality_options(conn: sqlite3.Connection, include_empty: bool = True) -> List[Dict[str, str]]:
    """
    nationality_code 옵션 조회
    """
    options = get_dtl_code_options_by_mst(conn, "NATIONALITY")
    if include_empty:
        return _prepend_empty_option(options)
    return options


def get_worker_company_options(conn: sqlite3.Connection, include_empty: bool = True) -> List[Dict[str, str]]:
    """
    company_code 옵션 조회
    """
    options = get_dtl_code_options_by_mst(conn, "COMPANY")
    if include_empty:
        return _prepend_empty_option(options)
    return options


def get_worker_job_type_options(conn: sqlite3.Connection, include_empty: bool = True) -> List[Dict[str, str]]:
    """
    job_type_code 옵션 조회
    """
    options = get_dtl_code_options_by_mst(conn, "JOB_TYPE")
    if include_empty:
        return _prepend_empty_option(options)
    return options


def get_worker_skill_level_options(conn: sqlite3.Connection, include_empty: bool = True) -> List[Dict[str, str]]:
    """
    skill_level_code 옵션 조회
    """
    options = get_dtl_code_options_by_mst(conn, "SKILL_LEVEL")
    if include_empty:
        return _prepend_empty_option(options)
    return options


# =============================================================================
# 조회
# =============================================================================

def search_worker_data(
    conn: sqlite3.Connection,
    keyword: str = "",
    use_yn: str = "",
    company_code: str = "",
    nationality_code: str = "",
) -> List[Dict[str, Any]]:
    """
    작업자 목록 조회 서비스
    """
    run_worker_setup(conn)
    return search_workers(
        conn=conn,
        keyword=keyword,
        use_yn=use_yn,
        company_code=company_code,
        nationality_code=nationality_code,
    )


def get_worker_detail_data(conn: sqlite3.Connection, worker_id: int) -> Optional[Dict[str, Any]]:
    """
    작업자 상세 조회 서비스
    """
    run_worker_setup(conn)
    return get_worker_detail(conn, worker_id)


def build_worker_list_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    화면 우측 리스트 표시용 DataFrame 생성
    """
    if not rows:
        return pd.DataFrame(
            columns=[
                "선택",
                "worker_id",
                "작업자번호",
                "작업자명",
                "국적",
                "연락처",
                "업체",
                "직종",
                "숙련도",
                "안전교육",
                "건강검진",
                "사용여부",
            ]
        )

    list_rows: List[Dict[str, Any]] = []
    for row in rows:
        list_rows.append(
            {
                "선택": False,
                "worker_id": row.get("worker_id", ""),
                "작업자번호": row.get("worker_no", ""),
                "작업자명": row.get("worker_name", ""),
                "국적": row.get("nationality_name", row.get("nationality_code", "")),
                "연락처": row.get("phone_no", ""),
                "업체": row.get("company_name", row.get("company_code", "")),
                "직종": row.get("job_type_name", row.get("job_type_code", "")),
                "숙련도": row.get("skill_level_name", row.get("skill_level_code", "")),
                "안전교육": row.get("safety_edu_yn", ""),
                "건강검진": row.get("health_check_yn", ""),
                "사용여부": row.get("use_yn", ""),
            }
        )

    return pd.DataFrame(list_rows)


# =============================================================================
# 저장 / 삭제
# =============================================================================

def save_worker_data(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    actor: str = "system",
) -> int:
    """
    작업자 등록/수정 저장
    """
    run_worker_setup(conn)
    return save_worker(conn, data, actor=actor)


def remove_worker_data(conn: sqlite3.Connection, worker_ids: Sequence[int]) -> int:
    """
    작업자 삭제
    """
    run_worker_setup(conn)
    return delete_workers(conn, worker_ids)


# =============================================================================
# 엑셀 다운로드
# =============================================================================

def _to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    DataFrame 을 Excel 바이트로 변환
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="workers")
    return output.getvalue()


def get_worker_sample_excel_bytes() -> bytes:
    """
    샘플 엑셀 다운로드 바이트 반환
    """
    df = pd.DataFrame(get_worker_sample_rows())
    return _to_excel_bytes(df)


def get_worker_export_excel_bytes(
    conn: sqlite3.Connection,
    worker_ids: Optional[Sequence[int]] = None,
) -> bytes:
    """
    선택자료/전체자료 엑셀 다운로드 바이트 반환
    """
    rows = get_worker_export_rows(conn, worker_ids=worker_ids)
    df = pd.DataFrame(rows)
    return _to_excel_bytes(df)


def get_worker_export_filename(prefix: str = "작업자관리") -> str:
    """
    다운로드 파일명 생성
    """
    now_text = datetime.now().strftime("%Y%m%d %H%M%S")
    return f"{prefix}_{now_text}.xlsx"


# =============================================================================
# 엑셀 업로드
# =============================================================================

def read_worker_upload_file(uploaded_file: Any) -> pd.DataFrame:
    """
    업로드 파일 읽기
    - xlsx / csv 지원
    """
    if uploaded_file is None:
        raise ValueError("업로드 파일이 없습니다.")

    file_name = str(uploaded_file.name).lower()

    if file_name.endswith(".xlsx"):
        df = pd.read_excel(uploaded_file, dtype=str)
    elif file_name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str, encoding="utf-8-sig")
    else:
        raise ValueError("지원하지 않는 파일 형식입니다. xlsx 또는 csv 파일을 업로드하세요.")

    df = df.fillna("")
    return df


def validate_worker_upload_dataframe(df: pd.DataFrame) -> List[str]:
    """
    업로드 파일 컬럼 검증
    """
    required_columns = {
        "worker_no",
        "worker_name",
        "resident_no",
        "nationality_code",
        "phone_no",
        "emergency_phone_no",
        "address",
        "detail_address",
        "bank_name",
        "account_no",
        "account_holder",
        "company_code",
        "job_type_code",
        "skill_level_code",
        "safety_edu_yn",
        "health_check_yn",
        "hire_date",
        "retire_date",
        "use_yn",
        "sort_order",
        "remark",
    }

    available_columns = set(df.columns.tolist())
    missing_columns = [column for column in required_columns if column not in available_columns]

    errors: List[str] = []
    if missing_columns:
        errors.append(f"업로드 파일에 누락된 컬럼이 있습니다: {', '.join(sorted(missing_columns))}")

    return errors


def upload_worker_excel(
    conn: sqlite3.Connection,
    uploaded_file: Any,
    actor: str = "system",
) -> Dict[str, int]:
    """
    작업자 엑셀 업로드 처리
    """
    run_worker_setup(conn)

    df = read_worker_upload_file(uploaded_file)
    errors = validate_worker_upload_dataframe(df)
    if errors:
        raise ValueError("\n".join(errors))

    return bulk_upsert_workers(conn, df, actor=actor)