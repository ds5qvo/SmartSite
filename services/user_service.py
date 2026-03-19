# -*- coding: utf-8 -*-
"""
프로그램명 : user_service.py
파일경로   : services/user_service.py
기능설명   : SmartSite 사용자관리 Service
화면설명   :
    - 사용자관리 화면에서 사용하는 서비스 계층
    - 사용자 테이블 준비
    - ROLE / DEPT 옵션 조회
    - 사용자 목록조회 / 단건조회 / 저장 / 삭제 제공
사용테이블 :
    - mst_user
    - mst_code
    - dtl_code
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : 최초 작성
    - 2026-03-16 : 전체 함수/호출 관계 재점검 후 재정리
주의사항   :
    - page_views/user_manage_page.py 에서 호출하는 함수명과 동일하게 유지
    - 실제 DB 처리는 repositories/user_repository.py 에서 수행
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd

from repositories.user_repository import (
    delete_users,
    ensure_user_table,
    get_dept_options,
    get_role_options,
    get_user,
    list_users,
    save_user,
)


# =============================================================================
# 초기설정
# =============================================================================
def run_user_setup(conn: sqlite3.Connection, actor: str = "system") -> None:
    """
    사용자관리 초기설정
    - mst_user 표준 테이블 보장
    - 현재 actor 인자는 향후 로그/이력 확장용으로 유지
    """
    ensure_user_table(conn)


# =============================================================================
# 코드 옵션 조회
# =============================================================================
def get_user_role_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    ROLE 코드 옵션 조회
    반환 예시:
        [
            {"code_value": "SYS_ADMIN", "code_name": "시스템관리자", "sort_order": 1},
            ...
        ]
    """
    return get_role_options(conn)


def get_user_dept_options(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    DEPT 코드 옵션 조회
    반환 예시:
        [
            {"code_value": "HQ", "code_name": "본사", "sort_order": 1},
            ...
        ]
    """
    return get_dept_options(conn)


# =============================================================================
# 조회
# =============================================================================
def search_users(
    conn: sqlite3.Connection,
    search_text: str = "",
    use_yn: str = "",
    role_code: str = "",
    dept_code: str = "",
) -> pd.DataFrame:
    """
    사용자 목록 조회
    """
    return list_users(
        conn=conn,
        search_text=search_text,
        use_yn=use_yn,
        role_code=role_code,
        dept_code=dept_code,
    )


def get_user_detail(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    """
    사용자 상세 조회
    """
    return get_user(conn, user_id)


# =============================================================================
# 저장 / 삭제
# =============================================================================
def save_user_data(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    actor: str = "system",
) -> int:
    """
    사용자 등록/수정 저장
    - 신규 등록 시 비밀번호 필수
    - 수정 시 비밀번호 미입력하면 기존 해시 유지
    """
    return save_user(conn, data, actor=actor)


def remove_users(
    conn: sqlite3.Connection,
    user_ids: List[int],
    actor: str = "system",
) -> int:
    """
    사용자 삭제
    - 현재 actor 인자는 향후 삭제이력/log 확장용으로 유지
    """
    return delete_users(conn, user_ids)