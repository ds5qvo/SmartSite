# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 공통 DB 연결 모듈
파일명     : core/db.py
설명       : SmartSite SQLite 데이터베이스 연결 및 공통 DB 유틸리티를 제공한다.
사용 테이블: 전체 공통
주요 기능  :
    1. SQLite 연결 반환
    2. Row Factory 적용(dict 형태 접근 지원)
    3. 기본 DB 경로 관리
작성일시   : 2026-03-15 18:10:00
변경이력   :
    - 2026-03-15 : user_manage_page 모듈 연계를 위한 기본 DB 연결 모듈 작성
주의사항   :
    1. DB DROP 방식은 사용하지 않는다.
    2. 컬럼 누락 시 ALTER TABLE ADD COLUMN 방식으로 보정한다.
    3. DB 경로는 환경변수 SMARTSITE_DB_PATH 지정 시 우선 사용한다.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DEFAULT_DB_PATH = os.environ.get(
    "SMARTSITE_DB_PATH",
    r"G:\내 드라이브\SmartSite\database\smartsite.db",
)


def get_db_path() -> str:
    """SmartSite DB 파일 경로를 반환한다."""
    return DEFAULT_DB_PATH


def ensure_db_directory() -> None:
    """DB 파일이 위치할 상위 폴더를 생성한다."""
    Path(get_db_path()).parent.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    """SQLite 연결 객체를 반환한다."""
    ensure_db_directory()
    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_cursor(commit: bool = False) -> Iterator[sqlite3.Cursor]:
    """공통 커서 컨텍스트 매니저."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
