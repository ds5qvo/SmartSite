# -*- coding: utf-8 -*-
"""
프로그램명 : db_config.py
파일경로   : config/db_config.py
기능설명   :
사용테이블 :
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : SmartSite 기준 DB 경로 통일용 공통 함수 작성
주의사항   :
    - 기준 DB 경로는 반드시 SmartSite 상위 폴더의 database/smartsite.db 사용
    - smartsite_app 하위 database 폴더 사용 금지
"""

from __future__ import annotations

import os
import sqlite3

from config.menu_config import DEFAULT_DB_FILENAME


def get_app_dir() -> str:
    """
    smartsite_app 폴더 경로 반환
    예:
        G:/내 드라이브/SmartSite/smartsite_app
    """
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_project_root_dir() -> str:
    """
    SmartSite 프로젝트 루트 경로 반환
    예:
        G:/내 드라이브/SmartSite
    """
    return os.path.dirname(get_app_dir())


def get_database_dir() -> str:
    """
    기준 database 폴더 경로 반환
    예:
        G:/내 드라이브/SmartSite/database
    """
    db_dir = os.path.join(get_project_root_dir(), "database")
    os.makedirs(db_dir, exist_ok=True)
    return db_dir


def get_database_path() -> str:
    """
    기준 DB 파일 경로 반환
    예:
        G:/내 드라이브/SmartSite/database/smartsite.db
    """
    return os.path.join(get_database_dir(), DEFAULT_DB_FILENAME)


def get_connection() -> sqlite3.Connection:
    db_path = get_database_path()
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn