# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 권한 공통 모듈
파일명     : auth.py
설명       : 현재 로그인/권한 구조가 확정되기 전까지 사용하는 임시 권한 판별 모듈.
사용 테이블 : 없음
주요 기능   :
    1. 기본 사용자 정보 반환
    2. 관리자 권한 여부 판별
작성일시   : 2026-03-15
변경이력   :
    - 2026-03-15 : 초기 생성
주의사항   :
    - 추후 user 테이블/로그인 구조 확정 시 교체 예정
"""

ADMIN_ROLES = {"관리자", "최상위자", "최고관리자", "시스템관리자"}


def get_current_user() -> dict:
    return {"user_id": "system", "user_name": "시스템", "role_name": "시스템관리자"}


def is_admin_role(role_name: str) -> bool:
    return role_name in ADMIN_ROLES
