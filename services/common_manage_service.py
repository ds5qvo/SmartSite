# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 공통 화면 서비스
파일명     : common_manage_service.py
설명       : 아직 상세 구현 전인 관리 화면의 공통 표시용 서비스.
사용 테이블 : 없음
주요 기능   :
    1. 화면 설명 데이터 제공
작성일시   : 2026-03-15
변경이력   :
    - 2026-03-15 : 초기 생성
주의사항   :
    - 실제 CRUD 구현 전 임시 화면 표시용
"""


def get_placeholder_message(page_title: str) -> str:
    return f"{page_title} 화면은 다음 개발 단계에서 실제 CRUD 구조로 확장 예정입니다."
