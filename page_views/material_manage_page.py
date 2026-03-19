# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 자재관리 화면
파일명     : material_manage_page.py
설명       : 자재관리 화면의 공통 템플릿 페이지.
사용 테이블 : 추후 확정
주요 기능   :
    1. 입력/리스트 4:6 레이아웃 표시
    2. 향후 CRUD 확장용 기본 화면 제공
작성일시   : 2026-03-15
변경이력   :
    - 2026-03-15 : 초기 생성
주의사항   :
    - 추후 실제 CRUD 파일로 확장 예정
"""

from page_views.common_page_template import render_placeholder_manage_page


def render_material_manage_page() -> None:
    render_placeholder_manage_page("자재관리")
