# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 작업자 등록관리 화면
파일경로 : page_views/worker_manage_page.py
기능설명 : 작업자 등록관리 메인 화면 렌더링을 담당한다.
작성일시 : 2026-03-17 16:10:00
"""

from __future__ import annotations

import streamlit as st

from page_views.worker_manage_page_helpers import (
    load_code_options_map,
    render_worker_editor_area,
    render_worker_list_area,
)
from page_views.worker_manage_page_state import (
    apply_pending_resets,
    initialize_worker_manage_state,
)


def render_worker_manage_page(conn) -> None:
    """
    작업자 등록관리 메인 화면
    """
    initialize_worker_manage_state()
    apply_pending_resets()

    code_options_map = load_code_options_map(conn)

    left_col, right_col = st.columns([4, 6], gap="medium")

    with right_col:
        render_worker_list_area(conn, code_options_map)

    with left_col:
        render_worker_editor_area(conn, code_options_map)