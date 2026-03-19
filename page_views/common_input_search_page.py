# -*- coding: utf-8 -*-
"""
프로그램명 : common_input_search_page.py
파일경로   : page_views/common_input_search_page.py
기능설명   : SmartSite 공통 입력/조회 Placeholder 화면
화면설명   :
    - 아직 업무 로직이 없는 메뉴도 공통 입력/조회 레이아웃 표시
    - 입력:리스트 = 4:6
    - 검색영역 / 입력영역 / 리스트영역 기본 제공
사용테이블 :
    - 없음
작성일시   : 2026-03-16
작성자     : ChatGPT
변경이력   :
    - 2026-03-16 : 최초 작성
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


def run_common_input_search_page(
    page_title: str,
    file_name: str,
    description: str = "",
) -> None:
    st.title(page_title)
    st.caption(f"화면 파일명 : {file_name}")

    if description:
        st.info(description)

    left_col, right_col = st.columns([4, 6])

    with left_col:
        st.markdown("### 입력 화면")
        st.text_input("코드")
        st.text_input("명칭")
        st.selectbox("사용여부", ["Y", "N"], index=0)
        st.number_input("정렬순서", min_value=0, step=1, value=0)
        st.text_area("비고", height=120)

        c1, c2, c3 = st.columns(3)
        c1.button("저장", width="stretch", key=f"{page_title}_save")
        c2.button("초기화", width="stretch", key=f"{page_title}_reset")
        c3.button("선택삭제", width="stretch", key=f"{page_title}_delete")

    with right_col:
        st.markdown("### 조회 화면")
        c1, c2, c3 = st.columns([5, 2, 2])
        c1.text_input("검색어", placeholder="검색어 입력", key=f"{page_title}_search_text")
        c2.selectbox("사용여부", ["전체", "Y", "N"], key=f"{page_title}_search_useyn")
        c3.button("조회", width="stretch", key=f"{page_title}_search_button")

        b1, b2 = st.columns(2)
        b1.button("전체선택", width="stretch", key=f"{page_title}_select_all")
        b2.button("선택해제", width="stretch", key=f"{page_title}_unselect_all")

        empty_df = pd.DataFrame(
            columns=["선택", "코드", "명칭", "사용", "정렬", "비고", "등록일시", "수정일시"]
        )
        st.dataframe(empty_df, width="stretch", height=560, hide_index=True)
        st.caption("조회건수 : 0 건 / 선택건수 : 0 건")