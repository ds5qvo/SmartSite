# -*- coding: utf-8 -*-
"""
프로그램명 : common_page_template.py
파일경로   : page_views/common_page_template.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


def run_common_input_search_page(page_title: str, file_name: str, description: str = "") -> None:
    st.title(page_title)
    st.caption(f"실행 화면 파일 : {file_name}")
    if description:
        st.info(description)

    left_col, right_col = st.columns([4, 6])
    with left_col:
        st.markdown("### 입력 화면")
        st.text_input("코드", key=f"{page_title}_code")
        st.text_input("명칭", key=f"{page_title}_name")
        st.selectbox("사용여부", ["Y", "N"], key=f"{page_title}_use")
        st.text_area("비고", key=f"{page_title}_note", height=120)
    with right_col:
        st.markdown("### 조회 화면")
        st.text_input("검색어", key=f"{page_title}_search")
        st.dataframe(pd.DataFrame(columns=["선택", "코드", "명칭"]), hide_index=True, width="stretch")


def render_placeholder_manage_page(page_title: str, description: str = "") -> None:
    run_common_input_search_page(page_title, f"page_views/{page_title}_manage_page.py", description)
