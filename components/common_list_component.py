# -*- coding: utf-8 -*-
"""
프로그램명 : common_list_component.py
파일경로   : components/common_list_component.py
기능설명   : SmartSite 공통 리스트 컴포넌트
사용테이블 : 없음
작성일시   : 2026-03-17
작성자     : ChatGPT
변경이력   :
    - 2026-03-17 : 최초 작성
주의사항   :
    - 검색 / 체크박스 다중선택 / 전체선택 / 선택해제 공통 처리
    - 단일선택 시 상세 표시용 데이터 반환
    - 다중선택 시 화면 메시지 분기 가능
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

import pandas as pd
import streamlit as st


@dataclass
class ListSelectionResult:
    filtered_df: pd.DataFrame
    display_df: pd.DataFrame
    selected_indices: List[int]
    selected_rows: pd.DataFrame
    is_single: bool
    is_multi: bool
    selected_row: Optional[dict]
    keyword: str


class CommonListComponent:
    """SmartSite 표준 리스트 컴포넌트."""

    @staticmethod
    def render(
        *,
        key_prefix: str,
        df: pd.DataFrame,
        searchable_columns: Optional[Sequence[str]] = None,
        default_checked: bool = False,
        height: int = 420,
        use_container_width: bool = True,
        hide_index: bool = True,
        disabled_columns: Optional[Sequence[str]] = None,
        column_order: Optional[Sequence[str]] = None,
    ) -> ListSelectionResult:
        source_df = df.copy() if df is not None else pd.DataFrame()

        if source_df.empty:
            st.info("조회된 데이터가 없습니다.")
            empty_df = pd.DataFrame()
            return ListSelectionResult(
                filtered_df=empty_df,
                display_df=empty_df,
                selected_indices=[],
                selected_rows=empty_df,
                is_single=False,
                is_multi=False,
                selected_row=None,
                keyword="",
            )

        if column_order:
            ordered = [col for col in column_order if col in source_df.columns]
            remain = [col for col in source_df.columns if col not in ordered]
            source_df = source_df[ordered + remain]

        searchable_columns = list(searchable_columns or source_df.columns.tolist())
        disabled_columns = list(disabled_columns or [])

        top_col1, top_col2, top_col3 = st.columns([6, 1, 1])
        with top_col1:
            keyword = st.text_input("검색", key=f"{key_prefix}_keyword")
        with top_col2:
            select_all = st.button("전체선택", key=f"{key_prefix}_select_all", use_container_width=True)
        with top_col3:
            clear_all = st.button("선택해제", key=f"{key_prefix}_clear_all", use_container_width=True)

        filtered_df = CommonListComponent._filter_dataframe(source_df, keyword, searchable_columns)
        display_df = filtered_df.copy()

        checked_key = f"{key_prefix}_checked_map"
        if checked_key not in st.session_state:
            st.session_state[checked_key] = {}

        checked_map = st.session_state[checked_key]

        if select_all:
            for idx in filtered_df.index:
                checked_map[int(idx)] = True

        if clear_all:
            for idx in list(checked_map.keys()):
                checked_map[idx] = False

        checked_values = []
        for idx in filtered_df.index:
            checked_values.append(bool(checked_map.get(int(idx), default_checked)))

        display_df.insert(0, "선택", checked_values)

        edited_df = st.data_editor(
            display_df,
            key=f"{key_prefix}_editor",
            height=height,
            use_container_width=use_container_width,
            hide_index=hide_index,
            disabled=[col for col in display_df.columns if col in disabled_columns and col != "선택"],
            column_config={
                "선택": st.column_config.CheckboxColumn("선택", width="small"),
            },
        )

        for idx, original_idx in enumerate(filtered_df.index):
            checked_map[int(original_idx)] = bool(edited_df.iloc[idx]["선택"])

        selected_indices = [idx for idx in filtered_df.index if bool(checked_map.get(int(idx), False))]
        selected_rows = filtered_df.loc[selected_indices].copy() if selected_indices else pd.DataFrame(columns=filtered_df.columns)

        is_single = len(selected_indices) == 1
        is_multi = len(selected_indices) > 1
        selected_row = selected_rows.iloc[0].to_dict() if is_single else None

        return ListSelectionResult(
            filtered_df=filtered_df,
            display_df=edited_df,
            selected_indices=[int(idx) for idx in selected_indices],
            selected_rows=selected_rows,
            is_single=is_single,
            is_multi=is_multi,
            selected_row=selected_row,
            keyword=keyword,
        )

    @staticmethod
    def show_selection_message(result: ListSelectionResult) -> None:
        if result.is_multi:
            st.warning("입력 화면에 데이타가 다중 선택되었습니다.")
        elif result.is_single:
            st.success("단일 데이터가 선택되었습니다.")

    @staticmethod
    def _filter_dataframe(
        df: pd.DataFrame,
        keyword: str,
        searchable_columns: Sequence[str],
    ) -> pd.DataFrame:
        if df.empty or not keyword:
            return df.copy()

        keyword_lower = str(keyword).strip().lower()
        if not keyword_lower:
            return df.copy()

        mask = pd.Series(False, index=df.index)
        for col in searchable_columns:
            if col in df.columns:
                series = df[col].fillna("").astype(str).str.lower()
                mask = mask | series.str.contains(keyword_lower, na=False)

        return df.loc[mask].copy()