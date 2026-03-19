# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 공통 거래 서비스
파일명     : expense_service.py
설명       : 경비관리 업무 로직 처리.
작성일시   : 2026-03-15
"""
from repositories.expense_repository import ExpenseRepository

class ExpenseService:
    def __init__(self) -> None:
        self.repository = ExpenseRepository()

    def get_list(self, keyword: str = ""):
        return self.repository.search_list(keyword)

    def get_detail(self, pk_value: int):
        return self.repository.get_by_id(pk_value)

    def get_next_id(self) -> int:
        return self.repository.get_next_id()

    def save(self, data: dict) -> str:
        if not data.get("project_id"):
            raise ValueError("프로젝트를 선택해 주세요.")
        if not str(data.get("item_name", "")).strip():
            raise ValueError("항목명을 입력해 주세요.")
        self.repository.save(data)
        return "경비관리 자료가 저장되었습니다."

    def delete_many(self, pk_values: list[int]) -> str:
        if not pk_values:
            raise ValueError("삭제할 자료를 선택해 주세요.")
        self.repository.delete_many(pk_values)
        return "선택한 자료가 삭제되었습니다."
