# -*- coding: utf-8 -*-
"""
프로그램명 : 스키마 메타 설정
파일경로   : config/schema_meta.py
기능설명   :
사용테이블 :
작성일시   : 2026-03-16 20:20
작성자     : ChatGPT
변경이력   :
주의사항   :
"""

SQLITE_NUMERIC_TYPES = {"INTEGER", "REAL", "NUMERIC", "FLOAT", "DOUBLE", "DECIMAL"}
SQLITE_DATE_TYPES = {"DATE", "DATETIME", "TIMESTAMP"}

TABLE_CANDIDATE_MAP = {
    "code_master": ["mst_code"],
    "code_detail": ["dtl_code"],
    "code_history": ["hist_code"],
    "deduction_item": ["payroll_deduction_items"],
    "user_main": ["mst_user", "users"],
    "worker_main": ["mst_worker", "workers"],
    "worker_detail": ["worker_details"],
    "worker_account": ["worker_accounts"],
}
