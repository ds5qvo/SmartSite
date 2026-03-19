# -*- coding: utf-8 -*-
"""
프로그램명 : page_registry.py
파일경로   : config/page_registry.py
기능설명   :
주요기능
변경이력
주의사항
"""

from __future__ import annotations

from config.menu_config import *
from page_views.attendance_manage_page import render_attendance_manage_page
from page_views.code_manage_page import render_code_manage_page
from page_views.common_page_template import render_placeholder_manage_page
from page_views.company_manage_page import render_company_manage_page
from page_views.daily_report_manage_page import render_daily_report_manage_page
from page_views.equipment_manage_page import render_equipment_manage_page
from page_views.expense_manage_page import render_expense_manage_page
from page_views.insurance_manage_page import render_insurance_manage_page
from page_views.material_manage_page import render_material_manage_page
from page_views.project_manage_page import render_project_manage_page
from page_views.site_manage_page import render_site_manage_page
from page_views.table_upload_manage_page import render_table_upload_manage_page
from page_views.user_manage_page import render_user_manage_page
from page_views.worker_manage_page import render_worker_manage_page


def _placeholder(title: str, desc: str):
    return lambda conn: render_placeholder_manage_page(title, desc)


PAGE_REGISTRY = {
    MENU_CODE: render_code_manage_page,
    MENU_USER: render_user_manage_page,
    MENU_COMPANY: render_company_manage_page,
    MENU_WORKER: render_worker_manage_page,
    MENU_INSURANCE: render_insurance_manage_page,
    MENU_PROJECT: render_project_manage_page,
    MENU_SITE: render_site_manage_page,
    MENU_DAILY_REPORT: render_daily_report_manage_page,
    MENU_ATTENDANCE: render_attendance_manage_page,
    MENU_WORK_ASSIGN: _placeholder(MENU_WORK_ASSIGN, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_MATERIAL: render_material_manage_page,
    MENU_EQUIPMENT: render_equipment_manage_page,
    MENU_EXPENSE: render_expense_manage_page,
    MENU_ESTIMATE: _placeholder(MENU_ESTIMATE, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_CONTRACT: _placeholder(MENU_CONTRACT, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_OUTPUT: _placeholder(MENU_OUTPUT, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_FILE: _placeholder(MENU_FILE, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_AUTH: _placeholder(MENU_AUTH, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_MENU: _placeholder(MENU_MENU, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_LOG: _placeholder(MENU_LOG, "기존 메뉴를 유지하기 위한 임시 연결 화면입니다."),
    MENU_TABLE_UPLOAD: render_table_upload_manage_page,
}
