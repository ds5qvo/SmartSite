# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 작업자 등록관리 Repository
파일경로 : repositories/worker_manage_repository.py
기능설명 : 작업자 등록관리 화면에서 사용하는 작업자 목록 조회, 상세 조회, 코드 조회, 저장, 삭제 SQL 처리를 담당한다.
작성일시 : 2026-03-17 16:10:00
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional


def _dict_fetchall(cursor) -> List[Dict[str, Any]]:
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def _dict_fetchone(cursor) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    if row is None:
        return None

    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def select_worker_list(conn, search_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    search_params = search_params or {}

    sql = """
        SELECT
            w.worker_id,
            w.worker_code,
            w.worker_name,
            w.birth_date,
            w.gender_code_id,
            w.phone,
            w.hire_date,
            w.department_code_id,
            w.position_code_id,
            w.employment_type_code_id,
            w.use_yn_code_id,
            wd.worker_detail_id,
            wd.resident_no,
            wd.nationality_code_id,
            wd.blood_type_code_id,
            wd.safety_shoes_size,
            wd.vehicle_no,
            wd.address,
            wd.zip_code,
            wd.emergency_contact,
            wa.worker_account_id,
            wa.bank_code_id,
            wa.account_no,
            wa.account_holder_name,
            wa.account_owner_code_id,
            wm.medical_file_id,
            wm.exam_date,
            wm.exam_institution_code_id,
            wm.exam_request_company_code_id,
            wm.exam_result_code_id,
            wm.file_name,
            wm.file_path,
            wm.file_type,
            w.created_at,
            w.created_by,
            w.updated_at,
            w.updated_by
        FROM workers w
        LEFT JOIN worker_details wd
            ON w.worker_id = wd.worker_id
        LEFT JOIN worker_accounts wa
            ON w.worker_id = wa.worker_id
        LEFT JOIN worker_medical_files wm
            ON w.worker_id = wm.worker_id
        WHERE 1 = 1
    """
    params: List[Any] = []

    worker_code = (search_params.get("worker_code") or "").strip()
    worker_name = (search_params.get("worker_name") or "").strip()
    department_code_id = search_params.get("department_code_id")

    if worker_code:
        sql += " AND w.worker_code LIKE ? "
        params.append(f"%{worker_code}%")

    if worker_name:
        sql += " AND w.worker_name LIKE ? "
        params.append(f"%{worker_name}%")

    if department_code_id not in (None, "", 0):
        sql += " AND w.department_code_id = ? "
        params.append(department_code_id)

    sql += " ORDER BY w.worker_code ASC, w.worker_id ASC "

    cur = conn.cursor()
    cur.execute(sql, params)
    return _dict_fetchall(cur)


def select_worker_by_id(conn, worker_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
            worker_id,
            worker_code,
            worker_name,
            birth_date,
            gender_code_id,
            phone,
            hire_date,
            department_code_id,
            position_code_id,
            employment_type_code_id,
            use_yn_code_id,
            created_at,
            created_by,
            updated_at,
            updated_by
        FROM workers
        WHERE worker_id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (worker_id,))
    return _dict_fetchone(cur)


def select_worker_detail_by_worker_id(conn, worker_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
            worker_detail_id,
            worker_id,
            resident_no,
            nationality_code_id,
            blood_type_code_id,
            safety_shoes_size,
            vehicle_no,
            address,
            zip_code,
            emergency_contact,
            created_at,
            created_by,
            updated_at,
            updated_by
        FROM worker_details
        WHERE worker_id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (worker_id,))
    return _dict_fetchone(cur)


def select_worker_account_by_worker_id(conn, worker_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
            worker_account_id,
            worker_id,
            bank_code_id,
            account_no,
            account_holder_name,
            account_owner_code_id,
            created_at,
            created_by,
            updated_at,
            updated_by
        FROM worker_accounts
        WHERE worker_id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (worker_id,))
    return _dict_fetchone(cur)


def select_worker_medical_by_worker_id(conn, worker_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
            medical_file_id,
            worker_id,
            exam_date,
            exam_institution_code_id,
            exam_request_company_code_id,
            exam_result_code_id,
            file_name,
            file_path,
            file_type,
            created_at,
            created_by,
            updated_at,
            updated_by
        FROM worker_medical_files
        WHERE worker_id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (worker_id,))
    return _dict_fetchone(cur)


def select_worker_bundle_by_worker_id(conn, worker_id: int) -> Dict[str, Any]:
    worker = select_worker_by_id(conn, worker_id) or {}
    detail = select_worker_detail_by_worker_id(conn, worker_id) or {}
    account = select_worker_account_by_worker_id(conn, worker_id) or {}
    medical = select_worker_medical_by_worker_id(conn, worker_id) or {}

    bundle: Dict[str, Any] = {}
    bundle.update(worker)
    bundle.update(detail)
    bundle.update(account)
    bundle.update(medical)
    return bundle


def select_dtl_code_options_by_mst_code(conn, mst_code: str) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            d.dtl_code_id,
            d.mst_code_id,
            d.dtl_code,
            d.code_name,
            d.sort_order
        FROM dtl_code d
        INNER JOIN mst_code m
            ON d.mst_code_id = m.mst_code_id
        WHERE m.mst_code = ?
        ORDER BY
            COALESCE(d.sort_order, 999999),
            d.dtl_code_id
    """
    cur = conn.cursor()
    cur.execute(sql, (mst_code,))
    return _dict_fetchall(cur)


def select_next_worker_id(conn, start_id: int = 500001) -> int:
    sql = """
        SELECT COALESCE(MAX(worker_id), 0) AS max_worker_id
        FROM workers
    """
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()

    max_worker_id = row[0] if row and row[0] is not None else 0

    if max_worker_id < start_id:
        return start_id

    return int(max_worker_id) + 1


def select_next_worker_code(conn, prefix: str = "JS-") -> str:
    sql = """
        SELECT worker_code
        FROM workers
        WHERE worker_code LIKE ?
        ORDER BY worker_code DESC
        LIMIT 1
    """
    like_text = f"{prefix}%"

    cur = conn.cursor()
    cur.execute(sql, (like_text,))
    row = cur.fetchone()

    if not row or not row[0]:
        return f"{prefix}001"

    last_code = str(row[0]).strip()

    try:
        last_seq = int(last_code.replace(prefix, ""))
    except ValueError:
        last_seq = 0

    next_seq = last_seq + 1
    return f"{prefix}{next_seq:03d}"


def insert_worker(conn, worker_data: Dict[str, Any]) -> int:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    next_worker_id = select_next_worker_id(conn, 500001)

    sql = """
        INSERT INTO workers (
            worker_id,
            worker_code,
            worker_name,
            birth_date,
            gender_code_id,
            phone,
            hire_date,
            department_code_id,
            position_code_id,
            employment_type_code_id,
            use_yn_code_id,
            created_at,
            created_by,
            updated_at,
            updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        next_worker_id,
        _none_if_blank(worker_data.get("worker_code")),
        _none_if_blank(worker_data.get("worker_name")),
        _none_if_blank(worker_data.get("birth_date")),
        _none_if_blank(worker_data.get("gender_code_id")),
        _none_if_blank(worker_data.get("phone")),
        _none_if_blank(worker_data.get("hire_date")),
        _none_if_blank(worker_data.get("department_code_id")),
        _none_if_blank(worker_data.get("position_code_id")),
        _none_if_blank(worker_data.get("employment_type_code_id")),
        _none_if_blank(worker_data.get("use_yn_code_id")),
        now_text,
        _none_if_blank(worker_data.get("created_by")) or "SYSTEM",
        now_text,
        _none_if_blank(worker_data.get("updated_by")) or "SYSTEM",
    )

    cur = conn.cursor()
    cur.execute(sql, params)
    return next_worker_id


def update_worker(conn, worker_data: Dict[str, Any]) -> None:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        UPDATE workers
           SET worker_code = ?,
               worker_name = ?,
               birth_date = ?,
               gender_code_id = ?,
               phone = ?,
               hire_date = ?,
               department_code_id = ?,
               position_code_id = ?,
               employment_type_code_id = ?,
               use_yn_code_id = ?,
               updated_at = ?,
               updated_by = ?
         WHERE worker_id = ?
    """
    params = (
        _none_if_blank(worker_data.get("worker_code")),
        _none_if_blank(worker_data.get("worker_name")),
        _none_if_blank(worker_data.get("birth_date")),
        _none_if_blank(worker_data.get("gender_code_id")),
        _none_if_blank(worker_data.get("phone")),
        _none_if_blank(worker_data.get("hire_date")),
        _none_if_blank(worker_data.get("department_code_id")),
        _none_if_blank(worker_data.get("position_code_id")),
        _none_if_blank(worker_data.get("employment_type_code_id")),
        _none_if_blank(worker_data.get("use_yn_code_id")),
        now_text,
        _none_if_blank(worker_data.get("updated_by")) or "SYSTEM",
        worker_data.get("worker_id"),
    )

    cur = conn.cursor()
    cur.execute(sql, params)


def upsert_worker_detail(conn, detail_data: Dict[str, Any]) -> int:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worker_id = detail_data.get("worker_id")

    existing = select_worker_detail_by_worker_id(conn, worker_id)

    if existing:
        sql = """
            UPDATE worker_details
               SET resident_no = ?,
                   nationality_code_id = ?,
                   blood_type_code_id = ?,
                   safety_shoes_size = ?,
                   vehicle_no = ?,
                   address = ?,
                   zip_code = ?,
                   emergency_contact = ?,
                   updated_at = ?,
                   updated_by = ?
             WHERE worker_id = ?
        """
        params = (
            _none_if_blank(detail_data.get("resident_no")),
            _none_if_blank(detail_data.get("nationality_code_id")),
            _none_if_blank(detail_data.get("blood_type_code_id")),
            _none_if_blank(detail_data.get("safety_shoes_size")),
            _none_if_blank(detail_data.get("vehicle_no")),
            _none_if_blank(detail_data.get("address")),
            _none_if_blank(detail_data.get("zip_code")),
            _none_if_blank(detail_data.get("emergency_contact")),
            now_text,
            _none_if_blank(detail_data.get("updated_by")) or "SYSTEM",
            worker_id,
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        return existing["worker_detail_id"]

    sql = """
        INSERT INTO worker_details (
            worker_id,
            resident_no,
            nationality_code_id,
            blood_type_code_id,
            safety_shoes_size,
            vehicle_no,
            address,
            zip_code,
            emergency_contact,
            created_at,
            created_by,
            updated_at,
            updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        worker_id,
        _none_if_blank(detail_data.get("resident_no")),
        _none_if_blank(detail_data.get("nationality_code_id")),
        _none_if_blank(detail_data.get("blood_type_code_id")),
        _none_if_blank(detail_data.get("safety_shoes_size")),
        _none_if_blank(detail_data.get("vehicle_no")),
        _none_if_blank(detail_data.get("address")),
        _none_if_blank(detail_data.get("zip_code")),
        _none_if_blank(detail_data.get("emergency_contact")),
        now_text,
        _none_if_blank(detail_data.get("created_by")) or "SYSTEM",
        now_text,
        _none_if_blank(detail_data.get("updated_by")) or "SYSTEM",
    )
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.lastrowid


def upsert_worker_account(conn, account_data: Dict[str, Any]) -> int:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worker_id = account_data.get("worker_id")

    existing = select_worker_account_by_worker_id(conn, worker_id)

    if existing:
        sql = """
            UPDATE worker_accounts
               SET bank_code_id = ?,
                   account_no = ?,
                   account_holder_name = ?,
                   account_owner_code_id = ?,
                   updated_at = ?,
                   updated_by = ?
             WHERE worker_id = ?
        """
        params = (
            _none_if_blank(account_data.get("bank_code_id")),
            _none_if_blank(account_data.get("account_no")),
            _none_if_blank(account_data.get("account_holder_name")),
            _none_if_blank(account_data.get("account_owner_code_id")),
            now_text,
            _none_if_blank(account_data.get("updated_by")) or "SYSTEM",
            worker_id,
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        return existing["worker_account_id"]

    sql = """
        INSERT INTO worker_accounts (
            worker_id,
            bank_code_id,
            account_no,
            account_holder_name,
            account_owner_code_id,
            created_at,
            created_by,
            updated_at,
            updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        worker_id,
        _none_if_blank(account_data.get("bank_code_id")),
        _none_if_blank(account_data.get("account_no")),
        _none_if_blank(account_data.get("account_holder_name")),
        _none_if_blank(account_data.get("account_owner_code_id")),
        now_text,
        _none_if_blank(account_data.get("created_by")) or "SYSTEM",
        now_text,
        _none_if_blank(account_data.get("updated_by")) or "SYSTEM",
    )
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.lastrowid


def upsert_worker_medical(conn, medical_data: Dict[str, Any]) -> int:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worker_id = medical_data.get("worker_id")

    existing = select_worker_medical_by_worker_id(conn, worker_id)

    if existing:
        sql = """
            UPDATE worker_medical_files
               SET exam_date = ?,
                   exam_institution_code_id = ?,
                   exam_request_company_code_id = ?,
                   exam_result_code_id = ?,
                   file_name = ?,
                   file_path = ?,
                   file_type = ?,
                   updated_at = ?,
                   updated_by = ?
             WHERE worker_id = ?
        """
        params = (
            _none_if_blank(medical_data.get("exam_date")),
            _none_if_blank(medical_data.get("exam_institution_code_id")),
            _none_if_blank(medical_data.get("exam_request_company_code_id")),
            _none_if_blank(medical_data.get("exam_result_code_id")),
            _none_if_blank(medical_data.get("file_name")),
            _none_if_blank(medical_data.get("file_path")),
            _none_if_blank(medical_data.get("file_type")),
            now_text,
            _none_if_blank(medical_data.get("updated_by")) or "SYSTEM",
            worker_id,
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        return existing["medical_file_id"]

    sql = """
        INSERT INTO worker_medical_files (
            worker_id,
            exam_date,
            exam_institution_code_id,
            exam_request_company_code_id,
            exam_result_code_id,
            file_name,
            file_path,
            file_type,
            created_at,
            created_by,
            updated_at,
            updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        worker_id,
        _none_if_blank(medical_data.get("exam_date")),
        _none_if_blank(medical_data.get("exam_institution_code_id")),
        _none_if_blank(medical_data.get("exam_request_company_code_id")),
        _none_if_blank(medical_data.get("exam_result_code_id")),
        _none_if_blank(medical_data.get("file_name")),
        _none_if_blank(medical_data.get("file_path")),
        _none_if_blank(medical_data.get("file_type")),
        now_text,
        _none_if_blank(medical_data.get("created_by")) or "SYSTEM",
        now_text,
        _none_if_blank(medical_data.get("updated_by")) or "SYSTEM",
    )
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.lastrowid


def delete_worker_bundle_by_worker_id(conn, worker_id: int) -> None:
    cur = conn.cursor()

    cur.execute("DELETE FROM worker_medical_files WHERE worker_id = ?", (worker_id,))
    cur.execute("DELETE FROM worker_accounts WHERE worker_id = ?", (worker_id,))
    cur.execute("DELETE FROM worker_details WHERE worker_id = ?", (worker_id,))
    cur.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))


def _none_if_blank(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped != "" else None

    return value