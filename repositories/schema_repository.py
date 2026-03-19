# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 스키마 저장소
파일경로 : repositories/schema_repository.py
기능설명 :
    - SQLite 테이블 생성 및 컬럼 구조 보정 공통 기능 제공
    - 기존 테이블은 최대한 유지하고, 없으면 생성 / 없어진 컬럼만 ADD COLUMN 수행
    - DROP TABLE 대신 ALTER TABLE / ADD COLUMN 중심으로 운영
    - sort_no 대신 sort_order 기준으로 전체 사용
    - dtl_code.remark 컬럼은 추가하지 않음
작성일시 : 2026-03-17
"""

from typing import Any, Dict, Iterable, List, Tuple


def get_table_column_names(conn, table_name: str) -> List[str]:
    """
    경로 : repositories/schema_repository.py

    설명 :
        지정한 테이블의 현재 컬럼 목록을 조회한다.
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()
    return [row[1] for row in rows]


def _extract_column_sql_map_from_create_sql(create_sql: str) -> Dict[str, str]:
    """
    경로 : repositories/schema_repository.py

    설명 :
        CREATE TABLE SQL 에서 컬럼명과 컬럼 SQL 정의를 추출한다.
    """
    column_sql_map: Dict[str, str] = {}

    lines = create_sql.splitlines()
    for raw_line in lines:
        line = raw_line.strip().rstrip(",")

        if not line:
            continue

        upper_line = line.upper()
        if upper_line.startswith("CREATE TABLE"):
            continue
        if line in ("(", ")"):
            continue
        if upper_line.startswith("PRIMARY KEY"):
            continue
        if upper_line.startswith("FOREIGN KEY"):
            continue
        if upper_line.startswith("UNIQUE"):
            continue
        if upper_line.startswith("CONSTRAINT"):
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        column_name = parts[0].strip('"').strip("'").strip("`")
        column_type_sql = " ".join(parts[1:]).strip()

        if column_name and column_type_sql:
            column_sql_map[column_name] = column_type_sql

    return column_sql_map


def _normalize_required_column_item(
    column_item: Any,
    create_sql_column_map: Dict[str, str],
) -> Tuple[str, str]:
    """
    경로 : repositories/schema_repository.py

    설명 :
        required_columns 항목을 (column_name, column_sql) 형태로 변환한다.

    지원 형식 :
        - "column_name"
        - ("column_name", "TEXT")
        - ("column_name", "TEXT", "DEFAULT ''")
        - {"name": "column_name", "type": "TEXT"}
    """
    if isinstance(column_item, str):
        column_name = column_item.strip()
        column_sql = create_sql_column_map.get(column_name)
        if not column_sql:
            raise ValueError(
                f"required_columns 문자열 항목의 타입을 create_sql 에서 찾을 수 없습니다: {column_item}"
            )
        return column_name, column_sql

    if isinstance(column_item, dict):
        column_name = (
            column_item.get("name")
            or column_item.get("column_name")
            or column_item.get("column")
        )
        column_sql = (
            column_item.get("type")
            or column_item.get("column_type")
            or column_item.get("column_type_sql")
            or column_item.get("type_sql")
        )

        if not column_name or not column_sql:
            raise ValueError(f"required_columns dict 형식 오류: {column_item}")

        return str(column_name).strip(), str(column_sql).strip()

    if isinstance(column_item, (tuple, list)):
        if len(column_item) < 2:
            raise ValueError(
                f"required_columns tuple/list 는 최소 2개 값이 필요합니다: {column_item}"
            )

        column_name = str(column_item[0]).strip()
        sql_parts = [str(part).strip() for part in column_item[1:] if str(part).strip()]
        column_sql = " ".join(sql_parts).strip()

        if not column_name or not column_sql:
            raise ValueError(f"required_columns tuple/list 형식 오류: {column_item}")

        return column_name, column_sql

    raise ValueError(f"지원하지 않는 required_columns 항목 형식입니다: {column_item}")


def ensure_table_and_columns(
    conn,
    table_name: str,
    create_sql: str,
    required_columns: Iterable[Any],
) -> List[str]:
    """
    경로 : repositories/schema_repository.py

    설명 :
        - 테이블이 없으면 create_sql 로 생성
        - 테이블이 있으면 required_columns 기준으로 누락 컬럼만 ADD COLUMN 수행
        - 기존 컬럼 삭제/변경은 수행하지 않음
    """
    messages: List[str] = []
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    exists = cursor.fetchone() is not None

    if not exists:
        cursor.execute(create_sql)
        conn.commit()
        messages.append(f"[생성] 테이블 생성 완료: {table_name}")

    existing_columns = get_table_column_names(conn, table_name)
    create_sql_column_map = _extract_column_sql_map_from_create_sql(create_sql)

    for raw_column in required_columns:
        column_name, column_type_sql = _normalize_required_column_item(
            raw_column,
            create_sql_column_map,
        )

        if column_name not in existing_columns:
            alter_sql = (
                f"ALTER TABLE {table_name} "
                f"ADD COLUMN {column_name} {column_type_sql}"
            )
            cursor.execute(alter_sql)
            messages.append(f"[보정] {table_name}.{column_name} 컬럼 추가")

    conn.commit()
    return messages


def ensure_sort_order_column(conn, table_name: str) -> List[str]:
    """
    경로 : repositories/schema_repository.py

    설명 :
        지정 테이블에 sort_order 컬럼이 없으면 추가하고,
        sort_no 컬럼이 있으면 기존 값을 sort_order 로 이관한다.
    """
    messages: List[str] = []
    cursor = conn.cursor()
    columns = get_table_column_names(conn, table_name)

    if "sort_order" not in columns:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN sort_order INTEGER DEFAULT 0"
        )
        messages.append(f"[보정] {table_name}.sort_order 컬럼 추가")

    columns = get_table_column_names(conn, table_name)
    if "sort_no" in columns and "sort_order" in columns:
        cursor.execute(
            f"""
            UPDATE {table_name}
               SET sort_order = COALESCE(sort_order, sort_no, 0)
             WHERE sort_order IS NULL OR sort_order = 0
            """
        )
        messages.append(f"[보정] {table_name}.sort_no -> sort_order 데이터 이관")

    conn.commit()
    return messages


def ensure_worker_tables(conn) -> List[str]:
    """
    경로 : repositories/schema_repository.py

    설명 :
        작업자 등록관리 테이블 구조를 보정한다.
    """
    messages: List[str] = []

    workers_create_sql = """
    CREATE TABLE IF NOT EXISTS workers (
        worker_id INTEGER PRIMARY KEY,
        worker_code TEXT,
        worker_name TEXT,
        department_code_id INTEGER,
        position_code_id INTEGER,
        employment_type_code_id INTEGER,
        gender_code_id INTEGER,
        birth_date TEXT,
        phone_no TEXT,
        hire_date TEXT,
        use_yn_code_id INTEGER,
        remark TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """

    messages.extend(
        ensure_table_and_columns(
            conn,
            "workers",
            workers_create_sql,
            [
                "worker_id",
                "worker_code",
                "worker_name",
                "department_code_id",
                "position_code_id",
                "employment_type_code_id",
                "gender_code_id",
                "birth_date",
                "phone_no",
                "hire_date",
                "use_yn_code_id",
                "remark",
                "created_at",
                "updated_at",
            ],
        )
    )

    worker_details_create_sql = """
    CREATE TABLE IF NOT EXISTS worker_details (
        worker_id INTEGER PRIMARY KEY,
        nationality_code_id INTEGER,
        blood_type_code_id INTEGER,
        resident_no TEXT,
        emergency_phone_no TEXT,
        address_1 TEXT,
        address_2 TEXT,
        memo TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """

    messages.extend(
        ensure_table_and_columns(
            conn,
            "worker_details",
            worker_details_create_sql,
            [
                "worker_id",
                "nationality_code_id",
                "blood_type_code_id",
                "resident_no",
                "emergency_phone_no",
                "address_1",
                "address_2",
                "memo",
                "created_at",
                "updated_at",
            ],
        )
    )

    worker_accounts_create_sql = """
    CREATE TABLE IF NOT EXISTS worker_accounts (
        worker_id INTEGER PRIMARY KEY,
        bank_code_id INTEGER,
        account_holder_type_code_id INTEGER,
        account_holder_name TEXT,
        account_no TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """

    messages.extend(
        ensure_table_and_columns(
            conn,
            "worker_accounts",
            worker_accounts_create_sql,
            [
                "worker_id",
                "bank_code_id",
                "account_holder_type_code_id",
                "account_holder_name",
                "account_no",
                "created_at",
                "updated_at",
            ],
        )
    )

    worker_medical_files_create_sql = """
    CREATE TABLE IF NOT EXISTS worker_medical_files (
        worker_id INTEGER PRIMARY KEY,
        medical_institution_code_id INTEGER,
        request_company_code_id INTEGER,
        medical_result_code_id INTEGER,
        medical_date TEXT,
        file_name TEXT,
        file_path TEXT,
        file_type TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """

    messages.extend(
        ensure_table_and_columns(
            conn,
            "worker_medical_files",
            worker_medical_files_create_sql,
            [
                "worker_id",
                "medical_institution_code_id",
                "request_company_code_id",
                "medical_result_code_id",
                "medical_date",
                "file_name",
                "file_path",
                "file_type",
                "created_at",
                "updated_at",
            ],
        )
    )

    return messages