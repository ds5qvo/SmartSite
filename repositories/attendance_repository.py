# -*- coding: utf-8 -*-
"""
프로그램명 : SmartSite 공통 거래 Repository
파일명     : attendance_repository.py
설명       : trn_attendance 테이블 CRUD 및 목록 조회 처리.
작성일시   : 2026-03-15
"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, List, Optional
from core.db import get_connection

class AttendanceRepository:
    table_name = "trn_attendance"
    pk_name = "attendance_id"

    def _now(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_next_id(self) -> int:
        conn = get_connection()
        try:
            row = conn.execute(f"SELECT COALESCE(MAX({self.pk_name}), 0) + 1 AS next_id FROM {self.table_name}").fetchone()
            return int(row["next_id"])
        finally:
            conn.close()

    def search_list(self, keyword: str = "") -> List[Dict]:
        conn = get_connection()
        try:
            sql = f"""
                SELECT t.{self.pk_name}, t.project_id, COALESCE(p.project_name,'') AS project_name,
                       t.work_date, t.item_code, t.item_name, t.qty, t.unit_price, t.amount, t.remark,
                       t.created_by, t.created_at, t.updated_by, t.updated_at
                  FROM {self.table_name} t
                  LEFT JOIN mst_project p ON t.project_id = p.project_id
                 WHERE 1=1
            """
            params = []
            if keyword:
                like = f"%{keyword}%"
                sql += " AND (COALESCE(p.project_name,'') LIKE ? OR COALESCE(t.item_code,'') LIKE ? OR COALESCE(t.item_name,'') LIKE ? OR COALESCE(t.remark,'') LIKE ?)"
                params.extend([like, like, like, like])
            sql += f" ORDER BY t.{self.pk_name}"
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def get_by_id(self, pk_value: int) -> Optional[Dict]:
        conn = get_connection()
        try:
            row = conn.execute(f"SELECT * FROM {self.table_name} WHERE {self.pk_name} = ?", (pk_value,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def save(self, data: Dict) -> int:
        conn = get_connection()
        try:
            now = self._now()
            amount = float(data.get("qty", 0) or 0) * float(data.get("unit_price", 0) or 0)
            if data.get(self.pk_name):
                conn.execute(
                    f"""
                    UPDATE {self.table_name}
                       SET project_id=?, work_date=?, item_code=?, item_name=?, qty=?, unit_price=?, amount=?,
                           remark=?, updated_by=?, updated_at=?
                     WHERE {self.pk_name}=?
                    """,
                    (
                        data.get("project_id"), data.get("work_date", ""), data.get("item_code", ""),
                        data.get("item_name", ""), data.get("qty", 0), data.get("unit_price", 0),
                        amount, data.get("remark", ""), data.get("updated_by", "system"),
                        now, data[self.pk_name]
                    ),
                )
                conn.commit()
                return int(data[self.pk_name])
            next_id = self.get_next_id()
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    {self.pk_name}, project_id, work_date, item_code, item_name, qty, unit_price, amount,
                    remark, created_by, created_at, updated_by, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_id, data.get("project_id"), data.get("work_date", ""), data.get("item_code", ""),
                    data.get("item_name", ""), data.get("qty", 0), data.get("unit_price", 0),
                    amount, data.get("remark", ""), data.get("created_by", "system"),
                    now, data.get("updated_by", "system"), now
                ),
            )
            conn.commit()
            return next_id
        finally:
            conn.close()

    def delete_many(self, pk_values: list[int]) -> None:
        conn = get_connection()
        try:
            for pk in pk_values:
                conn.execute(f"DELETE FROM {self.table_name} WHERE {self.pk_name} = ?", (pk,))
            conn.commit()
        finally:
            conn.close()
