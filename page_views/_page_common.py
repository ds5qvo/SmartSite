
from __future__ import annotations

import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Any, Dict, List


def _actor() -> str:
    for key in ["login_user_name", "user_name", "login_id"]:
        if st.session_state.get(key):
            return str(st.session_state.get(key))
    return "system"


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _pick_option(options: List[Dict[str, Any]], current_value: Any) -> int:
    if not options:
        return 0
    for idx, row in enumerate(options):
        if str(row.get("value")) == str(current_value):
            return idx
    return 0


def _excel_upload_to_df(uploaded_file):
    if uploaded_file is None:
        return None
    if uploaded_file.name.lower().endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)
