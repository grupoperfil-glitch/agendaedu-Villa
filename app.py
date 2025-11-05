# app.py — Dashboard CSAT (CSV) — FUNCIONA COM SEUS ARQUIVOS
# Baseado no código da outra empresa, adaptado para CSV + seus dados

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import re
import os
import shutil
from io import BytesIO
from zipfile import ZipFile
from datetime import date

# ====================== CONFIG ======================
DATA_DIR = "data_store"
os.makedirs(DATA_DIR, exist_ok=True)

SLA = {
    "WAITING_TIME_MAX_HOURS": 24,
    "CSAT_MIN": 4.0,
    "COMPLETION_RATE_MIN": 90.0,
    "EVAL_COVERAGE_MIN": 75.0,
}

CSAT_ORDER = ["Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"]

# Mapeamento de arquivos (SEUS NOMES REAIS)
FILE_PATTERNS = {
    "csat_by_cat": r"_data_product__csat_.*\.csv$",
    "csat_avg": r"_data_product__media_csat_.*\.csv$",
    "handle_avg": r"tempo_medio_de_atendimento_.*\.csv$",
    "wait_avg": r"tempo_medio_de_espera_.*\.csv$",
    "total": r"total_de_atendimentos_.*\.csv$",
    "completed": r"total_de_atendimentos_concluidos_.*\.csv$",
    "by_channel": r"tempo_medio_de_atendimento_por_canal_.*\.csv$",
}

EXPECTED_SCHEMAS = {
    "csat_by_cat": {"Categoria", "score_total"},
    "csat_avg": {"avg"},
    "handle_avg": {"mean_total"},
    "wait_avg": {"mean_total"},
    "total": {"total_tickets"},
    "completed": {"total_tickets"},
    "by_channel": {"Canal", "Tempo médio de atendimento", "Tempo médio de espera", "Total de atendimentos", "Total de atendimentos concluídos", "Média CSAT"},
}

# ====================== HELPERS ======================
def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

def hhmmss_to_seconds(s: str) -> int:
    if pd.isna(s) or not s: return 0
    s = str(s).strip()
    parts = s.split(":")
    if len(parts) != 3: return 0
    try:
        h, m, sec = map(int, parts)
        return h * 3600 + m * 60 + sec
    except:
        return 0

def seconds_to_hhmmss(sec: int) -> str:
    if pd.isna(sec): return "00:00:00"
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def classify_filename(name: str) -> str:
    for ftype, pattern in FILE_PATTERNS.items():
        if re.match(pattern, name, flags=re.IGNORECASE):
            return ftype
    return "unknown"

def load_csv_safe(file) -> pd.DataFrame:
    try:
        df = pd.read_csv(file)
        if df.empty:
            st.warning(f"Arquivo vazio: {file.name}")
            return pd.DataFrame()
        return df
    except Exception as e:
        st.error(f"Erro ao ler {file.name}: {e}")
        return pd.DataFrame()

def ensure_schema(df: pd.DataFrame, expected: set, label: str) -> pd.DataFrame:
    if df.empty:
        st.warning(f"{label}: vazio")
        return df
    df = df.rename(columns=lambda x: str(x).strip())
    cols = set(df.columns)
    if not expected.issubset(cols):
        st.warning(f"{label}: colunas erradas. Esperado: {sorted(expected)} | Encontrado: {sorted(cols)}")
        return pd.DataFrame()
    return df[list(expected)]

def validate_and_clean(data: dict) -> dict:
    cleaned = {}

    # CSAT por categoria
    if "csat_by_cat" in data:
        df = ensure_schema(data["csat_by_cat"], EXPECTED_SCHEMAS["csat_by_cat"], "CSAT por categoria")
        if not df.empty:
            df["Categoria"] = df["Categoria"].astype(str).str.strip()
            df["score_total"] = pd.to_numeric(df["score_total"], errors="coerce").fillna(0).astype(int)
            # Garante ordem
            ordered = []
            for cat in CSAT_ORDER:
                val = df.loc[df["Categoria"] == cat, "score_total"].sum()
                ordered.append({"Categoria": cat, "score_total": int(val)})
            cleaned["csat_by_cat"] = pd.DataFrame(ordered)

    # CSAT médio
    if "csat_avg" in data:
        df = ensure_schema(data["csat_avg"], EXPECTED_SCHEMAS["csat_avg"], "CSAT médio")
        if not df.empty:
            avg = pd.to_numeric(df["avg"], errors="coerce").iloc[0]
            cleaned["csat_avg"] = pd.DataFrame({"avg": [float(avg)]})

    # TMA / TME geral
    for key, col in [("handle_avg", "Tempo médio de atendimento"), ("wait_avg", "Tempo médio de espera")]:
        if key in data:
            df = ensure_schema(data[key], EXPECTED_SCHEMAS[key], col)
            if not df.empty:
                sec = hhmmss_to_seconds(df["mean_total"].iloc[0])
                cleaned[key] = pd.DataFrame({
                    "mean_total": [seconds_to_hhmmss(sec)],
                    "seconds": [sec]
                })

    # Totais
    for key, col in [("total", "Total de atendimentos"), ("completed", "Atendimentos concluídos")]:
        if key in data:
            df = ensure_schema(data[key], EXPECTED_SCHEMAS[key], col)
            if not df.empty:
                val = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
                cleaned[key] = pd.DataFrame({"total_tickets": [val]})

    # Por canal
    if "by_channel" in data:
        df = ensure_schema(data["by_channel"], EXPECTED_SCHEMAS["by_channel"], "Por canal")
        if not df.empty:
            df["Canal"] = df["Canal"].astype(str).str.strip()
            for c in ["Total de atendimentos", "Total de atendimentos concluídos"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
            df["Média CSAT"] = pd.to_numeric(df["Média CSAT"], errors="coerce")
            df["_handle_sec"] = df["Tempo médio de atendimento"].astype(str).apply(hhmmss_to_seconds)
            df["_wait_sec"] = df["Tempo médio de espera"].astype(str).apply(hhmmss_to_seconds)
            cleaned["by_channel"] = df

    return cleaned

def compute_kpis(cleaned: dict) -> dict:
    k = {
        "total": 0, "completed": 0, "completion_rate": 0,
        "wait_sec": 0, "csat": 0, "evaluated": 0, "coverage": 0
    }
    if "total" in cleaned: k["total"] = int(cleaned["total"]["total_tickets"].iloc[0])
    if "completed" in cleaned: k["completed"] = int(cleaned["completed"]["total_tickets"].iloc[0])
    if k["total"] > 0: k["completion_rate"] = k["completed"] / k["total"] * 100
    if "wait_avg" in cleaned: k["wait_sec"] = int(cleaned["wait_avg"]["seconds"].iloc[0])
    if "csat_avg" in cleaned: k["csat"] = float(cleaned["csat_avg"]["avg"].iloc[0])
    if "csat_by_cat" in cleaned: k["evaluated"] = int(cleaned["csat_by_cat"]["score_total"].sum())
    if k["completed"] > 0: k["coverage"] = k["evaluated"] / k["completed"] * 100
    return k

# ====================== PERSISTÊNCIA ======================
def save_month(mkey: str, data: dict):
    os.makedirs(f"{DATA_DIR}/{mkey}", exist_ok=True)
    cleaned = validate_and_clean(data)
    for t, df in cleaned.items():
        df.to_csv(f"{DATA_DIR}/{mkey}/{t}.csv", index=False)

def load_all() -> dict:
    result = {}
    if not os.path.isdir(DATA_DIR): return result
    for mkey in sorted(os.listdir(DATA_DIR)):
        path = f"{DATA_DIR}/{mkey}"
        if not os.path.isdir(path): continue
        payload = {}
        for f in os.listdir(path):
            if f.endswith(".csv"):
                t = f[:-4]
                try:
                    df = pd.read_csv(f"{path}/{f}")
                    payload[t] = df
                except: pass
        if payload: result[mkey] = payload
    return result

# ====================== APP ======================
st.set_page_config(page_title="CSAT Dashboard", layout="wide")
st.title("Dashboard CSAT — Upload CSV")

if "data" not in st.session_state:
    st.session_state.data = load_all()

# Sidebar
with st.sidebar:
    st.header("Mês")
    today = date.today()
    month = st.number_input("Mês", 1, 12, today.month)
    year = st.number_input("Ano", 2025, 2030, today.year)
    mk = month_key(year, month)

    st.subheader("Upload CSV")
    uploads = {
        "csat_by_cat": st.file_uploader("1) _data_product__csat_*.csv", type="csv"),
        "csat_avg": st.file_uploader("2) _data_product__media_csat_*.csv", type="csv"),
        "handle_avg": st.file_uploader("3) tempo_medio_de_atendimento_*.csv", type="csv"),
        "wait_avg": st.file_uploader("4) tempo_medio_de_espera_*.csv", type="csv"),
        "total": st.file_uploader("5) total_de_atendimentos_*.csv", type="csv"),
        "completed": st.file_uploader("6) total_de_atendimentos_concluidos_*.csv", type="csv"),
        "by_channel": st.file_uploader("7) tempo_medio_de_atendimento_por_canal_*.csv", type="csv"),
    }

    multi = st.file_uploader("Upload múltiplo", type="csv", accept_multiple_files=True)

    if st.button("Salvar mês"):
        raw = st.session_state.data.get(mk, {})
        for key, file in uploads.items():
            if file:
                df = load_csv_safe(file)
                if not df.empty:
                    ftype = classify_filename(file.name)
                    if ftype != "unknown":
                        raw[ftype] = df
                        st.success(f"{file.name} → {ftype}")

        for file in multi or []:
            ftype = classify_filename(file.name)
            if ftype != "unknown":
                df = load_csv_safe(file)
                if not df.empty:
                    raw[ftype] = df

        if raw:
            st.session_state.data[mk] = raw
            save_month(mk, raw)
            st.success(f"Mês {mk} salvo!")

# ====================== TABS ======================
tabs = st.tabs(["Visão Geral", "Por Canal", "Comparativo", "Dicionário"])

with tabs[0]:
    st.subheader(f"Visão Geral — {mk}")
    if mk not in st.session_state.data:
        st.info("Faça upload e salve o mês.")
    else:
        raw = st.session_state.data[mk]
        cleaned = validate_and_clean(raw)
        kpis = compute_kpis(cleaned)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total", kpis["total"])
        c2.metric("Concluídos", kpis["completed"])
        c3.metric("CSAT", f"{kpis['csat']:.2f}")
        c4.metric("Espera", f"{kpis['wait_sec']/3600:.1f}h")

        st.metric("Conclusão", f"{kpis['completion_rate']:.1f}%")
        st.metric("Cobertura", f"{kpis['coverage']:.1f}%")

        if "csat_by_cat" in cleaned:
            fig = px.bar(cleaned["csat_by_cat"], x="Categoria", y="score_total", title="CSAT por Categoria")
            st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    st.subheader("Por Canal")
    if "by_channel" not in st.session_state.data.get(mk, {}):
        st.info("Sem dados por canal.")
    else:
        df = validate_and_clean(st.session_state.data[mk])["by_channel"]
        fig = px.bar(df, x="Canal", y="Média CSAT", title="CSAT por Canal")
        st.plotly_chart(fig, use_container_width=True)

with tabs[2]:
    st.subheader("Comparativo Mensal")
    if len(st.session_state.data) < 2:
        st.info("Carregue pelo menos 2 meses.")
    else:
        rows = []
        for m, data in st.session_state.data.items():
            k = compute_kpis(validate_and_clean(data))
            rows.append({"Mês": m, "CSAT": k["csat"], "Conclusão %": k["completion_rate"]})
        comp = pd.DataFrame(rows)
        st.line_chart(comp.set_index("Mês"))

with tabs[3]:
    st.markdown("### Dicionário")
    st.write("Use os 7 CSVs com os nomes exatos que você enviou.")
