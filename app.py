# app.py — Dashboard CSAT Mensal (Escola Villa Criar)
# Versão simplificada focada APENAS na Escola Villa Criar.
# Não usa config.json e fixa o caminho dos dados.
# ATUALIZADO com a nova aba "Comparativo Mensal Por Canal".

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import re
import os
import requests # <- Necessário para a API do GitHub
import base64
import json     # <- Necessário para carregar o config.json
from io import BytesIO
from datetime import date

# ====================== CONFIG ======================
# --- CONFIGURAÇÃO FIXA (HARDCODED) ---
# Como este app é APENAS para a Escola Villa Criar,
# fixamos o caminho dos dados aqui.
EMPRESA_PATH_FIXO = "datavilla" 
EMPRESA_NOME_FIXO = "Escola Villa Criar"

# Mapeia os tipos para os padrões Regex
# Adiciona o operador | (OU) para aceitar os nomes de arquivo
# antigos (ex: total.csv) OU os nomes novos (ex: total_de_atendimentos...csv)
FILE_PATTERNS = {
    "dist_csat": r"^(_data_product__csat(_.*)?|csat_by_cat)\.csv$",
    "media_csat": r"^(_data_product__media_csat(_.*)?|csat_avg)\.csv$",
    "tempo_atendimento": r"^(tempo_medio_de_atendimento(_.*)?|handle_avg)\.csv$",
    "tempo_espera": r"^(tempo_medio_de_espera(_.*)?|wait_avg)\.csv$",
    "total_atendimentos": r"^(total_de_atendimentos(_.*)?|total)\.csv$",
    "concluidos": r"^(total_de_atendimentos_concluidos(_.*)?|completed)\.csv$",
    "por_canal": r"^(tempo_medio_de_atendimento_por_canal(_.*)?|by_channel)\.csv$",
}

# Metas de SLA
SLA = {
    "WAITING_TIME_MAX_SECONDS": 24 * 3600, # < 24h
    "CSAT_MIN": 4.0,
    "COMPLETION_RATE_MIN": 90.0,
    "EVAL_COVERAGE_MIN": 75.0,
    "NEAR_RATIO": 0.05 # margem ±5% (amarelo)
}

# Ordem do CSAT
CSAT_ORDER = [
    "Muito Insatisfeito", "Insatisfeito", "Neutro", "Satisfeito", "Muito Satisfeito"
]

# Define os tipos de arquivos
REQUIRED_TYPES = [
    "total_atendimentos", "concluidos", "tempo_atendimento", 
    "tempo_espera", "media_csat", "dist_csat"
]
OPTIONAL_TYPES = ["por_canal"]

# Define os esquemas esperados
EXPECTED_SCHEMAS = {
    "dist_csat": {"Categoria", "score_total"},
    "media_csat": {"avg"},
    "tempo_atendimento": {"mean_total"},
    "tempo_espera": {"mean_total"},
    "total_atendimentos": {"total_tickets"},
    "concluidos": {"total_tickets"},
    "por_canal": {
        "Canal", "Tempo médio de atendimento", "Tempo médio de espera",
        "Total de atendimentos", "Total de atendimentos concluídos", "Média CSAT"
    },
}
# --- FIM DA CONFIGURAÇÃO FIXA ---


# ====================== HELPERS (LÓGICA DE NEGÓCIO) ======================
# (Funções mantidas: init_state, month_key, hhmmss_to_seconds, etc.)

def init_state():
    """Inicializa o session_state se necessário."""
    if "data_cache" not in st.session_state:
        st.session_state.data_cache = {}

def month_key(year, month):
    """Formata a chave do mês como YYYY-MM."""
    return f"{int(year):04d}-{int(month):02d}"

def hhmmss_to_seconds(s: str) -> int:
    """Converte 'HH:MM:SS' para segundos inteiros."""
    if pd.isna(s):
        return 0
    s = str(s).strip()
    if not s or s.lower() in ["nan", "none"]:
        return 0
    
    h, m, sec = 0, 0, 0
    parts = s.split(":")
    if len(parts) == 3:
        try:
            h = int(parts[0]); m = int(parts[1]); sec = int(parts[2])
        except Exception:
            return 0
    elif len(parts) == 2:
        try:
            h = int(parts[0]); m = int(parts[1])
        except Exception:
            return 0
    elif len(parts) == 1:
        try:
            sec = int(parts[0])
        except Exception:
            return 0
            
    return h*3600 + m*60 + sec

def seconds_to_hhmmss(total: int) -> str:
    """Converte segundos inteiros para 'HH:MM:SS'."""
    if total is None or pd.isna(total) or not isinstance(total, (int, float)):
        return "00:00:00"
    total = int(total)
    h = total // 3600
    rem = total % 3600
    m = rem // 60
    s = rem % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def classify_filename(filename: str) -> str:
    """Classifica um nome de arquivo com base nos padrões."""
    for ftype, pattern in FILE_PATTERNS.items():
        if re.match(pattern, filename, flags=re.IGNORECASE):
            return ftype
    return "unknown"

def load_csv_result_sheet(uploaded_file) -> pd.DataFrame:
    """Lê um arquivo CSV (de upload ou URL)."""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}")
        return pd.DataFrame()

def ensure_schema(df: pd.DataFrame, expected_cols: set, file_label: str) -> pd.DataFrame:
    """Valida se o DataFrame contém as colunas esperadas."""
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    cols = set(df.columns)
    if not expected_cols.issubset(cols):
        st.warning(f"{file_label}: colunas inesperadas. Esperado: {sorted(expected_cols)} | Encontrado: {sorted(cols)}")
        return pd.DataFrame()
    return df

def ensure_csat_order(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que as categorias de CSAT estejam na ordem correta."""
    df = df.copy()
    present = set(df["Categoria"].astype(str).str.strip())
    rows = []
    for cat in CSAT_ORDER:
        val = 0
        if cat in present:
            val = int(pd.to_numeric(df.loc[df["Categoria"].str.strip() == cat, "score_total"], errors="coerce").sum())
        rows.append({"Categoria": cat, "score_total": val})
    return pd.DataFrame(rows)

def validate_and_clean(month_data: dict) -> dict:
    """
    Recebe um dicionário de DataFrames brutos e retorna
    DataFrames limpos e validados.
    """
    cleaned = {}
    
    # 1) CSAT por categoria
    if "dist_csat" in month_data:
        df = ensure_schema(month_data["dist_csat"], EXPECTED_SCHEMAS["dist_csat"], "CSAT por categoria")
        if not df.empty:
            df["Categoria"] = df["Categoria"].astype(str).str.strip()
            df["score_total"] = pd.to_numeric(df["score_total"], errors="coerce").fillna(0).astype(int)
            cleaned["dist_csat"] = ensure_csat_order(df)
            
    # 2) CSAT médio
    if "media_csat" in month_data:
        df = ensure_schema(month_data["media_csat"], EXPECTED_SCHEMAS["media_csat"], "CSAT médio")
        if not df.empty:
            try:
                avg_val = float(pd.to_numeric(df["avg"], errors="coerce").dropna().iloc[0])
            except Exception:
                avg_val = np.nan
            cleaned["media_csat"] = pd.DataFrame({"avg": [avg_val]})
            
    # 3) Tempo médio de atendimento
    if "tempo_atendimento" in month_data:
        df = ensure_schema(month_data["tempo_atendimento"], EXPECTED_SCHEMAS["tempo_atendimento"], "Tempo médio de atendimento")
        if not df.empty:
            sec = hhmmss_to_seconds(str(df["mean_total"].astype(str).iloc[0]))
            cleaned["tempo_atendimento"] = pd.DataFrame({"mean_total": [seconds_to_hhmmss(sec)], "seconds": [sec]})
            
    # 4) Tempo médio de espera
    if "tempo_espera" in month_data:
        df = ensure_schema(month_data["tempo_espera"], EXPECTED_SCHEMAS["tempo_espera"], "Tempo médio de espera")
        if not df.empty:
            sec = hhmmss_to_seconds(str(df["mean_total"].astype(str).iloc[0]))
            cleaned["tempo_espera"] = pd.DataFrame({"mean_total": [seconds_to_hhmmss(sec)], "seconds": [sec]})
            
    # 5) Totais
    if "total_atendimentos" in month_data:
        df = ensure_schema(month_data["total_atendimentos"], EXPECTED_SCHEMAS["total_atendimentos"], "Total de atendimentos")
        if not df.empty:
            total = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
            cleaned["total_atendimentos"] = pd.DataFrame({"total_tickets": [total]})
            
    if "concluidos" in month_data:
        df = ensure_schema(month_data["concluidos"], EXPECTED_SCHEMAS["concluidos"], "Atendimentos concluídos")
        if not df.empty:
            total = int(pd.to_numeric(df["total_tickets"], errors="coerce").sum())
            cleaned["concluidos"] = pd.DataFrame({"total_tickets": [total]})
            
    # 6) Por canal (opcional)
    if "por_canal" in month_data:
        df = ensure_schema(month_data["por_canal"], EXPECTED_SCHEMAS["por_canal"], "Por canal")
        if not df.empty:
            df["Canal"] = df["Canal"].astype(str).str.strip()
            df["Total de atendimentos"] = pd.to_numeric(df["Total de atendimentos"], errors="coerce").fillna(0).astype(int)
            df["Total de atendimentos concluídos"] = pd.to_numeric(df["Total de atendimentos concluídos"], errors="coerce").fillna(0).astype(int)
            df["Média CSAT"] = pd.to_numeric(df["Média CSAT"], errors="coerce")
            df["_handle_seconds"] = df["Tempo médio de atendimento"].astype(str).apply(hhmmss_to_seconds)
            df["_wait_seconds"] = df["Tempo médio de espera"].astype(str).apply(hhmmss_to_seconds)
            cleaned["por_canal"] = df
            
    return cleaned

def compute_kpis(cleaned: dict) -> dict:
    """Calcula os KPIs principais a partir dos dados limpos."""
    kpis = {
        "total": np.nan, "completed": np.nan, "completion_rate": np.nan,
        "handle_avg_sec": np.nan, "wait_avg_sec": np.nan,
        "csat_avg": np.nan, "evaluated": np.nan, "eval_coverage": np.nan
    }
    
    if "total_atendimentos" in cleaned:
        kpis["total"] = int(cleaned["total_atendimentos"]["total_tickets"].iloc[0])
    if "concluidos" in cleaned:
        kpis["completed"] = int(cleaned["concluidos"]["total_tickets"].iloc[0])
    if not pd.isna(kpis["total"]) and kpis["total"] > 0 and not pd.isna(kpis["completed"]):
        kpis["completion_rate"] = kpis["completed"] / kpis["total"] * 100.0
        
    if "tempo_atendimento" in cleaned:
        kpis["handle_avg_sec"] = int(cleaned["tempo_atendimento"]["seconds"].iloc[0])
    if "tempo_espera" in cleaned:
        kpis["wait_avg_sec"] = int(cleaned["tempo_espera"]["seconds"].iloc[0])
        
    if "media_csat" in cleaned:
        kpis["csat_avg"] = float(cleaned["media_csat"]["avg"].iloc[0])
    if "dist_csat" in cleaned:
        kpis["evaluated"] = int(pd.to_numeric(cleaned["dist_csat"]["score_total"], errors="coerce").sum())
    if not pd.isna(kpis["evaluated"]) and not pd.isna(kpis["completed"]) and kpis["completed"] > 0:
        if kpis["evaluated"] > kpis["completed"]:
            kpis["eval_coverage"] = 100.0
            st.warning(f"Inconsistência: N° de Avaliados ({kpis['evaluated']}) > N° de Concluídos ({kpis['completed']}). Cobertura definida como 100%.")
        else:
            kpis["eval_coverage"] = kpis["evaluated"] / kpis["completed"] * 100.0
            
    return kpis

def near_threshold(actual, target, greater_is_better=True, near_ratio=0.05):
    """Verifica se um valor está próximo da meta (para status 'Alerta')."""
    if target == 0 or pd.isna(actual):
        return False
    if greater_is_better:
        return (actual < target) and (actual >= target*(1 - near_ratio))
    else:
        return (actual > target) and (actual <= target*(1 + near_ratio))

def color_flag(ok: bool, warn: bool = False):
    """Retorna um ícone de status."""
    if ok:
        return "✅"
    if warn:
        return "⚠️"
    return "❌"

def sla_flags(kpis: dict):
    """Gera flags de status (OK/Alerta/Falha) para os KPIs."""
    flags = {}
    
    wt = kpis.get("wait_avg_sec", np.nan)
    if not pd.isna(wt):
        ok = wt < SLA["WAITING_TIME_MAX_SECONDS"]
        warn = near_threshold(wt, SLA["WAITING_TIME_MAX_SECONDS"], greater_is_better=False, near_ratio=SLA["NEAR_RATIO"])
        flags["wait"] = (ok, warn)
        
    cs = kpis.get("csat_avg", np.nan)
    if not pd.isna(cs):
        ok = cs >= SLA["CSAT_MIN"]
        warn = near_threshold(cs, SLA["CSAT_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["csat"] = (ok, warn)
        
    cr = kpis.get("completion_rate", np.nan)
    if not pd.isna(cr):
        ok = cr > SLA["COMPLETION_RATE_MIN"] # > 90%, não >=
        warn = near_threshold(cr, SLA["COMPLETION_RATE_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["completion"] = (ok, warn)
        
    ev = kpis.get("eval_coverage", np.nan)
    if not pd.isna(ev):
        ok = ev >= SLA["EVAL_COVERAGE_MIN"]
        warn = near_threshold(ev, SLA["EVAL_COVERAGE_MIN"], greater_is_better=True, near_ratio=SLA["NEAR_RATIO"])
        flags["coverage"] = (ok, warn)
        
    return flags

# ====================== PERSISTÊNCIA GITHUB ======================

@st.cache_data(ttl=300) # Cache de 5 minutos
def load_data_from_github(mes_key: str) -> dict:
    """
    Baixa todos os arquivos de dados de um mês/empresa específico do GitHub.
    Usa o EMPRESA_PATH_FIXO.
    """
    try:
        token = st.secrets["GH_TOKEN"]
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
    except KeyError as e:
        st.error(f"ERRO: Segredo do Streamlit não encontrado: {e}.")
        return {}
        
    # 1. Montar a URL da API para o *diretório*
    api_url = f"https://api.github.com/repos/{repo_name}/contents/{EMPRESA_PATH_FIXO}/{mes_key}?ref={branch}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # 2. Obter a lista de arquivos no diretório
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code != 200:
            if response.status_code == 404:
                return {} # Retorna dicionário vazio (nenhum dado)
            
            # --- LOG DE ERRO DETALHADO ---
            # Mostra outros erros (401, 403, 500) na tela
            st.error(f"Erro ao buscar dados ({api_url}): Status {response.status_code}")
            st.error(f"Verifique seus st.secrets (GH_TOKEN, GH_REPO) e permissões.")
            st.error(f"Resposta da API: {response.text}")
            # --- FIM DO LOG DE ERRO ---
            return {}
            
        file_list = response.json()
        if not isinstance(file_list, list):
            st.warning(f"Resposta inesperada da API do GitHub para {mes_key}.")
            return {}
            
    except Exception as e:
        st.error(f"Falha ao listar arquivos do GitHub para {mes_key}: {e}")
        return {}

    # 3. Mapear os padrões de arquivo para os arquivos reais
    month_data_raw = {}

    for file_type, pattern_regex in FILE_PATTERNS.items():
        if not pattern_regex:
            continue

        # 4. Encontra o arquivo correspondente na lista da API
        found_file = None
        for file_item in file_list:
            if file_item['type'] != 'file':
                continue
            
            file_name = file_item['name']
            match = re.match(pattern_regex, file_name, flags=re.IGNORECASE)
            
            if match:
                found_file = file_item
                break # Pára de procurar *arquivos* assim que um bate com o *padrão*

        # 5. Se encontramos, baixa o arquivo
        if found_file:
            download_url = found_file.get('download_url')
            if download_url:
                try:
                    df = pd.read_csv(download_url)
                    month_data_raw[file_type] = df
                except Exception as e:
                    st.warning(f"Falha ao ler o arquivo {found_file['name']}: {e}")

    return month_data_raw


@st.cache_data(ttl=3600) # Cache de 1 hora
def get_all_kpis() -> pd.DataFrame:
    """
    Busca KPIs de TODOS os meses para a aba 'Comparativo'.
    Usa o EMPRESA_PATH_FIXO.
    """
    try:
        token = st.secrets["GH_TOKEN"]
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
    except KeyError as e:
        st.error(f"ERRO: Segredo do Streamlit não encontrado: {e}.")
        return pd.DataFrame()

    # 1. Listar diretórios (meses) na pasta da empresa
    api_url = f"https://api.github.com/repos/{repo_name}/contents/{EMPRESA_PATH_FIXO}?ref={branch}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    try:
        response = requests.get(api_url, headers=headers)
        # --- LOG DE ERRO DETALHADO ---
        if response.status_code != 200:
             # Erro ao listar os meses *é* um problema
            st.error(f"Erro ao listar pastas ({api_url}): Status {response.status_code}")
            st.error(f"Verifique seus st.secrets (GH_TOKEN, GH_REPO) e permissões.")
            st.error(f"Resposta da API: {response.text}")
            return pd.DataFrame()
        # --- FIM DO LOG DE ERRO ---
            
        content = response.json()
        
        month_keys = sorted([
            item['name'] for item in content 
            if item['type'] == 'dir' and re.match(r'^\d{4}-\d{2}$', item['name'])
        ], reverse=True) 
        
        if not month_keys:
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Falha ao listar meses no repositório: {e}")
        return pd.DataFrame()

    # 2. Para cada mês, carregar dados e calcular KPIs
    rows = []
    
    for mkey in month_keys[:12]: # Limita a 12 meses
        try:
            raw_data = load_data_from_github(mkey) # Não precisa mais de empresa_path
            if not raw_data:
                continue
            
            cleaned = validate_and_clean(raw_data)
            k = compute_kpis(cleaned)
            
            rows.append({
                "mes": mkey,
                "total": k.get("total"),
                "concluidos": k.get("completed"),
                "taxa_conclusao": k.get("completion_rate"),
                "tempo_espera_s": k.get("wait_avg_sec"),
                "tempo_atendimento_s": k.get("handle_avg_sec"),
                "csat_medio": k.get("csat_avg"),
                "cobertura_%": k.get("eval_coverage"),
            })
        except Exception as e:
            st.warning(f"Falha ao processar dados para o mês {mkey}: {e}")

    # --- CORREÇÃO (KeyError: 'mes') ---
    # Se 'rows' estiver vazio, retorna um DataFrame vazio
    if not rows:
        return pd.DataFrame()
    # --- FIM DA CORREÇÃO ---
            
    comp = pd.DataFrame(rows).sort_values("mes")
    return comp

# --- INÍCIO DA NOVA FUNÇÃO ---
@st.cache_data(ttl=3600) # Cache de 1 hora
def get_all_channel_data() -> pd.DataFrame:
    """
    Busca dados de TODOS os meses e TODOS os canais para a nova aba.
    Usa o EMPRESA_PATH_FIXO.
    """
    try:
        token = st.secrets["GH_TOKEN"]
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
    except KeyError as e:
        st.error(f"ERRO: Segredo do Streamlit não encontrado: {e}.")
        return pd.DataFrame()

    # 1. Listar diretórios (meses) na pasta da empresa
    # (Exatamente como em get_all_kpis)
    api_url = f"https://api.github.com/repos/{repo_name}/contents/{EMPRESA_PATH_FIXO}?ref={branch}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code != 200:
            st.error(f"Erro ao listar pastas ({api_url}): Status {response.status_code}")
            return pd.DataFrame()
            
        content = response.json()
        month_keys = sorted([
            item['name'] for item in content 
            if item['type'] == 'dir' and re.match(r'^\d{4}-\d{2}$', item['name'])
        ]) # Ordenado do mais antigo para o mais novo
        
        if not month_keys:
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Falha ao listar meses no repositório: {e}")
        return pd.DataFrame()

    # 2. Para cada mês, carregar APENAS os dados por canal
    all_dfs = []
    
    for mkey in month_keys:
        try:
            # Reutiliza nossa função de leitura principal
            raw_data = load_data_from_github(mkey) 
            
            if "por_canal" in raw_data:
                df = raw_data["por_canal"].copy()
                
                # Garante que as colunas esperadas de "por_canal" existam
                schema = EXPECTED_SCHEMAS["por_canal"]
                if not schema.issubset(set(df.columns)):
                    st.warning(f"Arquivo 'por_canal' em {mkey} com esquema inválido. Pulando.")
                    continue

                df['mes'] = mkey # Adiciona a coluna 'mês'
                all_dfs.append(df)
            
        except Exception as e:
            st.warning(f"Falha ao processar dados 'por_canal' para o mês {mkey}: {e}")

    if not all_dfs:
        return pd.DataFrame()
            
    # 3. Concatenar e limpar os dados
    master_df = pd.concat(all_dfs, ignore_index=True)
    
    # 4. Limpeza e Conversão de Tipos (essencial para os gráficos)
    try:
        master_df['Canal'] = master_df['Canal'].astype(str).str.strip()
        master_df['TME_sec'] = master_df['Tempo médio de espera'].astype(str).apply(hhmmss_to_seconds)
        master_df['TME_Horas'] = master_df['TME_sec'] / 3600
        master_df['TA_sec'] = master_df['Tempo médio de atendimento'].astype(str).apply(hhmmss_to_seconds)
        master_df['TA_Horas'] = master_df['TA_sec'] / 3600
        master_df['Média CSAT'] = pd.to_numeric(master_df['Média CSAT'], errors='coerce')
        master_df['Total de atendimentos'] = pd.to_numeric(master_df['Total de atendimentos'], errors='coerce').fillna(0)
        master_df['Total de atendimentos concluídos'] = pd.to_numeric(master_df['Total de atendimentos concluídos'], errors='coerce').fillna(0)
        
        # Cálculo de % Concluídos por Canal
        master_df['% Concluídos'] = (
            (master_df['Total de atendimentos concluídos'] / master_df['Total de atendimentos']) * 100
        ).fillna(0)
        # Lida com divisão por zero (0 / 0 = NaN, que vira 0)
        master_df.loc[master_df['Total de atendimentos'] == 0, '% Concluídos'] = 0

    except Exception as e:
        st.error(f"Falha ao limpar dados mestre de canal: {e}")
        return pd.DataFrame()

    return master_df
# --- FIM DA NOVA FUNÇÃO ---


def upload_to_github(file_content: bytes, mes_key: str, target_filename: str):
    """
    Faz upload (ou atualização) de um único arquivo para o repositório GitHub.
    Usa o EMPRESA_PATH_FIXO.
    """
    try:
        token = st.secrets["GH_TOKEN"]
        repo_name = st.secrets["GH_REPO"]
        branch = st.secrets["GH_BRANCH"]
        author = {
            "name": st.secrets["GH_COMMITS_AUTHOR_NAME"],
            "email": st.secrets["GH_COMMITS_AUTHOR_EMAIL"],
        }
    except KeyError as e:
        st.sidebar.error(f"ERRO: Segredo não configurado: {e}")
        return False

    # Constrói o path no repositório
    path_no_repo = f"{EMPRESA_PATH_FIXO}/{mes_key}/{target_filename}"
    api_url = f"https://api.github.com/repos/{repo_name}/contents/{path_no_repo}"
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    # 1. Verifica se o arquivo já existe para obter o 'sha' (necessário para update)
    sha = None
    try:
        response_get = requests.get(api_url, headers=headers)
        if response_get.status_code == 200:
            sha = response_get.json()['sha']
    except Exception as e:
        st.sidebar.warning(f"Não foi possível verificar {target_filename}: {e}")

    # 2. Prepara os dados para o PUT (upload)
    content_b64 = base64.b64encode(file_content).decode('utf-8')
    
    data = {
        "message": f"Upload de dados: {path_no_repo}",
        "author": author,
        "content": content_b64,
        "branch": branch,
    }
    
    if sha:
        data["sha"] = sha

    # 3. Faz o PUT (cria ou atualiza o arquivo)
    try:
        response_put = requests.put(api_url, headers=headers, json=data)
        
        if response_put.status_code == 201: # Criado
            st.sidebar.success(f"Arquivo '{target_filename}' criado.")
            return True
        elif response_put.status_code == 200: # Atualizado
            st.sidebar.success(f"Arquivo '{target_filename}' atualizado.")
            return True
        else:
            st.sidebar.error(f"Erro ({response_put.status_code}) ao enviar '{target_filename}': {response_put.json().get('message')}")
            return False
            
    except Exception as e:
        st.sidebar.error(f"Exceção ao enviar '{target_filename}': {e}")
        return False

# ====================== APP (UI) ======================
st.set_page_config(page_title=f"CSAT Dashboard - {EMPRESA_NOME_FIXO}", layout="wide")
init_state()

# --- SIDEBAR (Seleção e Upload) ---

# --- 1. Seleção de Análise ---
st.sidebar.title("Filtros de Análise")
st.sidebar.markdown(f"**Instituição:** `{EMPRESA_NOME_FIXO}`")
st.sidebar.caption("Versão: 4.0 (Aba Comp. Canal)") # <-- Mudei a versão

# Seletores de Mês/Ano
col_m, col_y = st.sidebar.columns(2)
today = date.today()
if today.day <= 5:
    # Se for o início do mês, assume o mês anterior como padrão
    today = today.replace(day=1) - pd.DateOffset(months=1)

# --- INÍCIO DA ALTERAÇÃO (ANOS DE ANÁLISE) ---
year_list = list(range(2025, 2031)) # 2025 até 2030
# Tenta encontrar o ano atual na lista, senão usa o primeiro (2025)
try:
    default_index_anl = year_list.index(today.year)
except ValueError:
    default_index_anl = 0 # Padrão é 2025

month = col_m.selectbox("Mês", list(range(1, 13)), index=today.month - 1)
year = col_y.selectbox("Ano", year_list, index=default_index_anl)
# --- FIM DA ALTERAÇÃO (ANOS DE ANÁLISE) ---
current_month_key = month_key(year, month)


# --- 2. Seção de Upload (em um expander) ---
with st.sidebar.expander("Upload de Novos Dados"):
    st.markdown(f"### Enviar arquivos para `{EMPRESA_NOME_FIXO}`")
    
    # Mês/Ano para Upload
    st.caption("Selecione o mês/ano de destino do upload:")
    col_um, col_uy = st.columns(2)

    # --- INÍCIO DA ALTERAÇÃO (ANOS DE UPLOAD) ---
    # Reutiliza a year_list definida acima
    # Tenta encontrar o ano atual na lista, senão usa o primeiro (2025)
    try:
        default_index_upl = year_list.index(today.year)
    except ValueError:
        default_index_upl = 0 # Padrão é 2025

    upload_month = col_um.selectbox("Mês (Destino)", list(range(1, 13)), index=today.month - 1, key="upload_mes")
    upload_year = col_uy.selectbox("Ano (Destino)", year_list, index=default_index_upl, key="upload_ano")
    # --- FIM DA ALTERAÇÃO (ANOS DE UPLOAD) ---
    upload_month_key = month_key(upload_year, upload_month)

    st.markdown("---")
    st.caption(f"Os arquivos serão enviados para: `{EMPRESA_PATH_FIXO}/{upload_month_key}/`")

    # Mapeia os nomes de arquivo base para os tipos (ex: "dist_csat")
    # Ajusta os rótulos para refletir os nomes exatos dos arquivos.
    upload_map_config = {
        "dist_csat": "1) Distribuição CSAT (_data_product__csat_...)",
        "media_csat": "2) Média CSAT (_data_product__media_csat_...)",
        "tempo_atendimento": "3) Tempo Méd. Atendimento (tempo_medio_de_atendimento_...)",
        "tempo_espera": "4) Tempo Méd. Espera (tempo_medio_de_espera_...)",
        "total_atendimentos": "5) Total Atendimentos (total_de_atendimentos_...)",
        "concluidos": "6) Atendimentos Concluídos (total_de_atendimentos_concluidos_...)",
        "por_canal": "7) Dados Por Canal (tempo_medio_de_atendimento_por_canal_...)",
    }

    upload_files_map = {} # Armazena {UploadedFile: target_filename}
    
    for file_type, label in upload_map_config.items():
        uploaded_file = st.file_uploader(label, type=["csv"], key=f"upload_{file_type}")
        
        if uploaded_file:
            # Verifica se o nome do arquivo *upado* bate com o padrão
            if not re.match(FILE_PATTERNS[file_type], uploaded_file.name, flags=re.IGNORECASE):
                st.warning(f"O nome '{uploaded_file.name}' não parece ser um arquivo de '{label}'. Verifique o arquivo.")
            
            # Usa o nome original do arquivo upado como nome de destino
            upload_files_map[uploaded_file] = uploaded_file.name 

    # Botão de Envio
    if st.button("Enviar Arquivos para o GitHub"):
        if not upload_files_map:
            st.sidebar.warning("Nenhum arquivo selecionado para envio.")
        else:
            try:
                # Verifica os segredos
                st.secrets["GH_TOKEN"]
                st.secrets["GH_REPO"]
                st.secrets["GH_BRANCH"]
                st.secrets["GH_COMMITS_AUTHOR_NAME"]
                st.secrets["GH_COMMITS_AUTHOR_EMAIL"]
            except KeyError as e:
                st.sidebar.error(f"ERRO: Segredo não configurado: {e}.")
                st.stop()

            # Processa o upload
            with st.spinner(f"Enviando {len(upload_files_map)} arquivo(s) para {upload_month_key}..."):
                success_count = 0
                for uploaded_file, target_filename in upload_files_map.items():
                    file_content = uploaded_file.getvalue()
                    
                    if upload_to_github(file_content, upload_month_key, target_filename):
                        success_count += 1
                
                if success_count == len(upload_files_map):
                    st.sidebar.success("Todos os arquivos foram enviados!")
                    st.cache_data.clear() # Limpa o cache
                else:
                    st.sidebar.error("Alguns arquivos falharam no envio.")


# ====================== CONTEÚDO PRINCIPAL ======================
st.title(f"Dashboard CSAT - {EMPRESA_NOME_FIXO}")
st.caption(f"Exibindo dados do mês: **{current_month_key}**")

# Carrega os dados do mês selecionado no sidebar
try:
    raw_month_data = load_data_from_github(current_month_key)
except Exception as e:
    st.error(f"Falha crítica ao carregar dados do GitHub: {e}")
    raw_month_data = {}

# --- INÍCIO DA ALTERAÇÃO (ORDEM DAS ABAS) ---
# Define as abas na nova ordem solicitada
tabs = st.tabs([
    "Visão Geral", 
    "Comparativo Mensal", 
    "Por Canal", 
    "Comparativo Mensal Por Canal", # <-- NOVA ABA
    "Dicionário de Dados"
])
# --- FIM DA ALTERAÇÃO (ORDEM DAS ABAS) ---


# --- 1) Visão Geral ---
with tabs[0]:

    # Define 'cleaned' como um dicionário vazio (para evitar NameError)
    cleaned = {} 
    
    if not raw_month_data:
        st.info(f"Nenhum dado encontrado para '{EMPRESA_NOME_FIXO}' em '{current_month_key}' no GitHub.")
    else:
        cleaned = validate_and_clean(raw_month_data)
        
        missing_files = []
        for req in REQUIRED_TYPES:
            if req not in cleaned:
                pattern_name = FILE_PATTERNS.get(req, req).replace(r"\.", ".").replace(".*", "_*")
                missing_files.append(pattern_name)
        if missing_files:
            st.warning(f"Arquivos obrigatórios ausentes ou inválidos em {current_month_key}: {', '.join(missing_files)}")
        
        kpis = compute_kpis(cleaned)
        flags = sla_flags(kpis)
        
        c1, c2, c3, c4 = st.columns(4)
        c5, c6, c7 = st.columns(3)
        
        total = kpis.get("total")
        completed = kpis.get("completed")
        cr = kpis.get("completion_rate")
        ht = kpis.get("handle_avg_sec")
        wt = kpis.get("wait_avg_sec")
        cs = kpis.get("csat_avg")
        ev = kpis.get("evaluated")
        cov = kpis.get("eval_coverage")

        # Indicadores
        c1.metric("Total de atendimentos", f"{int(total) if not pd.isna(total) else '-'}")
        c2.metric("Atendimentos concluídos", f"{int(completed) if not pd.isna(completed) else '-'}")
        
        comp_icon = color_flag(*(flags.get("completion",(False,False))))
        c3.metric("Porcentagem de atendimentos concluídos", 
                  f"{(f'{cr:.1f}%' if not pd.isna(cr) else '-')}", 
                  help=f"Meta > {SLA['COMPLETION_RATE_MIN']}% {comp_icon}")
                  
        c4.metric("Tempo médio de atendimento", seconds_to_hhmmss(ht))
        
        w_ok, w_warn = flags.get("wait",(False,False))
        c5.metric("Tempo médio de espera", 
                  seconds_to_hhmmss(wt), 
                  help=f"Meta < 24:00:00 {color_flag(w_ok, w_warn)}")
                  
        cs_ok, cs_warn = flags.get("csat",(False,False))
        c6.metric("Média CSAT", 
                  f"{cs:.2f}" if not pd.isna(cs) else "-", 
                  help=f"Meta > {SLA['CSAT_MIN']} {color_flag(cs_ok, cs_warn)}")
                  
        cov_ok, cov_warn = flags.get("coverage",(False,False))
        c7.metric("Porcentagem de atendimentos avaliados", 
                  f"{(f'{cov:.1f}%' if not pd.isna(cov) else '-')}", 
                  help=f"Meta ≥ {SLA['EVAL_COVERAGE_MIN']}% {color_flag(cov_ok, cov_warn)}")

        st.markdown("---")
        
        # Gráfico
        if "dist_csat" in cleaned:
            dist = cleaned["dist_csat"].copy()
            dist["percent"] = dist["score_total"] / dist["score_total"].sum() * 100 if dist["score_total"].sum() > 0 else 0
            
            left, right = st.columns([2,1])
            with left:
                fig = px.bar(dist, x="Categoria", y="percent", title="Distribuição do CSAT", text=dist["percent"].round(1))
                fig.update_layout(xaxis_title="", yaxis_title="%")
                st.plotly_chart(fig, use_container_width=True)
            with right:
                st.dataframe(dist, use_container_width=True)
                st.download_button("Baixar tabela (CSV)", data=dist.to_csv(index=False).encode("utf-8"), file_name=f"csat_{current_month_key}.csv")
        else:
            st.warning("Arquivo de 'Distribuição do CSAT' não encontrado para o gráfico.")

# --- 2) Comparativo Mensal ---
# (Este era o código da antiga tabs[2])
with tabs[1]:
    st.subheader("Comparativo Mensal (KPIs Globais)")
    
    with st.spinner(f"Carregando histórico de KPIs para {EMPRESA_NOME_FIXO}..."):
        comp_df = get_all_kpis()
    
    if comp_df.empty:
        st.info(f"Nenhum dado histórico encontrado para '{EMPRESA_NOME_FIXO}'. Carregue dados de pelo menos um mês.")
    elif len(comp_df) < 2:
        st.info("Carregue dados de pelo menos dois meses para habilitar o comparativo.")
        st.dataframe(comp_df, use_container_width=True)
    else:
        if "tempo_espera_s" in comp_df.columns:
            comp_df["tempo_espera_h"] = comp_df["tempo_espera_s"] / 3600
        if "tempo_atendimento_s" in comp_df.columns:
            comp_df["tempo_atendimento_h"] = comp_df["tempo_atendimento_s"] / 3600
            
        st.dataframe(comp_df, use_container_width=True)
        st.download_button("Baixar comparativo (CSV)", data=comp_df.to_csv(index=False).encode("utf-8"), file_name=f"comparativo_mensal_{EMPRESA_PATH_FIXO}.csv")
        
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.line(comp_df, x="mes", y="csat_medio", markers=True, title="Média CSAT geral (Mensal)"), use_container_width=True)
            st.plotly_chart(px.line(comp_df, x="mes", y="tempo_espera_h", markers=True, title="Média do tempo de espera geral (Mensal)"), use_container_width=True)
        with c2:
            st.plotly_chart(px.line(comp_df, x="mes", y="taxa_conclusao", markers=True, title="Porcentagem de atendimentos concluídos (Mensal)"), use_container_width=True)
            st.plotly_chart(px.bar(comp_df, x="mes", y="total", title="Total de atendimentos recebidos (Mensal)"), use_container_width=True)

# --- 3) Por Canal ---
# (Este era o código da antiga tabs[1])
with tabs[2]:
    st.subheader(f"Indicadores por Canal (Mês: {current_month_key})")
    
    # 'cleaned' foi definido na Aba 1 e está disponível aqui
    if "por_canal" not in cleaned:
        st.info("Arquivo 'por canal' não disponível para o mês selecionado.")
    else:
        dfc = cleaned["por_canal"].copy()
        channels_available = sorted(dfc["Canal"].astype(str).unique())
        selected_channels = st.multiselect("Filtrar canais", channels_available, default=channels_available, key="channel_filter")
        
        if selected_channels:
            dfc = dfc[dfc["Canal"].astype(str).isin(selected_channels)]
            
        st.dataframe(dfc, use_container_width=True)
        st.download_button("Baixar por canal (CSV)", data=dfc.to_csv(index=False).encode("utf-8"), file_name=f"por_canal_{current_month_key}.csv")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(px.bar(dfc, x="Canal", y="Média CSAT", title="Média CSAT por canal"), use_container_width=True)
        with col2:
            dft = dfc.copy()
            dft["Tempo médio de espera (h)"] = dft["_wait_seconds"] / 3600
            st.plotly_chart(px.bar(dft, x="Canal", y="Tempo médio de espera (h)", title="Tempo médio de espera por canal (horas)"), use_container_width=True)
        
        col3, col4 = st.columns(2)
        with col3:
            dft = dfc.copy()
            dft["Tempo médio de atendimento (h)"] = dft["_handle_seconds"] / 3600
            st.plotly_chart(px.bar(dft, x="Canal", y="Tempo médio de atendimento (h)", title="Tempo médio de atendimento por canal (horas)"), use_container_width=True)
        with col4:
            dfc["% Concluídos"] = (dfc["Total de atendimentos concluídos"] / dfc["Total de atendimentos"] * 100).fillna(0)
            st.plotly_chart(px.bar(dfc, x="Canal", y="% Concluídos", title="% de atendimentos concluídos por canal"), use_container_width=True)

# --- 4) Comparativo Mensal Por Canal ---
# --- INÍCIO DA NOVA ABA ---
with tabs[3]:
    st.subheader("Comparativo Mensal Por Canal")

    with st.spinner(f"Carregando histórico de dados por canal para {EMPRESA_NOME_FIXO}..."):
        channel_df = get_all_channel_data()

    if channel_df.empty:
        st.info(f"Nenhum dado histórico 'por canal' encontrado para '{EMPRESA_NOME_FIXO}'.")
    else:
        # --- Filtros ---
        st.markdown("#### Filtros da Aba")
        
        # Filtro de Canal
        all_channels = sorted(channel_df['Canal'].unique())
        selected_channels = st.multiselect(
            "Selecione um ou mais canais",
            options=all_channels,
            default=all_channels, # Começa com todos selecionados
            key="multi_channel_filter"
        )
        
        # Filtro de Período
        all_months = sorted(channel_df['mes'].unique())
        if len(all_months) > 1:
            min_month, max_month = st.select_slider(
                "Selecione o período (Mês/Ano)",
                options=all_months,
                value=(all_months[0], all_months[-1]) # Começa com o período completo
            )
        else:
            min_month = all_months[0]
            max_month = all_months[0]

        # Aplicar filtros
        filtered_df = channel_df[
            (channel_df['Canal'].isin(selected_channels)) &
            (channel_df['mes'] >= min_month) &
            (channel_df['mes'] <= max_month)
        ]

        if filtered_df.empty:
            st.warning("Nenhum dado encontrado para os filtros selecionados.")
        else:
            st.markdown("---")
            
            # --- Gráficos de Evolução ---
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### Evolução da Média CSAT por Canal")
                fig_csat = px.line(
                    filtered_df, 
                    x="mes", 
                    y="Média CSAT", 
                    color="Canal",
                    markers=True,
                    title="Média CSAT (Mensal)"
                )
                fig_csat.update_layout(yaxis_range=[1,5]) # Fixa o eixo de 1 a 5
                st.plotly_chart(fig_csat, use_container_width=True)
                
                st.markdown("##### Evolução da % de Atendimentos Concluídos")
                fig_pct = px.line(
                    filtered_df,
                    x="mes",
                    y="% Concluídos",
                    color="Canal",
                    markers=True,
                    title="% Concluídos (Mensal)"
                )
                fig_pct.update_layout(yaxis_range=[0,100]) # Fixa o eixo de 0 a 100
                st.plotly_chart(fig_pct, use_container_width=True)

            with col2:
                st.markdown("##### Evolução do Tempo Médio de Espera (Horas)")
                fig_tme = px.line(
                    filtered_df,
                    x="mes",
                    y="TME_Horas",
                    color="Canal",
                    markers=True,
                    title="Tempo Médio de Espera (Horas) (Mensal)"
                )
                st.plotly_chart(fig_tme, use_container_width=True)
                
                st.markdown("##### Volume de Atendimentos por Canal (Mensal)")
                fig_vol = px.bar(
                    filtered_df,
                    x="mes",
                    y="Total de atendimentos",
                    color="Canal",
                    title="Volume de Atendimentos (Mensal)",
                    barmode="stack" # Empilhado, como solicitado no 'reasoning'
                )
                st.plotly_chart(fig_vol, use_container_width=True)
# --- FIM DA NOVA ABA ---

# --- 5) Dicionário de Dados ---
# (Este era o código da antiga tabs[3])
with tabs[4]:
    st.subheader("Dicionário de Dados e SLAs")
    st.markdown("**Arquivos .csv por mês (Padrões de Nome)**")
    
    rows = "<ul>"
    for key, pattern in FILE_PATTERNS.items():
        schema = EXPECTED_SCHEMAS.get(key)
        cols = f" (Colunas: `{', '.join(schema)}`)" if schema else ""
        pattern_fmt = pattern.replace(r"\.", ".").replace(".*", "_*")
        rows += f"<li>Padrão: <code>{pattern_fmt}</code> — Mapeado para <b>{key}</b>{cols}</li>"
    rows += "</ul>"
    st.markdown(rows, unsafe_allow_html=True)

    st.markdown(f"""
**Métricas e fórmulas:**
- Total: soma de `total_tickets` (de `total_atendimentos`)
- Concluídos: soma de `total_tickets` (de `concluidos`)
- Taxa de conclusão (%) = `concluídos / total * 100`
- Tempo médio de atendimento/espera: `mean_total` (HH:MM:SS)
- CSAT médio (1–5): `avg` (de `media_csat`)
- Cobertura de avaliação (%) = `avaliadas / concluídos * 100`, (avaliadas = soma(`score_total`) de `dist_csat`)
- Ordem CSAT: {", ".join(CSAT_ORDER)}

**SLAs (Metas):**
- **Tempo de Espera:** < 24:00:00 
- **CSAT Médio:** ≥ {SLA['CSAT_MIN']}
- **Taxa de Conclusão:** > {SLA['COMPLETION_RATE_MIN']}%
- **Cobertura de Avaliação:** ≥ {SLA['EVAL_COVERAGE_MIN']}%

**Persistência:**
- Os dados são lidos e escritos no repositório GitHub (definido em `st.secrets`).
- O caminho base para esta instituição é: `{EMPRESA_PATH_FIXO}/`
""")
