"""
App principal — Dashboards PURAFOR
Executa via: streamlit run app.py
"""
import os
import sys
import tempfile
from datetime import datetime

from datetime import date

import streamlit as st
import streamlit.components.v1 as components

# ─── Configuração da página ───────────────────────────────────────
st.set_page_config(
    page_title="Dashboards PURAFOR",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Injetar credenciais Omie via st.secrets (Streamlit Cloud) ───
# Quando rodando localmente sem secrets.toml, usa as variáveis de
# ambiente já definidas (ou o fallback hardcoded em PURAFOR_VENDAS.py).
try:
    if "OMIE_APP_KEY" in st.secrets:
        os.environ["OMIE_APP_KEY"]    = st.secrets["OMIE_APP_KEY"]
        os.environ["OMIE_APP_SECRET"] = st.secrets["OMIE_APP_SECRET"]
except Exception:
    pass

# ─── Sidebar — menu de dashboards ────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Dashboards PURAFOR")
    st.markdown("---")

    opcoes = {
        "📊 Dashboard de Vendas — PURAFOR": "purafor_vendas",
        # Adicione futuros dashboards aqui:
        # "📦 Estoque — PURAFOR": "purafor_estoque",
    }

    escolha = st.radio(
        "Selecione o dashboard:",
        list(opcoes.keys()),
        label_visibility="collapsed",
    )
    pagina = opcoes[escolha]

    # ── Período de busca ──────────────────────────────────────────
    st.markdown("### 📅 Período")
    _hoje    = date.today()
    _ini_def = date(_hoje.year, 1, 1)

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        data_ini_sel = st.date_input(
            "Data inicial",
            value=_ini_def,
            min_value=date(2020, 1, 1),
            max_value=_hoje,
            format="DD/MM/YYYY",
            key="periodo_ini",
        )
    with col_d2:
        data_fim_sel = st.date_input(
            "Data final",
            value=_hoje,
            min_value=date(2020, 1, 1),
            max_value=_hoje,
            format="DD/MM/YYYY",
            key="periodo_fim",
        )

    st.markdown("---")
    st.caption(f"Última execução: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

# ═══════════════════════════════════════════════════════════════════
# PÁGINA: Dashboard de Vendas — PURAFOR
# ═══════════════════════════════════════════════════════════════════
if pagina == "purafor_vendas":

    st.title("📊 Dashboard de Vendas — PURAFOR")

    # ── Chave de cache na sessão (inclui período p/ invalidar ao mudar datas) ──
    HTML_KEY   = "purafor_html"
    STATUS_KEY = "purafor_status"
    TIME_KEY   = "purafor_time"
    PERIOD_KEY = "purafor_period"

    # Converte datas para string formato Omie
    _data_ini_str = data_ini_sel.strftime("%d/%m/%Y")
    _data_fim_str = data_fim_sel.strftime("%d/%m/%Y")
    _period_id    = f"{_data_ini_str}_{_data_fim_str}"

    # Invalida cache se o período mudou
    if st.session_state.get(PERIOD_KEY) != _period_id:
        st.session_state.pop(HTML_KEY, None)
        st.session_state.pop(STATUS_KEY, None)

    # ── Barra de ação ─────────────────────────────────────────────
    col1, col2 = st.columns([3, 1])
    with col2:
        btn_atualizar = st.button(
            "🔄 Atualizar Dados",
            use_container_width=True,
            type="primary",
        )

    # ── Executa coleta quando clicado ────────────────────────────
    if btn_atualizar or HTML_KEY not in st.session_state:
        with st.spinner("⏳ Coletando dados da API Omie e gerando dashboard..."):
            log_container = st.empty()

            # Redireciona print() para o Streamlit
            import io, contextlib
            log_buf = io.StringIO()

            # Importa o módulo (re-importa se necessário para pegar env vars)
            _base = os.path.dirname(os.path.abspath(__file__))
            if _base not in sys.path:
                sys.path.insert(0, _base)

            import importlib
            if "PURAFOR_VENDAS" in sys.modules:
                pv = sys.modules["PURAFOR_VENDAS"]
                # Atualiza credenciais se mudaram
                pv.OMIE_APP_KEY    = os.getenv("OMIE_APP_KEY",    pv.OMIE_APP_KEY)
                pv.OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", pv.OMIE_APP_SECRET)
            else:
                import PURAFOR_VENDAS as pv

            # Gera HTML em arquivo temporário
            with tempfile.NamedTemporaryFile(
                suffix=".html", delete=False, mode="w", encoding="utf-8"
            ) as tmp:
                tmp_path = tmp.name

            try:
                with contextlib.redirect_stdout(log_buf):
                    html_content = pv.main(
                        saida_html=tmp_path,
                        saida_excel=None,          # sem Excel no cloud
                        data_ini=_data_ini_str,
                        data_fim=_data_fim_str,
                    )

                if html_content:
                    st.session_state[HTML_KEY]    = html_content
                    st.session_state[STATUS_KEY]  = "ok"
                    st.session_state[PERIOD_KEY]  = _period_id
                    st.session_state[TIME_KEY]    = datetime.now().strftime(
                        "%d/%m/%Y às %H:%M:%S"
                    )
                    log_container.success("✅ Dashboard gerado com sucesso!")
                else:
                    st.session_state[STATUS_KEY] = "erro"
                    log_container.error(
                        "❌ Nenhum dado encontrado. Verifique as credenciais e o período."
                    )
            except Exception as exc:
                import traceback
                st.session_state[STATUS_KEY] = "erro"
                log_container.error(f"❌ Erro ao gerar dashboard: {exc}")
                with st.expander("🔍 Traceback completo", expanded=True):
                    st.code(traceback.format_exc(), language=None)
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

            # Mostra log do console em expander
            log_txt = log_buf.getvalue()
            if log_txt.strip():
                with st.expander("📋 Log de execução", expanded=False):
                    st.code(log_txt, language=None)

    # ── Exibe o dashboard HTML ────────────────────────────────────
    if HTML_KEY in st.session_state and st.session_state.get(STATUS_KEY) == "ok":
        with col1:
            st.caption(
                f"🕐 Gerado em: {st.session_state.get(TIME_KEY, '')}  |  "
                f"📅 Período: {st.session_state.get(PERIOD_KEY,'').replace('_',' a ')}"
            )

        components.html(
            st.session_state[HTML_KEY],
            height=900,
            scrolling=True,
        )

    elif HTML_KEY not in st.session_state:
        st.info(
            "Clique em **🔄 Atualizar Dados** para buscar os dados da API Omie "
            "e gerar o dashboard."
        )
