"""
App principal — Dashboards PURAFOR
Executa via: streamlit run app.py
"""
import os
import sys
import tempfile
from datetime import datetime, date, timezone, timedelta

_BRT = timezone(timedelta(hours=-3))

import streamlit as st
import streamlit.components.v1 as components

# ─── Configuração da página ───────────────────────────────────────
st.set_page_config(
    page_title="Dashboards PURAFOR",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Tela de login ────────────────────────────────────────────────
_SENHA_CORRETA = "zampa254"

if not st.session_state.get("_autenticado"):
    _logo_login = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo_purafor.jpg")
    col_c, col_f, col_r = st.columns([1, 1, 1])
    with col_f:
        st.markdown("<div style='margin-top:60px'></div>", unsafe_allow_html=True)
        if os.path.exists(_logo_login):
            st.image(_logo_login, use_container_width=True)
        st.markdown(
            "<h2 style='text-align:center;margin-top:12px'>🔒 Área Restrita — PURAFOR</h2>",
            unsafe_allow_html=True,
        )
        with st.form("_login_form"):
            senha = st.text_input("Senha de acesso", type="password")
            submitted = st.form_submit_button("Entrar", use_container_width=True, type="primary")
        if submitted:
            if senha == _SENHA_CORRETA:
                st.session_state["_autenticado"] = True
                st.rerun()
            else:
                st.error("Senha incorreta.")
    st.stop()

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
    _logo_sb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo_purafor.jpg")
    if os.path.exists(_logo_sb):
        _cl, _cr = st.columns([1, 1])
        with _cl:
            st.image(_logo_sb, use_container_width=True)
    st.markdown("## 📊 Dashboards PURAFOR")
    st.markdown("---")

    # ── Seção: Vendas ──────────────────────────────────────────
    st.markdown("##### 🛒 Vendas")
    opcoes_vendas = {
        "📊 Dashboard de Vendas por Canais": "purafor_vendas",
        # Adicione futuros dashboards de vendas aqui
    }

    # ── Seção: (outras seções futuras) ────────────────────────────
    # st.markdown("##### 📦 Estoque")
    # opcoes_estoque = { ... }

    todas_opcoes = {**opcoes_vendas}

    escolha = st.radio(
        "Selecione o dashboard:",
        list(todas_opcoes.keys()),
        label_visibility="collapsed",
    )
    pagina = todas_opcoes[escolha]

    # ── Período de busca ──────────────────────────────────────────
    st.markdown("### 📅 Período")
    _hoje    = date.today()
    _ini_def = date(_hoje.year - 1, 1, 1)

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
    st.caption(f"Última execução: {datetime.now(_BRT).strftime('%d/%m/%Y %H:%M')}")

# ═══════════════════════════════════════════════════════════════════
# PÁGINA: Dashboard de Vendas por Canais
# ═══════════════════════════════════════════════════════════════════
if pagina == "purafor_vendas":

    st.title("📊 Dashboard de Vendas por Canais — PURAFOR")

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
        import io, contextlib, threading, queue, traceback as _tb

        _prog_bar     = st.progress(0, text="⏳ Iniciando...")
        _prog_status  = st.empty()
        log_container = st.empty()
        log_buf       = io.StringIO()

        # Importa o módulo
        _base = os.path.dirname(os.path.abspath(__file__))
        if _base not in sys.path:
            sys.path.insert(0, _base)

        if "PURAFOR_VENDAS" in sys.modules:
            pv = sys.modules["PURAFOR_VENDAS"]
            pv.OMIE_APP_KEY    = os.getenv("OMIE_APP_KEY",    pv.OMIE_APP_KEY)
            pv.OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET", pv.OMIE_APP_SECRET)
        else:
            import PURAFOR_VENDAS as pv

        # Fila de progresso: itens são (pct, msg) ou (None, exc) p/ erro
        _q: queue.Queue = queue.Queue()

        def _cb(pct: float, msg: str):
            _q.put((float(pct), str(msg)))

        pv._progresso = _cb

        # Gera HTML em arquivo temporário
        with tempfile.NamedTemporaryFile(
            suffix=".html", delete=False, mode="w", encoding="utf-8"
        ) as tmp:
            tmp_path = tmp.name

        # Resultado da thread: [html_content | None, exc | None]
        _result: list = [None, None]

        def _worker():
            try:
                with contextlib.redirect_stdout(log_buf):
                    _result[0] = pv.main(
                        saida_html=tmp_path,
                        saida_excel=None,
                        data_ini=_data_ini_str,
                        data_fim=_data_fim_str,
                    )
            except Exception as exc:
                _result[1] = exc
            finally:
                pv._progresso = None
                _q.put(None)   # sentinela: fim da execução

        t = threading.Thread(target=_worker, daemon=True)
        t.start()

        # Loop principal: lê a fila e atualiza a UI em tempo real
        import time as _time
        while True:
            try:
                item = _q.get(timeout=0.15)
            except queue.Empty:
                continue
            if item is None:          # sentinela → thread encerrou
                break
            pct, msg = item
            _prog_bar.progress(min(pct, 1.0), text=f"⏳ {msg}")
            _prog_status.caption(msg)

        t.join()

        # Processa resultado
        if _result[1] is not None:
            st.session_state[STATUS_KEY] = "erro"
            log_container.error(f"❌ Erro ao gerar dashboard: {_result[1]}")
            with st.expander("🔍 Traceback completo", expanded=True):
                st.code(_tb.format_exception(type(_result[1]), _result[1],
                                              _result[1].__traceback__),
                        language=None)
        elif _result[0]:
            st.session_state[HTML_KEY]   = _result[0]
            st.session_state[STATUS_KEY] = "ok"
            st.session_state[PERIOD_KEY] = _period_id
            st.session_state[TIME_KEY]   = datetime.now(_BRT).strftime(
                "%d/%m/%Y às %H:%M:%S"
            )
            _prog_bar.progress(1.0, text="✅ Concluído!")
            _prog_status.empty()
            log_container.success("✅ Dashboard gerado com sucesso!")
        else:
            st.session_state[STATUS_KEY] = "erro"
            log_container.error(
                "❌ Nenhum dado encontrado. Verifique as credenciais e o período."
            )

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
