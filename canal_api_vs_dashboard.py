"""
Verificação de vendas por Canal (PURAFOR / REAVITA / TERCEIRIZADO) via API Omie
Replica a lógica do PURAFOR_VENDAS.py:
  - Canal determinado por Marca/Família do catálogo de produtos
  - Marca == 'PURAFOR'        → PURAFOR
  - Marca == 'REAVITA'        → REAVITA
  - Família == 'terceirizado' → TERCEIRIZADO
  - else                      → SEM_CANAL
"""
import html
import math
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

import pandas as pd
import requests

# ── Configurações ────────────────────────────────────────────────────────────
CATALOGO_XLSX = r"Z:\codigos\Fabio\Produtos Purafor.xlsx"
URL_API       = "https://app.omie.com.br/api/v1/contador/xml/"
NS            = "http://www.portalfiscal.inf.br/nfe"

CFOP_VENDA = {"5101","6101","6107","6109","5102","6102","5108","6108","5110","6110"}

# Credenciais das duas empresas
EMPRESAS = {
    "PURAFOR FILIAL": {
        "app_key":    "2786968546362",
        "app_secret": "0552cb74d4e7dd891a7960a879615385",
    },
    "REAVITA NATURAIS": {
        "app_key":    "4325421674574",
        "app_secret": "781e07b7c339aa23e03da3a1f109e53b",
    },
}

ANO     = 2025
DATA_INI = f"01/01/{ANO}"
DATA_FIM = f"31/12/{ANO}"
REG_PAG  = 50  # paginação segura


# ── Carrega catálogo ─────────────────────────────────────────────────────────
def carregar_catalogo() -> dict:
    """Retorna {codigo_str: {Familia, Marca}}."""
    try:
        df = pd.read_excel(CATALOGO_XLSX, sheet_name=0, usecols=[2, 3, 19], header=0)
        df.columns = ["Codigo", "Familia", "Marca"]
        df["Codigo"] = df["Codigo"].astype(str).str.strip()
        df = df[df["Familia"].notna() | df["Marca"].notna()].copy()
        df["Familia"] = df["Familia"].fillna("").astype(str).str.strip()
        df["Marca"]   = df["Marca"].fillna("").astype(str).str.strip()
        print(f"  ✔ Catálogo: {len(df)} produtos com Família/Marca")
        return df.set_index("Codigo")[["Familia", "Marca"]].to_dict("index")
    except Exception as e:
        print(f"  [ERRO] Catálogo não carregado: {e}")
        return {}


# ── Determina canal pelo catálogo ────────────────────────────────────────────
def canal_do_produto(codigo: str, catalogo: dict) -> str:
    info  = catalogo.get(str(codigo).strip(), {})
    marca = (info.get("Marca", "") or "").upper().strip()
    fam   = (info.get("Familia", "") or "").lower().strip()
    if marca == "PURAFOR":
        return "PURAFOR"
    if marca == "REAVITA":
        return "REAVITA"
    if fam == "terceirizado":
        return "TERCEIRIZADO"
    return "SEM_CANAL"


# ── Busca NF-e da API (paginação) ────────────────────────────────────────────
def listar_documentos(empresa: str, creds: dict) -> list[dict]:
    """Retorna lista de {cXml, nNumero, dEmissao}."""
    docs = []
    pagina = 1
    tot_pag = None

    print(f"\n  [{empresa}] Buscando NF-e de {DATA_INI} a {DATA_FIM}...")
    while True:
        payload = {
            "call": "ListarDocumentos",
            "app_key": creds["app_key"],
            "app_secret": creds["app_secret"],
            "param": [{
                "nPagina":       pagina,
                "nRegPorPagina": REG_PAG,
                "cModelo":       "55",
                "dEmiInicial":   DATA_INI,
                "dEmiFinal":     DATA_FIM,
            }],
        }
        try:
            r = requests.post(URL_API, json=payload, timeout=60)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"    [ERRO] Página {pagina}: {e}")
            break

        if "faultstring" in data:
            print(f"    [FAULT] {data['faultstring']}")
            break

        if tot_pag is None:
            tot_reg = int(data.get("nTotRegistros", 0))
            tot_pag = max(1, math.ceil(tot_reg / REG_PAG))
            print(f"    Total de registros: {tot_reg} → {tot_pag} página(s)")

        for doc in data.get("documentosEncontrados", []):
            docs.append(doc)

        print(f"    Página {pagina}/{tot_pag} — acumulado: {len(docs)}", end="\r")
        if pagina >= tot_pag:
            break
        pagina += 1

    print(f"    [{empresa}] {len(docs)} documentos baixados                  ")
    return docs


# ── Parseia XML e extrai itens por canal ─────────────────────────────────────
def processar_docs(docs: list[dict], catalogo: dict) -> list[dict]:
    """Retorna lista de registros {ano, mes, canal, vliq, nf, cod_prod}."""
    registros = []
    erros = 0

    for doc in docs:
        xml_raw = doc.get("cXml", "")
        if not xml_raw:
            continue
        try:
            xml_limpo = html.unescape(xml_raw)
            root = ET.fromstring(xml_limpo)
        except Exception as e:
            erros += 1
            continue

        # Navega até infNFe
        nfe = root.find(f"{{{NS}}}NFe")
        if nfe is None:
            nfe = root
        infnfe = nfe.find(f"{{{NS}}}infNFe")
        if infnfe is None:
            continue

        # Data de emissão
        ide = infnfe.find(f"{{{NS}}}ide")
        dh_emi = ide.findtext(f"{{{NS}}}dhEmi", "") if ide is not None else ""
        try:
            dt = datetime.fromisoformat(dh_emi[:19])
        except Exception:
            continue

        if dt.year != ANO:
            continue

        ano = dt.year
        mes = dt.month
        num_nf = ide.findtext(f"{{{NS}}}nNF", "") if ide is not None else ""

        # Itens
        for det in infnfe.findall(f"{{{NS}}}det"):
            prod = det.find(f"{{{NS}}}prod")
            if prod is None:
                continue
            cfop = prod.findtext(f"{{{NS}}}CFOP", "")
            if cfop not in CFOP_VENDA:
                continue

            cod_prod = prod.findtext(f"{{{NS}}}cProd", "")
            try:
                v_bruto = float(prod.findtext(f"{{{NS}}}vProd", "0"))
            except Exception:
                v_bruto = 0.0
            try:
                v_desc = float(prod.findtext(f"{{{NS}}}vDesc", "0"))
            except Exception:
                v_desc = 0.0
            v_liq = v_bruto - v_desc

            canal = canal_do_produto(cod_prod, catalogo)

            registros.append({
                "ano":      ano,
                "mes":      mes,
                "nf":       num_nf,
                "cod_prod": cod_prod,
                "canal":    canal,
                "v_liq":    v_liq,
            })

    if erros:
        print(f"    [AVISO] {erros} documents com erro de parse XML")
    return registros


# ── Relatório ────────────────────────────────────────────────────────────────
def imprimir_relatorio(registros: list[dict]):
    canais = ["PURAFOR", "REAVITA", "TERCEIRIZADO", "SEM_CANAL"]

    # Totais por canal
    tot_canal: dict[str, float] = defaultdict(float)
    nfs_canal: dict[str, set]   = defaultdict(set)
    for r in registros:
        tot_canal[r["canal"]] += r["v_liq"]
        nfs_canal[r["canal"]].add(r["nf"])

    total_geral = sum(tot_canal.values())

    print("\n" + "="*60)
    print("  TOTAIS POR CANAL — 2025")
    print("="*60)
    print(f"  {'Canal':<20} {'Vlr Líquido':>18}  {'NFs':>6}  {'Share':>6}")
    print(f"  {'-'*20} {'-'*18}  {'-'*6}  {'-'*6}")
    for c in canais:
        v = tot_canal.get(c, 0.0)
        n = len(nfs_canal.get(c, set()))
        share = (v / total_geral * 100) if total_geral else 0
        print(f"  {c:<20} R$ {v:>15,.2f}  {n:>6}  {share:>5.1f}%")
    print(f"  {'TOTAL':<20} R$ {total_geral:>15,.2f}  {'':>6}")

    # Por mês × canal
    print("\n" + "="*60)
    print("  DETALHE MENSAL POR CANAL — 2025")
    print("="*60)
    mes_canal: dict[tuple, float] = defaultdict(float)
    for r in registros:
        mes_canal[(r["mes"], r["canal"])] += r["v_liq"]

    meses_nomes = ["Jan","Fev","Mar","Abr","Mai","Jun",
                   "Jul","Ago","Set","Out","Nov","Dez"]

    hdr = f"  {'Mês':<6}"
    for c in canais:
        hdr += f"  {c:>18}"
    hdr += f"  {'TOTAL':>18}"
    print(hdr)
    print("  " + "-"*90)

    tot_mes_geral = 0.0
    tot_cols: dict[str, float] = defaultdict(float)
    for m in range(1, 13):
        row_total = sum(mes_canal.get((m, c), 0.0) for c in canais)
        if row_total == 0:
            continue
        tot_mes_geral += row_total
        linha = f"  {meses_nomes[m-1]:<6}"
        for c in canais:
            v = mes_canal.get((m, c), 0.0)
            tot_cols[c] += v
            linha += f"  R$ {v:>14,.2f}"
        linha += f"  R$ {row_total:>14,.2f}"
        print(linha)

    # Linha de totais
    print("  " + "-"*90)
    linha_tot = f"  {'TOTAL':<6}"
    for c in canais:
        linha_tot += f"  R$ {tot_cols[c]:>14,.2f}"
    linha_tot += f"  R$ {tot_mes_geral:>14,.2f}"
    print(linha_tot)

    # Produtos sem canal (para diagnóstico)
    sem_canal = [r for r in registros if r["canal"] == "SEM_CANAL"]
    if sem_canal:
        top_sem = defaultdict(float)
        for r in sem_canal:
            top_sem[r["cod_prod"]] += r["v_liq"]
        top10 = sorted(top_sem.items(), key=lambda x: -x[1])[:15]
        print(f"\n  Produtos SEM_CANAL — top 15 por valor (total: R$ {tot_canal['SEM_CANAL']:,.2f})")
        print(f"  {'Código':<20}  {'Vlr Líquido':>18}")
        print(f"  {'-'*20}  {'-'*18}")
        for cod, v in top10:
            print(f"  {cod:<20}  R$ {v:>14,.2f}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  CANAL BREAKDOWN — API OMIE vs Dashboard")
    print("=" * 60)

    print("\nCarregando catálogo de produtos...")
    catalogo = carregar_catalogo()
    if not catalogo:
        print("[ERRO] Catálogo vazio — impossível categorizar canais!")
        return

    todos_registros = []

    for empresa, creds in EMPRESAS.items():
        docs = listar_documentos(empresa, creds)
        if not docs:
            print(f"  [AVISO] Nenhum documento retornado para {empresa}")
            continue

        print(f"  Parseando XMLs de {empresa}...")
        regs = processar_docs(docs, catalogo)
        print(f"  ✔ {len(regs)} itens de venda extraídos de {empresa}")
        todos_registros.extend(regs)

    if not todos_registros:
        print("[ERRO] Nenhum registro processado!")
        return

    print(f"\n  Total geral de itens de venda: {len(todos_registros)}")
    imprimir_relatorio(todos_registros)


if __name__ == "__main__":
    main()
