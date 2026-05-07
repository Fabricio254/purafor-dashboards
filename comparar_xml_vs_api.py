"""
COMPARAÇÃO: XMLs da pasta de rede  vs  XMLs baixados via API
Usa exatamente a mesma lógica de extração do PURAFOR_VENDAS.py
- Filtra ano 2025
- Filtra CFOPs de venda
- Soma vProd - vDesc = vLiquido por mês
"""
import os
import xml.etree.ElementTree as ET
import html
from datetime import datetime
from collections import defaultdict
import requests

# ── Configurações ──────────────────────────────────────
PASTA_XML = r"Z:\codigos\Fabio\XML"
PURAFOR   = {'app_key': '2786968546362', 'app_secret': '0552cb74d4e7dd891a7960a879615385'}
URL_API   = 'https://app.omie.com.br/api/v1/contador/xml/'
NS        = "http://www.portalfiscal.inf.br/nfe"

CFOP_VENDA = {
    "5101", "6101", "6107", "6109",
    "5102", "6102", "5108", "6108",
    "5110", "6110",
}

MESES = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

# ── Parser de XML (string ou arquivo) ─────────────────
def parsear_xml(xml_string_ou_arvore, origem=""):
    """
    Recebe string XML ou ET.ElementTree.
    Retorna lista de (mes, vlr_liquido) para itens de venda de 2025.
    """
    try:
        if isinstance(xml_string_ou_arvore, str):
            # A API retorna o XML com HTML entities (&lt; &gt; &quot;) - precisa desescapar
            xml_limpo = html.unescape(xml_string_ou_arvore)
            root = ET.fromstring(xml_limpo)
        else:
            root = xml_string_ou_arvore.getroot()

        nfe = root.find(f"{{{NS}}}NFe")
        if nfe is None:
            nfe = root

        infnfe = nfe.find(f"{{{NS}}}infNFe")
        if infnfe is None:
            return []

        ide = infnfe.find(f"{{{NS}}}ide")
        dh_emi = ide.findtext(f"{{{NS}}}dhEmi", "")
        try:
            data = datetime.fromisoformat(dh_emi[:19])
        except Exception:
            return []

        if data.year != 2025:
            return []

        mes = data.month
        itens = []
        for det in infnfe.findall(f"{{{NS}}}det"):
            prod = det.find(f"{{{NS}}}prod")
            if prod is None:
                continue
            cfop = prod.findtext(f"{{{NS}}}CFOP", "")
            if cfop not in CFOP_VENDA:
                continue
            try:
                v_bruto = float(prod.findtext(f"{{{NS}}}vProd", "0"))
            except Exception:
                v_bruto = 0.0
            try:
                v_desc = float(prod.findtext(f"{{{NS}}}vDesc", "0"))
            except Exception:
                v_desc = 0.0
            itens.append((mes, v_bruto - v_desc))
        return itens

    except Exception as e:
        print(f"  [AVISO] Erro ao parsear XML ({origem}): {e}")
        return []


# ══════════════════════════════════════════════════════
# FONTE 1: Pasta de rede
# ══════════════════════════════════════════════════════
print("=" * 65)
print("  FONTE 1: XMLs da pasta de rede")
print(f"  {PASTA_XML}")
print("=" * 65)

por_mes_pasta  = defaultdict(float)
nfs_pasta      = 0
itens_pasta    = 0
arqs_ignorados = 0

arquivos = sorted(f for f in os.listdir(PASTA_XML)
                  if f.lower().endswith(".xml") and "procEventoNFe" not in f)
print(f"  Total arquivos XML: {len(arquivos)}")

for nome in arquivos:
    caminho = os.path.join(PASTA_XML, nome)
    try:
        tree = ET.parse(caminho)
        itens = parsear_xml(tree, nome)
        if itens:
            nfs_pasta += 1
            for mes, val in itens:
                por_mes_pasta[mes] += val
                itens_pasta += 1
        else:
            arqs_ignorados += 1
    except Exception as e:
        print(f"  [AVISO] Erro ao ler {nome}: {e}")
        arqs_ignorados += 1

# ══════════════════════════════════════════════════════
# FONTE 2: API Omie (contador/xml/)
# ══════════════════════════════════════════════════════
print()
print("=" * 65)
print("  FONTE 2: XMLs via API Omie (contador/xml/)")
print("=" * 65)

por_mes_api = defaultdict(float)
nfs_api     = 0
itens_api   = 0
pag         = 1
tot_pag     = 1
REG_PAG     = 50  # maximo seguro para nao estourar timeout

while pag <= tot_pag:
    pl = {
        'call': 'ListarDocumentos',
        'app_key': PURAFOR['app_key'],
        'app_secret': PURAFOR['app_secret'],
        'param': [{
            'nPagina': pag,
            'nRegPorPagina': REG_PAG,
            'cModelo': '55',
            'dEmiInicial': '01/01/2025',
            'dEmiFinal': '31/12/2025',
        }]
    }
    r = requests.post(URL_API, json=pl, timeout=60).json()

    if 'faultstring' in r:
        print(f"  ERRO API pag {pag}: {r['faultstring']}")
        break

    if pag == 1:
        tot_reg = r.get('nTotRegistros', 0)
        tot_pag = -(-tot_reg // REG_PAG)  # ceil division
        print(f"  Total NF-e 2025 na API: {tot_reg}  ({tot_pag} paginas de {REG_PAG})")

    docs = r.get('documentosEncontrados', [])
    for d in docs:
        xml_str = d.get('cXml', '')
        if not xml_str:
            continue
        itens = parsear_xml(xml_str, f"NF {d.get('nNumero','?')}")
        if itens:
            nfs_api += 1
            for mes, val in itens:
                por_mes_api[mes] += val
                itens_api += 1

    if pag % 10 == 0 or pag == tot_pag:
        print(f"  ...processando pagina {pag}/{tot_pag}")
    pag += 1

# ══════════════════════════════════════════════════════
# COMPARAÇÃO
# ══════════════════════════════════════════════════════
print()
print("=" * 65)
print("  COMPARAÇÃO: Pasta XML  vs  API Omie  (2025)")
print("=" * 65)
print(f"{'Mes':<6} {'Pasta (R$)':>15} {'API (R$)':>15} {'Dif (R$)':>15} {'Dif %':>7}")
print("-" * 62)

meses_uniao = sorted(set(por_mes_pasta.keys()) | set(por_mes_api.keys()))
total_pasta = 0.0
total_api   = 0.0

for m in meses_uniao:
    vp = por_mes_pasta.get(m, 0.0)
    va = por_mes_api.get(m, 0.0)
    dif = va - vp
    pct = (dif / vp * 100) if vp else 0
    total_pasta += vp
    total_api   += va
    ok = "OK" if abs(pct) < 0.5 else "<<< DIFERENÇA!"
    print(f"{MESES[m-1]:<6} {vp:>15,.2f} {va:>15,.2f} {dif:>+15,.2f} {pct:>6.2f}%  {ok}")

print("-" * 62)
dif_total = total_api - total_pasta
pct_total = (dif_total / total_pasta * 100) if total_pasta else 0
print(f"{'TOTAL':<6} {total_pasta:>15,.2f} {total_api:>15,.2f} {dif_total:>+15,.2f} {pct_total:>6.2f}%")
print()
print(f"  NFs com venda 2025 (pasta) : {nfs_pasta}")
print(f"  NFs com venda 2025 (API)   : {nfs_api}")
print(f"  Arquivos ignorados (pasta) : {arqs_ignorados}  (sem 2025 ou sem CFOP venda)")
print("=" * 65)
