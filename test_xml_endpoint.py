# ENCONTRADO! contador/xml/ -> ListarDocumentos -> campo cXml tem o XML completo
import requests, json

PURAFOR = {'app_key': '2786968546362', 'app_secret': '0552cb74d4e7dd891a7960a879615385'}
REAVITA = {'app_key': '4325421674574', 'app_secret': '781e07b7c339aa23e03da3a1f109e53b'}
URL = 'https://app.omie.com.br/api/v1/contador/xml/'

# Testar REAVITA com diferentes modelos e sem filtro de data
print("=== REAVITA - testando modelos ===")
for modelo in ['55', '65', '57', '']:
    pl = {
        'call': 'ListarDocumentos',
        'app_key': REAVITA['app_key'],
        'app_secret': REAVITA['app_secret'],
        'param': [{'nPagina': 1, 'nRegPorPagina': 3, 'cModelo': modelo}]
    }
    r = requests.post(URL, json=pl, timeout=20).json()
    if 'faultstring' in r:
        print(f"  modelo={modelo!r} -> ERRO: {r['faultstring'][:70]}")
    else:
        tot = r.get('nTotRegistros', 0)
        docs = r.get('documentosEncontrados', [])
        print(f"  modelo={modelo!r} -> Total: {tot}, primeiros: {len(docs)}")
        for d in docs:
            print(f"    NF {d.get('nNumero','?')} | {d.get('dEmissao','?')} | val {d.get('nValor','?')}")




