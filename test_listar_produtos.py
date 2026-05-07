import requests, json

creds = {'app_key': '2786968546362', 'app_secret': '0552cb74d4e7dd891a7960a879615385'}
URL = 'https://app.omie.com.br/api/v1/geral/produtos/'

# Tenta variações de parâmetros até achar a que funciona
testes = [
    {'pagina': 1, 'registros_por_pagina': 3},
    {'pagina': 1, 'registros_por_pagina': 3, 'filtrar_apenas_omiepdv': 'N'},
    {'pagina': 1, 'registros_por_pagina': 3, 'inativo': 'N'},
    {'pagina': 1, 'registros_por_pagina': 3, 'exibir_caracteristicas': 'S'},
    {'nPagina': 1, 'nRegPorPagina': 3},
]

for i, param in enumerate(testes, 1):
    payload = {
        'call': 'ListarProdutos',
        'app_key': creds['app_key'],
        'app_secret': creds['app_secret'],
        'param': [param]
    }
    r = requests.post(URL, json=payload, timeout=30)
    data = r.json()
    total = data.get('total_de_registros', 'N/A')
    fault = data.get('faultstring', '')
    print(f"Teste {i} | param: {param}")
    print(f"  total_de_registros: {total}  |  fault: {fault[:80] if fault else '-'}")
    prods = data.get('produto_servico_cadastro', [])
    if prods:
        p = prods[0]
        print(f"  -> codigo: {p.get('codigo')}  familia: {p.get('familia_produto')}  descricao: {str(p.get('descricao',''))[:40]}")
        print("  CAMPOS PREENCHIDOS:")
        for k, v in p.items():
            if v not in ('', None, 0, 0.0, [], {}, 'N'):
                print(f"     {k}: {repr(v)[:80]}")
        break
    print()

if False:

for url in urls:
    payload = {
        'call': 'ListarProdutos',
        'app_key': creds['app_key'],
        'app_secret': creds['app_secret'],
        'param': [{'pagina': 1, 'registros_por_pagina': 2, 'apenas_importado_api': 'N'}]
    }
    r = requests.post(url, json=payload, timeout=30)
    data = r.json()
    total = data.get('total_de_registros', 'N/A')
    fault = data.get('faultstring', '')
    print(f"\nURL: {url}")
    print(f"  Total registros: {total}")
    if fault:
        print(f"  FAULT: {fault}")

    prods = data.get('produto_servico_cadastro', [])
    for p in prods:
        print(f"\n  === Produto: {p.get('codigo')} — {p.get('descricao','')[:40]} ===")
        CAMPOS_INTERESSE = [
            'codigo', 'descricao', 'familia_produto', 'marca',
            'ncm', 'unidade', 'caracteristicas', 'obs_internas',
            'info', 'recomendacoes', 'tags',
        ]
        for k in CAMPOS_INTERESSE:
            v = p.get(k)
            if v not in ('', None, 0, 0.0, [], {}):
                print(f"    {k}: {repr(v)}")

        # Mostra também qualquer campo que contenha "familia" ou "marca" (case insensitive)
        for k, v in p.items():
            if ('familia' in k.lower() or 'marca' in k.lower()) and k not in CAMPOS_INTERESSE:
                print(f"    [extra] {k}: {repr(v)}")

# Testa ConsultarProduto com código real
print("\n\n=== ConsultarProduto (CXAMORA) ===")
payload_c = {
    'call': 'ConsultarProduto',
    'app_key': creds['app_key'],
    'app_secret': creds['app_secret'],
    'param': [{'codigo': 'CXAMORA'}]
}
r = requests.post('https://app.omie.com.br/api/v1/geral/produtos/', json=payload_c, timeout=30)
data = r.json()
if 'faultstring' in data:
    print(f"  Fault: {data['faultstring']}")
else:
    for k, v in data.items():
        if v not in ('', None, 0, 0.0, [], {}):
            print(f"  {k}: {repr(v)}")
