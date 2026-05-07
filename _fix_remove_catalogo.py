# -*- coding: utf-8 -*-
"""Remove unused PRODUTOS_OMIE (2.4 MB catalog never referenced in JS)."""

path = r"Z:\codigos\Fabio\PURAFOR_VENDAS.py"
content = open(path, "r", encoding="utf-8").read()

# Remove the comment block + declaration (4 lines → 0)
old = (
    "// Catálogo completo de produtos Omie (indexado pelo código da NF-e)\n"
    "// Campos: codigo, descricao, descricao_familia, marca, ean, ncm, unidade,\n"
    "//         valor_unitario, peso_bruto, peso_liq, inativo, tipoItem, imagens, etc.\n"
    "const PRODUTOS_OMIE = {jv(produtos_omie or {{}})};\n"
)

c = content.count(old)
print(f"Match: {c}")
assert c == 1, f"Expected 1, got {c}"
content = content.replace(old, "")

open(path, "w", encoding="utf-8").write(content)
print("Removed PRODUTOS_OMIE. File saved.")

v = open(path, "r", encoding="utf-8").read()
print(f"PRODUTOS_OMIE gone: {'PRODUTOS_OMIE' not in v}")
