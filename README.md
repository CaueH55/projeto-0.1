Projeto Python de Exemplo

Projeto mínimo para subir no GitHub com package, CLI, testes e CI.

Funcionalidades:
- Pacote `myproject` com função `greet()` em português.
- CLI simples via `python -m myproject.cli`.
- Testes com `pytest`.
- Workflow de GitHub Actions para rodar os testes.

Como usar:

1. Criar e ativar um virtualenv (opcional):

```bash
python -m venv .venv
source .venv/bin/activate  # Unix/macOS
.venv\\Scripts\\activate     # Windows
```

2. Instalar dependências:

```bash
pip install -r requirements.txt
```

3. Executar CLI:

```bash
python -m myproject.cli Caue
```

4. Rodar testes:

```bash
pytest -q
```
