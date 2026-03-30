# Contributing to OpenTrade

The best way to contribute is to **add your country's trade data**.

## Adding a new source (30–60 min)

```bash
# 1. Scaffold
python runner.py add CC/SourceName    # e.g. IN/DGFT, JP/Customs, BR/MDIC

# 2. Implement bootstrap.py + fill config.yaml
# 3. Add sample records to sample/
# 4. Test
python runner.py sample CC/SourceName
python runner.py validate CC/SourceName
python runner.py run CC/SourceName

# 5. Open a PR
```

Full guide: [docs/adding-a-source.md](docs/adding-a-source.md)

## Other contributions

- 🐛 Bug fixes
- 📖 Documentation improvements
- 🧪 Tests for existing sources
- 🌐 Translations

## Code style

- Python 3.12+
- Type hints everywhere
- Keep bootstrap.py under 200 lines
- Every source must have 10+ sample records

## Questions?

Open an issue or start a Discussion.
