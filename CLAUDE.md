# EternalTeleSales — Copilot Instructions

- Docstrings must use **reStructuredText (reST)** format: `:param name:`, `:type name:`, `:returns:`, `:rtype:`, `:raises ExceptionType:`.
- For DataFrame assertions in tests, use `chispa.dataframe_comparer.assert_df_equality`. Never use plain `==` to compare DataFrames.
- Never use `print()` in `src/`. All logging must go through `get_logger(__name__)` imported from `eternalsalesdata.utils`.
- Install dependencies with `uv sync --extra dev`. Do not use `pip install`.