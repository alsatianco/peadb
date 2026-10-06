# Per-suite skipfiles for native-mode upstream TCL runs

One file per suite (`unit_type_string.skip` for `unit/type/string`), one test-name
pattern per line, passed to `runtest --skipfile`. Every line here is a tracked
compatibility debt: adding a line requires a matching `compat/delta.md` update
(enforced by `scripts/ci/check_suite_ratchet.py`); removing lines is always welcome.

A skipfile skips tests by name, so it cannot skip code that sits outside any `test` block; when such code would abort a whole suite on PeaDB, `scripts/redis/suite-native.patch` edits it instead (applied by `scripts/redis/run_redis_tests.sh` in native mode). Each hunk there carries a `# PeaDB:` comment naming its `compat/delta.md` item, and the file stays as small as possible.
