# M8 Module Certification Status

Date: 2026-02-17

## Implemented certification smoke set

- `tests/integration/test_m8_module_load.py`
- `tests/integration/test_m8_module_key_api.py`
- `tests/integration/test_m8_module_command_api.py`
- `tests/integration/test_m8_module_certification.py` (suite runner)

The current certification gate verifies:
- unmodified shared library load/unload
- module key open/read/write API subset
- module command registration and reply building

## Remaining gap

Popular Redis ecosystem modules (RedisJSON, RediSearch, RedisTimeSeries,
RedisBloom) are not yet certified in this milestone and remain tracked as
follow-up compatibility work.
