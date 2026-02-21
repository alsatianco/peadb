# Command Compatibility Test Template

For each command implementation, add:
- Happy-path behavior diff test.
- Wrong arity diff test.
- Syntax/option error precedence diff test.
- Wrongtype diff test (when multiple types exist).

Suggested files:
- `tests/diff/<group>/<command>_happy.json`
- `tests/diff/<group>/<command>_errors.json`
