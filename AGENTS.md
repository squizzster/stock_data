# Agent Bootloader

This repository is an Executable Context workspace.

Do not infer stock-universe workflows from prose first. Treat the interface as
the curriculum: discover the current command surface, schemas, effects, repair
paths, and next actions through `xctx`.

Start here:

```bash
./stock_universe.cli xctx doctor
./stock_universe.cli xctx tree
```

After that, follow the returned schemas, examples, repair envelopes, recipes,
and `next_actions`.

Run only concrete commands from recipe `command` fields or schema
`argv`/`source_checkout_argv` fields. Treat `command.name`,
`logical_command`, and `stock-universe ...` names as logical identifiers;
never prepend `./stock_universe.cli` to them.

If a `next_actions` entry only exposes a logical `command.name`, resolve it
through `xctx schema` or `xctx compose` before running anything. Do not infer
flag spelling, wrapper paths, or mutation commands from a logical name.

If a bare entrypoint or namespace reports a missing command, treat that as
discovery pressure: inspect help or return to `xctx doctor` and `xctx tree`
before choosing the next concrete command.
