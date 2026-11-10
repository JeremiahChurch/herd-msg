# Contributing

Issues and pull requests are welcome. This is a small single-maintainer project,
so please open an issue to discuss larger changes before writing them.

## Build and test

You need Go 1.26.4 or newer. If your system Go is older, install a pinned toolchain
with `go install golang.org/dl/go1.26.4@latest && go1.26.4 download`; the Makefile
picks up `~/sdk/go1.26.4` automatically, or pass `make GO=/path/to/go`.

```sh
make build    # dist/herd-msg
make vet
make test     # unit and integration tests; no harnesses or network needed
go test -race ./internal/broker/... ./internal/store/... ./internal/drivers/...   # needs CGO_ENABLED=1
```

CI runs vet, the full test suite and the race tests on every push and pull request.

## Live tests

Tests tagged `live` drive real harnesses in disposable homes and are never run in
CI. They need `tmux`, `herdr`, and authenticated Claude Code and Codex CLIs, and
they spend a small amount of model usage. The Muse test is separate and needs
`HERD_MSG_LIVE_MUSE=1`:

```sh
HERD_MSG_LIVE=1 make test-live
HERD_MSG_LIVE_MACHINE='<saved herdr machine label>' make test-live   # cross-machine
HERD_MSG_LIVE=1 HERD_MSG_LIVE_MUSE=1 go test -tags live ./internal/e2e -run '^TestMuseNativeDelivery$' -v   # Muse
```

They create temporary homes and never inject into an existing session. The Muse
test also needs Muse Code 1.4.14 and a `herd-spawn`-compatible launcher on PATH:
a command that accepts `-b -a muse -l <label> <dir> <prompt>`, runs `$HERD_MUSE_CMD`
in a new herdr workspace, and prints `workspace <id> ... pane=<pane-id>`.

## Harness protocol changes

Delivery depends on harness behaviour that is not a stable public API. When a
harness release changes it, record the experiment and its result in
[docs/protocol-notes.md](docs/protocol-notes.md) alongside the code change, and
never report delivery without proof of acceptance from the recipient's history.

## Pull requests

- Keep changes focused, with tests for new behaviour.
- Run `make vet test` before pushing.
- Update the README when you change a command, config key or default.
