# herd-msg

Durable mailbox messaging between Claude Code, Codex, and opted-in Muse sessions in
[herdr](https://herdr.dev), on one machine or across SSH machines, without typing into panes.

## Why

When several coding agents run side by side, they need to hand work to each other:
a reviewer reports to a builder, a coordinator fans out tasks and collects results.
Typing into another agent's terminal is fragile. It can clobber a half-written
prompt, land mid-turn, or vanish when a session restarts. herd-msg gives each
session a mailbox instead. A small broker stores every message in SQLite and
delivers it through each harness's own input channel (the Codex app-server
socket, a Claude Code plugin receiver, or Muse's native session messages). It
then checks the recipient's recorded history to prove the input was accepted.

- Agents send with `herd-msg send <session> -- '<body>'` and answer with `herd-msg reply <id>`.
- Mail survives broker restarts, reboots and closed terminals; it waits for an
  offline recipient for a grace period, then expires.
- Delivery never types into a pane and preserves unsent composer drafts.
- Linked machines exchange mail over your existing SSH access. Cross-machine mail
  is held for operator release unless you grant auto-run on that link.
- Hop limits, pair rate limits, a kill switch and per-session opt-out bound
  runaway agent-to-agent loops.

## Status

This is an early release built for one operator running agents on a few Linux
machines. It works daily for its author, but expect rough edges. The delivery
paths were verified against specific harness versions (listed under
[Install and remove](#install-and-remove)). Codex older than 0.157.1 and Muse
versions other than the verified one queue mail. Newer Codex and Claude Code
releases are assumed compatible; every delivery still needs proof of acceptance
from the recipient's history, so a protocol change parks mail instead of
reporting false success. Read [Trust and troubleshooting](#trust-and-troubleshooting) before
linking machines or enabling auto-run.

## Quick start

Requirements: Linux amd64 with a systemd user manager, herdr 0.9.3 or newer, Go
1.26.4 or newer to build, and Claude Code 2.1.311 or newer (setup installs its
receiver plugin and currently stops if `claude` is missing). Codex 0.157.1 or newer,
attached to its app-server daemon, is optional; Muse Code 1.4.14 is optional and
needs the separate opt-in described under [Reference](#reference).

```sh
git clone https://github.com/JeremiahChurch/herd-msg && cd herd-msg
make install      # builds, installs ~/.local/bin/herd-msg, the Claude plugin,
                  # the Codex skill, the herdr viewer and herd-msg.service
herd-msg doctor   # checks the service, harness versions and sockets
```

Restart open Claude Code sessions so they load the receiver. Start Codex sessions
attached to the shared daemon (`codex --remote unix://`) so they can receive mail.
Then, from any agent session:

```sh
herd-msg peers --json                       # find a session ID or name
herd-msg send reviewer -- 'PR 12 is ready for review'
```

The rest of this README is the operator reference.

## Reference

The mailbox uses a private SQLite database, a Unix socket, and JSON configuration.
`HERD_MSG_HOME` overrides the mailbox home and also places configuration there.
The default data home is `~/.local/share/herd-msg`; default configuration is
`~/.config/herd-msg/config.json`.

Build with `make build`. Run `herd-msg broker` to serve the mailbox. Session
registration and protocol drivers provide recipients; a broker with no driver
keeps accepted mail queued with an explanation.

Commands include `send`, `reply`, `inbox`, `read`, `log`, `show`, `status`,
`release`, `resolve`, `prune`, `allow`, `kill`, `config`, and `watch`.
Each accepts `--json`. Use `status --json` for the complete pending list and
`show <id> --json` for the body, attempts and state history. `watch --json`
emits successive complete status objects, including offline snapshots.

Quote message bodies, or use `--` before body text that contains separate flag tokens.
Flag words before `--` are command options. Use `--` for a body that is exactly an
option such as `--json`, even when that body is quoted:

```sh
herd-msg send reviewer -- 'see --json in the example'
herd-msg kill on --json
herd-msg config set hop_limit 5 --json
herd-msg config set links '{}' --json
herd-msg resolve <id> resend --confirm --json
```

Config changes replace only named settings under a lock. Removing a link from
`links` revokes it and marks its waiting mail dead. Shell callers are the
operator; harness environment variables identify agent callers. Agent identity
is advisory within the same OS account. Operator actions reject agent callers.
An uncertain send is parked for a bounded confirmation window. It then expires
without automatic resend if delivery remains unproven.
Resend can execute twice. `injected` means accepted by the harness, not read or
acted on. SIGKILL tests cover process crashes; they do not prove power-loss
survival or secure erasure of filesystem copies.

## Local harness delivery

Run `make install` as the operator to start the service and install the receiver.
The broker discovers Codex threads through the local app-server socket. For
disposable development sessions, `--plugin-dir claude-plugin/` loads the receiver
and its conversation binding hook with `herd-msg` on PATH.

Use `herd-msg peers --json` to find session IDs or names; it lists live and detached
peers, and `--all` adds offline and ended ones. From an attached Codex
session, `herd-msg name <name>` updates the daemon's thread name. Send with
`herd-msg send <recipient> "<body>"`, and reply with the command in the received
envelope. `herd-msg watch --json` streams complete status snapshots, including
queued and uncertain mail.

Codex server versions below 0.157.1 are unverified and leave mail queued. A
missing socket pauses Codex delivery. For a detached session, run `herd-msg doctor`
and use its pinned Codex resume command. Daemon-loaded threads remain reachable after a terminal
closes; use explicit IDs when names collide. Approval and user-input waits hold
mail until the recipient can proceed.

Delivery preserves unsent composer drafts. A successful socket write or turn
request does not prove acceptance. The broker checks stored user input and parks
uncertain results without automatically resending. Input may remain pending until
the recipient's current turn reaches a boundary. Wait for that boundary before
considering a confirmed resend, which can run the input twice.

Claude `/clear` changes conversation identity. New mail follows the new
registration; already accepted mail keeps its original recipient. Resume the
original conversation to deliver its queued mail, or prune its offline mailbox.
A send crossing the hook publication boundary remains uncertain until a known
original or successor transcript proves acceptance.

Run disposable live tests with `HERD_MSG_LIVE=1 make test-live`. They require
`tmux`, `herdr`, authenticated Claude and Codex CLIs, and no existing private
`herd-msg-test` server. Tests create temporary homes and use inexpensive models;
they never inject into an existing operator session.

After Codex `/clear`, both the old daemon thread and the blank replacement can
remain reachable under different IDs. Sending to the old ID still addresses the
old conversation; the verified live send did not appear in the replacement
terminal. Confirm the intended new conversation in `herd-msg peers` before using
its name. Accepted input alone does not prove a terminal is open.

## Mail cleanup and delivery pacing

Automatic cleanup runs on startup and every ten minutes. These JSON config keys
use seconds; explicit settings in an existing config keep their values.

| Key | Default | Meaning |
| --- | --- | --- |
| `lookup_window_seconds` | 3600 | Maximum time after an uncertain attempt to seek proof |
| `lookup_interval_seconds` | 10 | Initial receipt retry delay, doubling up to ten minutes |
| `sweep_interval_seconds` | 600 | Cleanup and WAL checkpoint cadence |
| `offline_grace_seconds` | 86400 | Grace for queued or held mail to offline or detached recipients |
| `session_retention_seconds` | 604800 | Remove ordinary offline sessions after seven days unseen and no pending mail |
| `retention_seconds` | 604800 | Keep terminal message bodies for seven days |
| `dedupe_seconds` | 2592000 | Keep replay protection for thirty days |

Seven days of bodies supports a weekly review and keeps about 7,000 messages at
1,000 messages per day, instead of 30,000. Replay protection remains thirty days.
Cleanup removes at most 100 message bodies, 100 ordinary sessions, and 100 old
dedupe entries per pass. Tombstones, successor chains and outstanding receiver
work remain protected. Detached discovery does not renew the grace period.
The mail grace begins after the later of creation and the recipient's last live
observation. Cleanup does not send a new notification to the sender.

Unconfirmed mail blocks the next automatic delivery to that recipient. Codex
waits for idle before delivering one queued message, preserving each message's
header and receipt instead of combining unrelated work into a digest. Active
turns never receive automatic `turn/steer`, including versions on which steering
has been verified. Approval and user-input waits continue to block delivery. Claude and Muse
working observations also hold mail until an idle or done observation.
JSON-RPC rejections record their error text as proven non-delivery; transport
failures after a write remain uncertain.

Codex receipt checks resume a saved cursor and read at most two pages per check.
Known older Codex protocols without pagination allow one history read per
attempt in each broker process, capped by the 32 MiB WebSocket frame limit.
Muse saves a file identity and offset with the attempt before sending, resumes scanning there,
and reads at most 4 MiB per check. Legacy attempts without offsets start a bounded
scan at the beginning. Rotated or truncated logs restart that scan. Incomplete pre-send records are
skipped through their newline. Partial new records continue across checks, with
a 16 MiB record limit and a 64 MiB driver cache limit. Neither a partial record
nor a missing receipt proves delivery. Receipt checks stop after
the window or when the recipient is offline with no live successor. Discovery
wakes delivery when an unavailable recipient returns.

The terminal state `expired` covers offline grace, TTL and unproven delivery.
`status --json` keeps schema 1 and adds `counts.expired`. Expired messages appear
in `recent`, retain their reason in `error`, and contribute neither to `pending`
nor session `unsent` nor the displayed stuck count. Consumers should label this
state as expired, without implying the input was never received. The full
message JSON also adds `lookup_checks` for backoff bookkeeping.

Receipt timer changes update only the message row. They do not rewrite unchanged
attempts or audit history. SQLite checkpoints automatically after 256 WAL pages
and limits a reusable journal to 16 MiB when readers permit; the slow sweep also
tries a nonwaiting truncate checkpoint. A reader holding a snapshot can defer
truncation, which is reported in health and retried at the next sweep.

## SSH links and peers

Run `herd-msg link '<saved herdr machine label>'` as the operator after installing
herd-msg on both machines. It reads saved routes from `herdr machine list --json`,
records an absolute executable path, exchanges a private link key, and tests both
directions. Addresses use the peer's declared machine name printed by `link`,
which can differ from its saved herdr label. Set the local name before mailbox
records exist with `herd-msg config set machine '"node-a"'`.

Reverse discovery checks enabled saved routes with a fresh challenge and an
HMAC-SHA256 proof of the link key. A route must prove the peer's declared name
before it receives an established credential. The same check protects relinking,
mail exchanges, doctor probes, and revocation retries, including previously working targets.
The first link gives a fresh key only to the operator-selected SSH route.
Both machines need binaries supporting `peer-route-proof`. An older peer leaves
reverse discovery passive with a `reverse_error`; established outbound operations
also fail closed. Upgrade both machines before resuming traffic. If one side has
lost its link record, unlink and relink to create a fresh key.

Mail from a linked machine waits held by default. On the receiving machine,
`herd-msg link '<declared machine name>' --auto-run` grants automatic
execution, and `--no-auto-run` removes that permission. These updates work without
network access, including on a passive peer. A saved label also works when that
label was recorded by a locally initiated link. Passive peers use the declared name. Existing held mail still needs
`release`. Auto-run delegates shell authority to that machine's agents. Mail can
contain hostile instructions; the envelope identifies another agent and cannot
grant user permission. Hop and pair limits bound automated reply loops.

SSH uses noninteractive authentication and strict host-key checking. It never
accepts unknown or changed keys. Verify a new host's fingerprint separately.
Link errors include commands for checking the saved target. Make the SSH agent
available to the broker's environment, including when started by systemd. Existing
SSH multiplexing remains enabled; a shared connection can delay noticing a host
key change until it reconnects. Authentication failures back off for five minutes.

Each reachable side pushes outbound mail and polls every 30 seconds. If only one
side can initiate SSH, that side also drains return mail, commits it locally and
then acknowledges it. The sender retains its outbox until a matching durable
receipt. Lost replies retry the same ID; mailbox dedupe prevents another insert.
This is independent of harness injection, whose uncertain results remain parked.
Permanent refusals become dead mail with the peer's reason. Temporary policy
refusals keep the outbox and leave unrelated recipients moving.

`herd-msg peers --json` includes cached remote sessions, their machine and cache
age. Unreachable observations and passive caches older than ten minutes display
offline. Names resolve to full session IDs when accepted. Duplicate names are
ambiguous, even if one is offline; use the intended ID and machine. A unique ID
prefix of at least 8 characters also resolves, and a name always wins over a prefix. Accepted mail
never follows a renamed or replaced session.

Run `herd-msg unlink '<linked name>'` to preview outbound and inbound counts.
Repeat with `--keep` or `--drop` and `--expect-unsent <total>`. Keep retains outbound
mail blocked. Drop cannot recall a row already in flight or committed remotely. Removing
trust moves inbound pending mail to dead on both sides. Relink and explicitly
resolve resend to recover it. A private revocation tombstone answers late contact;
outgoing revocation retries stop after the configured outbox lifetime. Credentials
and routes live in a separate 0600 file and never appear in config, status or peers.

The cross-machine live test runs with
`HERD_MSG_LIVE_MACHINE='<saved label>' make test-live`. It copies a static binary
into temporary homes, starts disposable brokers and harnesses, tests a real reply,
and verifies cleanup. Its explicit bootstrap path options apply to those temporary
links. No installed auto-run grant or account-level trust configuration is changed.

Idle receivers wait up to 25 seconds for their session to change. SessionStart hooks
interrupt that wait when a conversation binds to another session. New sends and
herdr pane events wake broker work; a periodic resync catches missed events.
Existing receiver processes need a rolling restart after installing a new binary
to use long polling.

## Stalled broker

The broker commits a small watchdog write every minute. After three minutes
without a successful probe it logs a goroutine dump, sends a herdr notification and exits, and
systemd restarts it. Accepted mail is durable across that restart. Sends refused during
a stall print a retry command with their `--id`. Read the dump with
`journalctl --user -u herd-msg`.

## Install and remove

Linux amd64, a running systemd user manager, herdr 0.9.3 or newer and Claude Code 2.1.311 or newer
are required; setup stops without `claude`. Attached Codex 0.157.1 or newer is
optional, but `doctor` always checks Codex and reports failures when it is absent. Build with Go 1.26.4 or newer.
Claude 2.1.311 is the oldest version with verified live delivery; older versions
need a disposable round trip before use.
The binary has no libc dependency. Distribution uses your existing SSH access.

```sh
make install
herd-msg doctor
scripts/deploy.sh '<saved herdr machine label>'
```

`make install` builds and runs setup as the operator. Setup installs the binary in
`~/.local/bin`, links the herdr viewer, registers the local Claude marketplace and
receiver plugin, installs the Codex skill, and enables `herd-msg.service` under
systemd. Restart Claude sessions to load the new receiver. The viewer's Open
Messages action has no key binding. `make dist` creates
`dist/herd-msg-linux-amd64`; deploy resolves the saved route at runtime and runs
setup remotely. Deployment reports installation success and prints status; it does
not certify delivery readiness. Run `doctor` manually to check readiness and get
repairs; failed health checks return exit 1.

Operator-run `doctor` opens the operator's shell startup to check Codex; broker probes and deployment do not.

Setup can be repeated. Preflight refuses a missing systemd user manager. A failure
restores replaced files, removes new registrations, and restores the old service.
Rollback failures name the repair and keep the error visible. Linger lets the user
service run after logout; setup tries to enable it without a password prompt and
prints the exact command if permission is needed. On WSL, systemd must be enabled
and started before setup. On systems with an ephemeral home or user-unit directory,
keep those paths persistent across boots and rerun doctor after reboot.

```sh
herd-msg uninstall
herd-msg uninstall --purge
```

Uninstall stops and disables the service, unlinks herdr, removes the Claude plugin
and its owned cache, removes the Codex skill, and removes the binary. By default,
mail, config and link credentials remain. Purge removes them, including queued mail.
Neither command revokes a peer's independent trust grant; unlink first when retiring
a machine. Linger, shared agent registries and their backups remain account-owned.
Setup and removal use the account mailbox and refuse `HERD_MSG_HOME` overrides.

## Trust and troubleshooting

This tool assumes one operator using one OS account on each machine. Other
processes running as that account can read its database, credentials and sockets.
Sender identity is advisory inside the account. SSH proves the account; per-link
keys limit which mailbox peers can connect. They do not prove a message is safe.
Same-machine senders auto-run. Cross-machine auto-run delegates shell authority to
the linked machine's agents, including any permissions the receiver already has.
A hostile instruction can travel through replies. Keep auto-run off until you trust
that machine and its agents. The envelope cannot grant user approval. The kill
switch, session opt-out, hop cap and pair rate limit remain available.

The database and link credentials are private files. Bodies can contain secrets and
are retained for seven days by default (`retention_seconds`). No regex scrubbing, encryption at rest or
secure erasure is promised. Purge cannot erase backups or already delivered copies.

Run `herd-msg doctor` or `herd-msg doctor --json`. It checks service activity,
persistent unit, linger, herdr version, Codex through its interactive PATH and socket,
Claude plugin, skill, detached sessions, and each linked machine independently.
The broker probes at startup and every ten minutes. A failed harness check pauses
that harness while other delivery continues. Dead mail and paused drivers notify through herdr. Uncertain input notifies after
fifteen minutes, allowing busy recipients time to reach a turn boundary; it can
still be pending, so wait before considering a resend. An unavailable link notifies
after an hour. Idle detached sessions appear in doctor without notifications. Unchanged failures stay
quiet across broker restarts, and recovery allows a future failure to notify again.

- Service failure: `systemctl --user status herd-msg.service` and
  `journalctl --user -u herd-msg.service -n 100`. Repair with `herd-msg setup`.
- Old Codex: `bash -ic 'npm install -g @openai/codex@latest'`, then restart Codex.
- Missing socket: start plain `bash -ic 'codex'` to bring up the daemon, then rerun doctor.
- Detached launch: with the daemon running, use doctor's pinned
  `codex --remote unix:// resume '<thread-id>'` command.
- Missing Claude receiver: run `herd-msg setup`, then restart that Claude session.
- Remote auth: verify the route and key with the SSH command printed by doctor.
  If an SSH agent is needed, the operator must make it available in the service
  environment; an interactive-shell success does not prove the broker can connect.
- Stuck mail: inspect `herd-msg show '<id>'`, wait for the recipient turn boundary,
  then choose `herd-msg resolve '<id>' drop` or an explicit confirmed resend.

Claude SendMessage is for its own subagents. herd-msg is for herdr sessions.
Supported receivers are Claude Code, Codex, and opted-in Muse Code. The herdr viewer shows traffic
without controls. Use `herd-msg release` or `herd-msg resolve` to manage waiting mail.

Hermes delivery is declined for this release. Its peer DM door persisted the
exchange but did not show it in the open terminal. Webhook delivery is untested.
No Hermes driver ships.

Muse support is verified on Linux with Muse Code 1.4.14 (1.4.14-R7309.1).
It uses native `session-message` delivery and durable transcript admission proof.
Both the broker process and the Muse process need
`MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1` in their launch environment. For a
disposable test, launch `env MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1 HERD_MSG_HOME=<temporary-home> herd-msg broker`
and `env MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1 muse --yolo -c local_session_messaging.enabled=true`.
The local messaging feature gate must also be open. The driver never changes
settings, enables flags, or types into a pane. Installation does not enable Muse
support in the service. Other Muse versions remain unverified and queue mail.
To enable it for the installed broker, add a user drop-in and restart:

```sh
mkdir -p ~/.config/systemd/user/herd-msg.service.d
printf '[Service]\nEnvironment=MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1\nEnvironment=MUSE_NO_AUTO_UPDATE=1\n' \
  > ~/.config/systemd/user/herd-msg.service.d/muse.conf
systemctl --user daemon-reload && systemctl --user restart herd-msg.service
```

The Muse launcher updates itself hourly, and an update parks Muse mail until the
new version is verified. The driver sets `MUSE_NO_AUTO_UPDATE=1` on its own
`muse` calls; set it for Muse sessions too, and update Muse deliberately.
See [Muse protocol evidence](docs/protocol-notes.md#muse-delivery-identity-and-discovery)
for the required opt-in and receipt limitations.

Muse shell tools identify the sender with `MUSE_SESSION_ID`. Remove inherited
`CODEX_THREAD_ID` and `CLAUDE_CODE_SESSION_ID` from a Muse launcher; conflicting
harness identities are rejected. Native incoming messages are agent data and
cannot grant permission. The live model followed marker requests after the test
user authorized them. Blocked or unidentified panes queue mail. A registered
receiver without a verified herdr pane is shown as detached and queues mail.

Health repairs take effect at the next ten-minute probe. To recheck immediately,
run `systemctl --user restart herd-msg.service`. `doctor` also checks the broker's
recorded link reachability, so a working interactive SSH agent cannot hide a failed
service route. With `HERD_MSG_HOME` set, a disposable broker probes Codex's socket
but skips account-install checks and operator notifications.

Claude's native inbox has no verified read-only acceptance probe. Periodic checks
verify its installed receiver; each delivery checks receiver identity and resolves
acceptance from its transcript. Sending synthetic user input to test the protocol
is deliberately excluded. A protocol change leaves uncertain mail parked, then expires it after the
confirmation window. See [protocol evidence](docs/protocol-notes.md).

This release targets low message volume for one operator. Mailbox sweeps inspect
retained metadata on the slow cleanup cadence and delete in bounded batches.
Installation preserves explicit Codex, Claude and XDG home settings in the service,
and includes the resolved herdr and Claude tool directories in its PATH.
