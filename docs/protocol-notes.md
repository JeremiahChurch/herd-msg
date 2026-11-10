# Harness protocol behaviour

Attached Codex sessions and plugin-owned Claude MCP children can receive socket
input without changing an unsent composer draft. The observations below pin the
identity, blocked-state, acknowledgement, and lifecycle rules for the drivers.
There is no unresolved transport or identity blocker for the verified receivers
and launch forms described here. Muse requires the explicit opt-in below.

## Versions and evidence

- Local Codex CLI and protocol server: 0.166.0.
- Older saved machine: CLI and isolated protocol server 0.157.1. Its shared
  server reported 0.159.2, so that shared server was not used as evidence for
  the older protocol. The other saved machine passed transport checks with
  CLI and isolated server 0.166.0.
- Claude Code: pinned native 2.1.316, Haiku 5.5, low effort. Earlier inventory
  found remote CLIs 2.1.311 and 2.1.309. A real cross-machine reply was
  verified on 2.1.311; 2.1.309 requires its own disposable round trip.
- Herdr: local private servers 0.9.3; saved-machine inventory 0.9.5 and 0.9.3.
- Go 1.26.4 with `CGO_ENABLED=0`; `modernc.org/sqlite` 1.38.2, SQLite 3.50.3.

Methods used private tmux and named herdr servers, temporary directories under
home, and disposable Codex homes with credential copies. Foreground protocol
servers listened at explicit temporary Unix sockets. Test discovery used exact
`thread/list.cwd` filters and checked returned directories before reading any
history. No unrelated thread history was read in this run. An earlier run's
unrelated discovery captures were removed and are not evidence here.

All message injection used sockets. Terminal input was limited to authorized
test lifecycle commands, unsent drafts, and dialog choices. New configuration
overrides on TUIs always included `--remote unix://`. The attachment matrix's
deliberate detached control used `--no-daemon` without `-c`. No machine was
upgraded, no operator daemon was restarted, and no real MCP or harness
configuration was edited. Private raw evidence is retained outside this repo;
examples here replace live identifiers. Temporary processes, credential copies,
plugin registrations, and test directories were removed after verification.

The Go probes live in `internal/e2e/probes/codex`,
`internal/e2e/probes/claude`, and `internal/e2e/probes/sqlite`. The Claude probe
also supports batch writes and a `hook` mode that records hook input and token
fingerprints. These are diagnostic programs, not production delivery code.
The [official app-server contract](https://learn.chatgpt.com/docs/app-server)
describes initialization, thread operations, turns, and notifications. Local
experiments establish the version-specific results below.

## Previously tested assumptions

Question: which assumptions were reverified, and with which versions?

| Assumption | Method and evidence | Answer and version |
| --- | --- | --- |
| A second client can initialize, resume, start, and steer an attached TUI | Framed WebSocket Go probe, unique temporary cwd, persisted user items and visible replies | Passed on servers 0.157.1 and 0.166.0 |
| Idle start preserves a composer draft | Type an unsent draft, inject over the socket, inspect composer and accepted history | Passed on Codex 0.166.0 |
| Claude inbox accepts newline-delimited auth and user records | Plugin child writes both records; transcript and TUI show the marker | Passed on Claude 2.1.316 |
| Bypass holds outside senders despite a token | Outside processes sent valid-token, missing-auth, and invalid-token controls | Held in all three cases on Claude 2.1.316 |
| A real plugin MCP child delivers idle and mid-turn, preserving drafts | Child injection during idle and shell sleep; composer and transcript checks | Passed on Claude 2.1.316 |
| Config overrides can detach Codex; explicit remote fixes attachment | Earlier fresh-launch matrix plus completed process-resume matrix below | Verified on 0.166.0; extra launch flags also affect attachment |
| Attached Codex identity comes from the daemon | Private herdr pane, daemon thread reads, shell environment, no `agent_session` | Passed on Codex 0.166.0 and herdr 0.9.3 |
| Detached Codex exposes its ID through herdr | Deliberate `--no-daemon` pane with installed herdr hooks; compare daemon list and shell ID | Matching `agent_session.value`, `notLoaded`, and active writer rejection on 0.166.0 |
| MCP child environments differ by harness | Go child snapshots, including all inherited `HERDR_` keys | Codex lacks thread and herdr identity; Claude has session, socket, token |
| Herdr plugins invoke commands rather than supervise a broker | Fully isolated plugin link, action invocation, process exit, unlink; manifest and CLI inspection | One action ran once and exited on herdr 0.9.3; broker needs its own service |
| Herdr event subscriptions are not durable | Receive a creation event, disconnect, create another workspace, reconnect | Live event received; disconnected event not replayed on 0.9.3 |
| Installation is per machine and `--machine` forwards API calls | Installed CLI help and local implementation inspection | Local install/checkout operations require execution on that machine; deploy via SSH |
| Saved machine IDs are client-local | Local machine registry and herdr implementation inspection | Treat IDs as registry-local; resolve labels/targets at runtime, never share an ID as a global identity |
| User systemd is active; one saved machine lacks lingering | Local and SSH `systemctl --user is-system-running`, remote `loginctl ... -p Linger` | All three running; one saved machine returned `Linger=no` |
| Remote Codex is absent from ordinary SSH PATH | Noninteractive lookup, interactive version query, absolute npm-global executable launch | Both saved machines: absent noninteractively; CLIs 0.157.1 and 0.166.0 found interactively |
| Local bin, herd-spawn, and authenticated GitHub exist remotely | Noninteractive command lookup and `gh auth status` on both saved machines | Passed; deployment must still use recorded absolute paths |

Installation forwarding and registry-ID statements were checked against the
installed CLI and implementation; they are not claims that a remote GitHub
plugin install or an overlapping-registry experiment was performed.

## Claude plugin delivery and drafts

Question: does a plugin-owned MCP child deliver into a bypass session, including
while busy, without changing its draft?

Method: a temporary `.claude-plugin/plugin.json` declared a zero-tool stdio MCP
server. Launch with `--plugin-dir`, `--dangerously-skip-permissions`,
`--model haiku`, and `--effort low`. No `--mcp-config` or `.mcp.json` registration
was used. The child inherited the session ID, inbox socket, and token and wrote:

```json
{"type":"auth","token":"<inherited-token>"}
{"type":"user","message":{"role":"user","content":"[herd-msg] id probe-id\nReply exactly PLUGIN_DELIVERED."}}
```

Evidence on 2.1.316: the TUI displayed the peer attribution and permission
boundary wrapper, answered the marker, and the matching transcript contained an
accepted conversation `user` record. A second child message sent during a shell
sleep also produced its requested answer. Unsent idle and active drafts remained
in the composer after both deliveries. The draft text was not the injected body.

Answer: plugin loading, bypass delivery, busy delivery, and draft preservation
passed. A socket read still timed out after five seconds with zero reply bytes
on successful delivery. Sending successfully is not a positive acknowledgement.

## Claude clear, compaction, resume, and rebinding

Question: what changes across `/clear`, compaction, and process `--resume`?
Can a plugin hook supply a fresh identity after clear?

Method: stream-json headless commands first established the lifecycle. Then an
interactive plugin session used actual `/compact` and `/clear` controls. A Go
`SessionStart` hook recorded stdin, inherited session/socket coordinates, and a
token fingerprint. Child PID and startup environment were sampled separately.
A marked plugin message after clear was matched to the new transcript. Finally,
restart the TUI with `--resume` for that new conversation and repeat delivery.

| Operation | Conversation ID | Existing MCP child | Socket and token | Evidence on 2.1.316 |
| --- | --- | --- | --- | --- |
| `/compact` | Unchanged | Same PID and startup environment | Unchanged | Hook source `compact`; same ID and coordinates; headless child snapshots agree |
| `/clear` | New ID | Same PID; inherited session ID stays at its startup value | Unchanged | Hook source `clear` has new `session_id` and matching fresh hook environment; old child injects into new transcript |
| New process with `--resume <current-id>` | Preserved | New child PID with resumed ID in its environment | New path and token | TUI resumes, hook runs, marked plugin input delivers |

Evidence, abbreviated hook records:

```json
{"event":{"hook_event_name":"SessionStart","source":"compact","session_id":"<old-id>"},"env":{"CLAUDE_CODE_SESSION_ID":"<old-id>","CLAUDE_CODE_MESSAGING_SOCKET":"<same-socket>"}}
{"event":{"hook_event_name":"SessionStart","source":"clear","session_id":"<new-id>"},"env":{"CLAUDE_CODE_SESSION_ID":"<new-id>","CLAUDE_CODE_MESSAGING_SOCKET":"<same-socket>"}}
```

Answer: child startup environment alone becomes stale after clear. Socket path,
token, and child PID identify the live process, not its current conversation.
The clear hook supplies the new identity and the existing delivery coordinates;
there is a working rebinding path. This is a lifecycle requirement, not a blocker.

Recommended rule: use plugin `SessionStart` hook stdin `session_id` as the current
conversation identity. Notify the broker and the existing receiver on `clear`,
serialize rebinding with claims, fence the previous registration generation, and
register the new ID with a new generation. Keep already accepted mail pinned to
the old ID; do not move it to the new conversation. An in-flight send crossing
the fence remains ambiguous until transcript lookup proves acceptance. On
process resume, use the new child's fresh environment and replace its generation.
Do not infer a rebind from a matching socket or token. Hook code performs
rebinding; the real MCP child continues to perform inbox injection.

An inbox body `/compact` is ordinary peer text because inbound peer messages skip
slash-command dispatch. It is not a lifecycle operation. Positional Claude
startup commands did execute built-ins; interactive controls confirmed the result.

## Codex lifetime, attachment, and drafts

Question: do clear, compaction, second-client resume, or process resume change
the thread ID or disturb the attached TUI?

Method: exact temporary cwd discovery; actual terminal lifecycle controls;
`thread/compact/start`; second-client `thread/resume`; fresh TUI process resumes
of persisted seed threads. Type only unsent drafts and controls into the pane;
all new conversational input comes through the protocol socket.

Evidence on 0.166.0:

- Both compaction methods kept the thread ID. The real TUI control displayed
  `Context compacted`.
- Actual `/clear` created a new ID, shown by that pane's `/status`. The new blank
  thread was omitted from ordinary `thread/list` until its first user turn.
  A socket seed addressed the ID from the test pane; exact-cwd listing then
  found it before any new history read.
- Process resume kept the supplied thread ID. Idle and active second-client
  resumes succeeded; the original TUI continued to show output and accept
  subsequent injected input.
- An idle `turn/start` and an active `turn/steer` both preserved unsent drafts.

Answer: preserve identity across compaction and resume; treat clear as a new
recipient. A directory is not a session ID. Positional Codex startup prompt
strings `/clear` and `/compact` were persisted as literal user input in an early
attempt; they are not evidence for built-in command behaviour.

Question: which launch and resume forms attach on 0.166.0?

| Form | Method and evidence | Answer |
| --- | --- | --- |
| Plain seeded launch | Earlier minimum launch, read, resume, injection and visible answer | Attached |
| `-m gpt-6-luna` | Earlier launch matrix and injected answer | Attached |
| Initial prompt | Earlier launch matrix and injected answer | Attached |
| `-c model_reasoning_effort="low"` without remote | Earlier deliberate matrix: `notLoaded`, active-writer error on resume, thread-not-found on start | Detached; unsupported launch form |
| `--remote unix://` with `-c` | Launch matrix, low effort in TUI, successful injection | Attached |
| Plain `codex resume <id>` | New TUI process on a stored Luna seed, second-client resume and visible injection | Attached; same ID |
| Resume with `-m` | Same experiment with model selection | Attached; same ID |
| Resume with an initial prompt | Same experiment with a resume prompt | Attached; same ID |
| Resume with `--remote unix://` and `-c` | Same experiment with low-effort override | Attached; same ID |
| Resume with `-c` alone | Not launched again under the explicit-remote rule | Unverified; use the verified remote form |

Earlier combinations of bypass and hook-trust flags also caused detachment.
Do not use absence of `-c` as a universal attachment guarantee. Explicit remote
attachment and daemon state are the reliable checks. `--remote unix://` requires
an existing socket; it did not start an absent isolated server in this test.
A detached launch is a diagnosed limitation: keep mail queued and recommend
exiting the detached terminal, then using the pinned
`codex --remote unix:// resume '<thread-id>'` command printed by doctor.

## Codex active state, approval, and message IDs

Question: how can the driver identify an active turn, and what do concurrent
start, stale steer, approval waits, and questions do?

Method: inject a shell sleep, read thread status and full turns, then issue an
active start and both stale/current steer calls. Separately request escalation
and a multiple-choice question in throwaway attached sessions.

Evidence on 0.166.0, with approval/question flags also verified on 0.157.1:

```json
{"status":{"type":"active","activeFlags":[]},"turns":[{"id":"<active-turn>","status":"inProgress"}]}
{"status":{"type":"active","activeFlags":["waitingOnApproval"]}}
{"status":{"type":"active","activeFlags":["waitingOnUserInput"]}}
{"error":{"code":-32600,"message":"expected active turn id `stale-turn-id` but found `<active-turn>`"}}
```

An active `turn/start` returned the existing turn ID, not a new turn. Current-ID
steer returned that ID too. Both accepted additional input. Stale steer rejected
and its input was absent from accepted history. While waiting on approval,
start and current steer still accepted input, and `waitingOnApproval` remained.
Interrupting that test turn did not leave those queued inputs as accepted user
items. Herdr independently reported `blocked` for approval and question dialogs.

Answer: read `status` and the current `inProgress` turn, then steer with
`expectedTurnId`. Reads are observations subject to races; the expected ID is the
precondition. Gate both blocked flags before sending. Never use start as an
automatic fallback after uncertain steer: it can append more input to the same
turn. An RPC acceptance does not imply that pending input survived interruption.

Question: does `clientUserMessageId` persist, and is experimental opt-in needed?

Method: initialize with `experimentalApi: false`; send idle start, active start,
and active steer with distinct IDs; wait for completion and read user items.
Also page through a test thread with `thread/items/list`, two items per page.

Evidence on both servers 0.157.1 and 0.166.0:

```json
{"type":"userMessage","clientId":"probe-base","content":[{"type":"text","text":"<marked body>"}]}
{"type":"userMessage","clientId":"probe-active-start"}
{"type":"userMessage","clientId":"probe-current-steer"}
```

Answer: IDs persist as `userMessage.clientId`. Experimental opt-in is unnecessary
for those fields and operations. In the 0.166.0 paginated response the path is
`result.data[].item.clientId`; follow `nextCursor` to exhaustion. Full-history
reads worked but emitted a deprecation notice. A positive ID match proves
acceptance. Absence while work is queued, or after interruption, does not prove
rejection and must not authorize automatic resend. No server deduplication
promise was established.

## Codex discovery, naming, and availability

Question: can daemon identity distinguish same-directory sessions, blocked
sessions, detached sessions, and unavailable transport?

Method: two attached TUIs in one exact temporary cwd; distinct seed replies and
names; loaded-list membership; reads of their known IDs; private herdr identity
comparisons; a deliberate detached launch and its subsequent closure.

Evidence on Codex 0.166.0 and herdr 0.9.3:

- `thread/loaded/list` returns IDs and a pagination cursor. It does not return
  names or directories. `thread/read` supplies `id`, `name`, `cwd`, status,
  active flags, and optional turns.
- Two attached sessions in one cwd had distinct IDs, independent names set by
  `thread/name/set`, and both IDs appeared in the loaded list. Each TUI displayed
  its own new name. Naming also passed on 0.157.1.
- An attached private herdr pane's shell had `CODEX_THREAD_ID` but lacked
  `HERDR_PANE_ID`, `HERDR_SESSION`, and `HERDR_SOCKET_PATH`. Herdr omitted
  `agent_session`. Its TUI process did have the private pane variables.
- A detached control's shell retained those variables. Its herdr
  `agent_session.value` equaled the thread ID, while the daemon reported
  `notLoaded`. Resume returned `already has an active writer`; start returned
  `thread not found`. Closing that workspace removed its live herdr identity;
  the persisted daemon record stayed `notLoaded`.
- Exiting one attached TUI did not immediately unload its daemon session: the
  thread remained `idle`. Loaded means a reachable daemon session, not proof
  that a TUI is still open.
- A real Codex stdio MCP child lacked `CODEX_THREAD_ID` and all inherited
  `HERDR_` identity variables. A direct `thread/start.config` MCP declaration
  launched the child. The tested TUI `--remote -c mcp_servers...` forms did not
  launch that child and should not be assumed to configure a shared server.

Answer: enumerate daemon threads and identify them by ID. Use their name, or cwd
basename, for display and return ambiguity when names collide. No attached
pane-to-thread mapping is needed. Combine observations as follows:

| Observation | Broker interpretation |
| --- | --- |
| Loaded, idle or active, no blocking flags | Reachable daemon session |
| Loaded with either waiting flag | Blocked; queue mail |
| `notLoaded` plus matching live herdr `agent_session` | Detached; queue mail and show explicit-remote fix |
| `notLoaded` and no matching live herdr identity | Offline; keep old mail pinned |
| Missing socket or failed initialization | Driver transport unavailable; do not infer every session is offline |

A resume call can load a stored thread itself, so inspect availability before
resuming; do not use a successful resume as proof of a previously attached TUI.
A stale herdr snapshot is not proof of detachment. Reconcile current state.

Question: what happens across server restart?

Method: stop only the foreground server belonging to the disposable Codex home,
then start a new process at the same explicit Unix socket. Observe a plain
process-resumed TUI and an explicit-remote/config TUI; inspect exact-cwd thread
state, resume from a second client, inject a new marker, and capture both panes.

Answer on 0.166.0: both TUIs reconnected, retained their thread IDs, reported idle,
and rendered the recovery marker. No operator daemon was stopped. The temporary
socket gap is transport unavailability, not evidence that every mailbox went
offline. This proves idle-session recovery; active-turn survival, approval
survival, and restart recovery on 0.157.1 were not tested and must not be assumed.

## Claude transcript lookup, rejection, and queue limits

Question: how can a body ID resolve ambiguity, and what replies indicate
rejection or overflow?

Method: use the known test project's session transcript under
`~/.claude/projects`; parse JSON lines and match the exact envelope marker in
accepted conversation input. Idle delivery uses a `user` record with
`message.content`. Absorbed mid-turn delivery uses an `attachment` record with
`attachment.type: queued_command`, peer origin, and the marked `attachment.prompt`.
Compare the rendered record and the TUI response.
Separately send outside controls, deny one in the test UI, and send batches while
an approved shell sleep keeps the recipient busy.

Evidence and answer on 2.1.316:

- Idle, clear, and resumed plugin inputs appeared as accepted `user` records.
  The mid-turn marker appeared in a persisted `attachment` with peer origin,
  `commandMode: prompt`, and `renderedRole: system`; its rendered reminder reached
  the model and the TUI showed the requested answer. It did not create a separate
  `user` record. Held previews, API request blobs alone, and queue-operation
  records are not sufficient acceptance evidence.
- Use the known transcript filename and its outer camel-case `sessionId`. After
  clear, the absorbed attachment also carried a snake-case `session_id` equal to
  the child's stale startup ID. That field must not select the conversation.
- A valid-token outside message was held. Selecting Deny prevented delivery.
  The original connection returned zero reply bytes. With a valid listening
  `uds:` reply address, separate reverse connections received these records:

```json
{"type":"control","action":"peer_message_status","status":"held","reason":"<recipient approval required>","from":"uds:<recipient-socket>","msgV":1,"msg_id":"<generated-id>"}
{"type":"control","action":"peer_message_status","status":"denied","reason":"<recipient declined; not delivered>","from":"uds:<recipient-socket>","msgV":1,"msg_id":"<generated-id>"}
```

- The probe's supplied `msg_id` was not echoed, including when it was a UUID.
  Receipt correlation is unproven. Do not use an uncorrelated reverse receipt
  to settle a particular mailbox row.
- A plugin child sent 51 distinct inputs during a shell sleep. Debug evidence
  logged all 51 as `Routed user message to queue (priority=next)`. No 50-input
  cap was observed on this trusted-child path. This is enqueue evidence, not
  proof that every queued input later became a conversation user record.
- A separate process sent 101 held inputs. The UI reached 100 held; debug output
  said `hold buffer full` and evicted the oldest as expired before holding the
  101st. The count remained 100. The sender's original socket returned no bytes.

The claimed accepted-queue limit of 50 is therefore not a verified backpressure
contract for this receiver path. Enforce broker-owned limits. Direct write
success and read timeout cannot distinguish accepted, held, denied, or evicted
messages. Use positive transcript lookup; otherwise retain ambiguity and never
automatically resend. Reverse receipts may support a future correlated protocol
but are not needed for the verified transcript lookup path.

## Herdr state, completion, and identity

Question: which fields appear at idle, working, blocked, and done? Can injected
turns satisfy a status-based wait (the author's `herd-await` helper, which wraps
`herdr agent wait`) without completing a delegated task?

Method: real Claude and Codex panes in a private herdr 0.9.3 session, launched
through a temporary executable pane shell. Inject shell sleeps and snapshot
`herdr agent list`. Run `herd-await -s 0 -t 35 --json` through a wrapper selecting
only that private server. Repeat after a prior completion. Open real approval
and question dialogs, and compare Claude child and Codex shell IDs.

| Harness and state | Observed fields and behaviour |
| --- | --- |
| Attached Codex idle | `agent: codex`, `agent_status: idle`, pane/cwd/title fields, `state_change_seq`; no `agent_session` |
| Attached Codex working | `agent_status: working`; sequence advances; no session identity |
| Attached Codex blocked | `agent_status: blocked` on approval and question; title indicates action required |
| Codex done | Detached control with real herdr hooks reported `done`, `completion_seq`, and matching `agent_session` |
| Claude idle | `agent: claude`, `agent_status: idle`, pane/cwd/title, sequence, `agent_session: {agent: claude, kind: id, source: herdr:claude, value: <session-id>}` |
| Claude working | Same session identity; `agent_status: working` |
| Claude blocked | Real manual-mode tool approval yielded `agent_status: blocked` and retained identity |
| Claude done | Natural Stop yielded `agent_status: done` and `completion_seq`, with the same identity |

Both awaits exited 0 after injected work. The Codex pane settled idle; Claude
settled done through its hook. Injecting again after completion moved both to
working, then each completed again, and both new awaits exited 0. Explicitly
running the installed outcome-stamp helper did not add a separate visible stamp
to the attached Codex list response in this experiment. Do not infer more from
that helper's zero exit code.

Answer: a watcher can mistake an injected turn's completion for completion of
its delegated task. Require the task's own report and outcome. `agent_status`,
`state_change_seq`, or a successful await alone cannot identify which work
finished. Hook-stamped done is not a task-specific acknowledgement.

Claude `agent_session.value` matched its child session ID. Detached Codex matched
its thread ID; attached Codex correctly had no herdr session mapping. Invoking a
pinned Claude version through a temporary symlink named `claude` enabled herdr
process detection. Invoking the version-named executable directly had provided a
session identity but status `unknown`; do not combine those two detection cases.

## Installation and removal

Question: which noninteractive marketplace/plugin and Codex skill operations
work, and what remains after removal?

Method: a local fixture marketplace and plugin, a disposable
`CLAUDE_CONFIG_DIR`, and the pinned 2.1.316 CLI. No account-level plugin state was
changed. All commands below exited 0, and final lists were empty:

```text
claude plugin marketplace add <local-marketplace> --scope user --json
claude plugin install protocol-probe@protocol-probe-market --scope user --json
claude plugin list --json
claude plugin uninstall protocol-probe@protocol-probe-market --scope user --json
claude plugin marketplace remove protocol-probe-market --scope user --json
claude plugin list --json
claude plugin marketplace list --json
```

Answer: native CLI registration and clean deregistration work without terminal
input. The CLI retained the uninstalled plugin's versioned cache, marked with
`.orphaned_at`, as well as settings/registry files and a config backup. Uninstall
is not secure cache erasure. If an installer removes owned cache files, restrict
it to its own plugin; never delete the entire shared cache. Fixture directories
were removed after testing. Command-source plugins have additional confirmation
options; this test did not install or approve an arbitrary command-source plugin.

For Codex 0.166.0, create `$CODEX_HOME/skills/<name>/SKILL.md` with valid `name`
and `description` frontmatter. `skills/list` with the exact test cwd and
`forceReload: true` discovered the fixture. Removing only that skill directory
and reloading removed it from discovery. No MCP configuration change is required.

## Older Codex and supported minimum

Question: does 0.157.1 use the socket, support required operations, and establish
a minimum version without upgrading the machine?

Method: SSH to the saved machine, private tmux, disposable home, static Go probe,
and an isolated foreground server launched from the installed 0.157.1 binary.
The initialization response identified server 0.157.1. Both explicit remote and
plain model-selected TUIs were exercised; only test trust choices were accepted.

Evidence: `--remote` exists and documents `unix://` and `unix://PATH`. Plain
launch appeared loaded at its exact temporary cwd. Explicit remote resume,
idle start, active start, stale/current steer, and completed user `clientId`
persistence passed with experimental opt-in false. Loaded listing, naming, and
both blocked flags passed too. The TUI rendered accepted remote inputs. In the
older explicit-remote form, the initial thread used the server's cwd instead of
the TUI's launch cwd; restarting only that test server inside the temporary
session directory made scoped discovery possible. Plain launch used its own cwd.

Answer: **0.157.1 is the oldest fully exercised driver protocol version here**;
it is a supported floor, not proof that every older version fails. 0.166.0 also
passed. Check server version and required capabilities, plus live loaded state;
a CLI version or an existing socket alone proves neither protocol compatibility
nor attachment. The older machine's shared server reported 0.159.2 despite its
0.157.1 CLI. Versions below the verified floor should leave mail queued and be
reported as unsupported/unverified. An operator repair command is
`npm install -g @openai/codex@0.166.0`; it was not run. Do not upgrade automatically.

## Static binary and SQLite WAL portability

Question: does a locally built static Go executable run on every saved machine
and use a WAL database in its home directory?

Method: build the SQLite probe with Go 1.26.4 and `CGO_ENABLED=0`, inspect the ELF,
execute locally, copy it over SSH to both saved machines, and run there. The
probe creates a private directory under home, enables WAL, creates and commits a
row, reads it through an independent database connection, checks the WAL file,
and removes its database directory.

Evidence: static Linux x86-64 ELF; local and both saved-machine executions exited
0 and printed:

```text
sqlite=3.50.3 journal_mode=wal committed_row=committed wal_file=present
```

Answer: passed on all three machines with driver 1.38.2. This proves basic binary
and home-filesystem compatibility. It does not establish power-loss durability,
crash recovery, or concurrent-writer behaviour.

## Limits of verification

No unresolved result prevents the proposed delivery paths. The driver must
implement the verified blocked-state gates, positive-only lookup, clear
rebinding, and detached-session diagnosis before accepting mail for injection.

Deliberately untested: config-only process resume under the explicit-remote rule;
active-turn or approval survival across server restart; older-version restart
recovery; any version below 0.157.1; arbitrary command-source plugin installation;
receipt correlation and exactly-once harness deduplication. An accepted-queue cap
of 50 was not observed for trusted child input. These limits must not become
implicit guarantees.

## Delivery without resuming loaded threads

Question: can a fresh connection deliver without `thread/resume`, including a
blank thread with no rollout?

Method: independent foreground-server probes on 0.166.0 and 0.157.1, followed by
a real attached 0.157.1 terminal in a disposable home. Start and steer through
the production driver, require a stored user client ID, and compare unsent
composer drafts with full accepted history. The attached test uses the optional
`HERD_MSG_LIVE_CODEX_FLOOR_BINARY` input to `TestSupportedCodexFloor`.

Answer: passed. On 0.157.1, both attached idle start and active steer were accepted
without resume and preserved their drafts. The isolated probes also confirmed
positive lookup for two sends. On 0.166.0, a loaded blank thread accepted its
first mailbox input through the broker without a rollout resume. A blank thread
can reject history with `list_turns is not supported yet` and reject resume with
`no rollout found`; metadata reads suffice while idle. Only an active turn needs
history to obtain its expected turn ID. Ephemeral threads remain unavailable for
injection because their acceptance history is not verifiable.

After a 0.166.0 server restart, a blank or ephemeral thread that disappeared
returns `thread not loaded: <id>` with code -32600. That specific response uses
the normal fresh herdr check for offline versus detached. Other read failures
remain transport or protocol uncertainty and cannot authorize prune.

A verified interactive Codex `/clear` on 0.166.0 changed the session ID shown by
`/status`. Both the original thread and the blank replacement appeared in
`thread/loaded/list`. Input addressed to the old ID was accepted by that original
thread and did not render in the replacement terminal. This differs from the
ordinary `thread/list` omission recorded above: use the loaded list for discovery.
Daemon reachability does not establish that a terminal is watching a thread.
Use the intended conversation ID from `herd-msg peers`; do not infer it from cwd
or a retained name. The live controls wait briefly between text and Enter so the
terminal's input buffer can finish receiving the command.

## Periodic health checks

Codex's initialize handshake verifies the daemon's reported supported version without
starting or resuming a thread. Claude's inbox has no verified read-only acceptance
probe. We deliberately exclude synthetic input from health checks because it would
execute a turn in a live recipient. Health checks verify the installed receiver;
per-delivery receiver identity and transcript proof remain the acceptance boundary.
Protocol errors keep mail queued or uncertain and visible, with an alert after the
confirmation window. A successful socket write alone never proves acceptance.

## Hermes delivery decision

Hermes is not a supported receiver in this release. The peer DM door returned a
reply and persisted the exchange for the open session, but neither input nor reply
appeared in that open terminal. The webhook source creates a separate chat per
delivery; live webhook delivery is untested. The required non-typing path to a live
terminal was not demonstrated, so no Hermes driver ships.

## Muse delivery, identity, and discovery

Verified locally on Linux with `muse --version` returning
`Muse Code 1.4.14 (1.4.14-R7309.1)`, herdr 0.9.3, and Go 1.26.4.
Tests used disposable workspaces created with `herd-spawn -b -a muse`, a
temporary `HERD_MSG_HOME` broker, and tiny prompts. The automated transport test
uses the echo provider and isolated `XDG_DATA_HOME` storage, without model billing.
The installed broker, service, and Muse settings were not changed. No delivery
was sent to an unrelated session. Test workspaces were closed with
`herdr workspace close <workspace-id>`.

### Opening the native door

The ordinary command remained closed:

```sh
muse session-message list --json
# exit 1; {"schema_version":1,"status":"unavailable","error_code":"external_agent_ingress_closed"}
```

The cached `feature-config/*.json` already had `local_session_messaging: true`.
`muse config --help` exposes enterprise document validation and status, not a
user setting editor. `muse --help` documents per-run JSON `-c KEY=VALUE` overrides.
The installed binary's strings contain `local_session_messaging.enabled` and
`MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS`. The following command-scoped override
opened discovery without modifying a configuration file or shell profile:

```sh
env MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1 muse session-message list --json
# exit 0; {"schema_version":1,"sessions":[]}, before launching test receivers
```

The verified receiver launch was:

```sh
HERD_MUSE_CMD='env -u CODEX_THREAD_ID -u CLAUDE_CODE_SESSION_ID MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1 muse --yolo -c local_session_messaging.enabled=true' \
  herd-spawn -b -a muse -e low -l muse-driver-probe /tmp/herd-muse-probe '<tiny test prompt>'
```

The sender and receiver both need the external-ingress opt-in. A separate echo
receiver launched with the same local-messaging setting but without that
environment variable still appeared in the native session list; a native send
returned `external_agent_ingress_closed` and did not deliver. Listing alone is
therefore insufficient proof that external agents can send to a receiver.
The driver requires the target process's explicit environment value `1`.
No global experimental flag was enabled. The broker also requires an explicit
per-process opt-in; setup does not install one in the running service.
Without that opt-in, the driver refuses before launching any Muse command, even
for sessions retained from a previous broker run. Availability refreshes share
one fresh discovery result; every send performs a new target check.

`muse -c local_session_messaging.enabled=true session-message list --json` was
parsed as TUI arguments and refused `--json`. Placing `-c` after `session-message
list --json` returned `unknown option -c`. Use `-c` on the receiver launch, not
on the session-message subcommand. No supported `-c` ingress key was established.

### Identity and discovery

A test Muse shell printed `MUSE_SESSION_ID` and `MUSE_THREAD_ID` with the same
UUID. `HERDR_PANE_ID` matched its test pane. The launcher explicitly removed
inherited Claude and Codex identity variables. `internal/cli/cli.go` now uses
`MUSE_SESSION_ID` for Muse callers and refuses conflicting harness identities;
Muse callers cannot perform operator actions.

`muse session-message list --json` returned schema 1 session objects with
`session_id`, `session_name`, and `workspace_label`. The UUID matched the shell
environment and the exact row selected by UUID from
`~/.local/share/muse/session-index.db`. Its `sessions` table has `session_id`,
`session_log_path`, `workspace_root`, `session_name`, `status`, and
`latest_segment_terminated`, among other fields. `status: valid` describes a
stored log, not a live receiver. The driver queries only known IDs and reads only
their indexed `sessions/.../<uuid>/session.jsonl` files.

The bundled hook at
`~/.local/share/muse/plugins/cache/builtin/herdr/hooks/*/hooks/herdr_report.py`
explicitly states `agent_session_id is never emitted (D11)`. It derives
`resume_argv = ["muse", "resume", <session_id>]` from hook payloads. The test pane
reported `agent: muse` and working/done state through `herdr agent list`, but no
`agent_session`. Neither that list nor `herdr pane get` exposed `resume_argv` on
herdr 0.9.3, so the driver does not guess a UUID from cwd or a pane name.

For a native-listed UUID,
`runtime/muse/sessions/<uuid>.json` under Muse's data home contained schema 1,
the exact `session_id`, `target_eligibility: message_capable`, and
`process_generation_hint: pid=<pid>`. `herdr pane process-info --pane <pane>`
returned that PID as its sole foreground process. This binds the native session
to the pane without a directory heuristic. Two test panes in the same cwd had
distinct UUIDs and PIDs. The driver reads only the opt-in value from that
process's environment and uses herdr's current lifecycle state. Working, idle,
and done allow delivery; blocked and unknown hold it. A native-listed receiver
without a verified pane is detached and queues mail. A known UUID absent from a
successful native list is offline. Historical index rows alone are never
advertised as live. Missing transcripts hold mail because acceptance is unverifiable.

During an echo-provider peer turn, herdr's preceding Stop stamp remained done
while the durable log showed an active run. A done stamp is therefore not proof
of model inactivity. The live test uses the session log to establish the busy
condition; delivery permits both working and done and relies on native queueing.

### Delivery and acceptance

The tested native command was:

```sh
env MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1 \
  muse session-message send --target <test-session-uuid> --json < <body-file>
```

Idle delivery appeared in the live TUI as `Received message from external agent`.
A send during an observed `sleep 20` was queued, appeared after the shell result,
and triggered another model turn after the first turn completed. The model
initially declined to execute peer text as user instructions. After an explicit
user test prompt authorized harmless marker replies, the busy delivery produced
the exact assistant response `MUSE_BUSY_DELIVERED_03`. Native peer input remains
data, not user consent or tool approval. The driver preserves that attribution.

The implemented driver then delivered through the disposable broker. `herd-msg
show <message-id> --json` reported `state: injected`, one attempt, and
`outcome: accepted`; the live model printed `MUSE_BROKER_IDLE_04`. Closing the
workspace removed it from native discovery and the disposable broker reported
the same UUID as offline. The echo-provider end-to-end test passed with isolated
storage and its own broker. Echo repeats the seed prompt on a peer-triggered
turn; its new response and native received-message preview were visible.

On Linux this build returned exit 1 even for accepted sends:

```json
{"schema_version":1,"status":"unavailable","error_code":"unverified_target_receipt","target_verification":"unverified_kernel_peer","receipts":[{"stage":"target_resolved","disposition":"confirmed"},{"stage":"transport_accepted","disposition":"confirmed"}],"claimed_outcome":{"status":"accepted","receipts":[{"stage":"target_resolved","disposition":"confirmed"},{"stage":"transport_accepted","disposition":"confirmed"},{"stage":"target_admission","disposition":"accepted"}]}}
```

Actual output also included the target UUID, message ID, and request ID. Neither
exit status nor `claimed_outcome` is acceptance proof. The target log persisted a
durable schema 1 envelope with `stream.kind: session`, the exact session UUID,
`payload_type: peer_message.admission.settled`, and payload schema 2. The payload
had `cause: authenticated_external_agent`, an outcome with `status: accepted`,
and `inbox_item.body` equal to the submitted text. Its source had schema 2,
`authority: runtime_context`, `source: session_message`,
`ingress_provenance: external_agent`, the exact target UUID,
`delivery_policy: queue_next_turn`, and `wake_policy: wake_when_idle`.

The driver requires that durable admission and an exact match of the full
herd-msg envelope, which includes the unique attempt ID. Tool output, quoted
text, pending admission, rejected admission, an unrelated session, and the CLI's
unverified receipt cannot confirm delivery. A native send which started but has
no positive transcript proof remains uncertain and is never automatically
resent. Admission means accepted by the harness, not completed by the model;
busy input can wait until a turn boundary. Acceptance after interruption,
compaction, or process restart has not been established beyond retained records
that the parser can positively match.

No pane-typing delivery driver was needed or implemented. Unit tests cover
strict acceptance, retained-frame parsing, partial logs, wrong transcript paths,
blocked receivers, process binding, conflicting caller identities, and isolated
driver health. The opt-in `TestMuseNativeDelivery` creates an echo receiver,
isolated Muse storage, and a disposable broker, then sends while idle and during
an active echo run. It checks one injected attempt per message and visible native
input with another echo response for each delivery. Run it with
`HERD_MSG_LIVE=1 HERD_MSG_LIVE_MUSE=1 go test -tags live ./internal/e2e -run '^TestMuseNativeDelivery$' -v`.

## Codex launcher and cold-start checks

The exact herd-spawn launch form was verified on CLI 0.157.1 and 0.166.0 in
throwaway private tmux sessions with isolated CODEX_HOME directories:
`codex --yolo --no-alt-screen -C <dir> -m gpt-6.1-luna --remote unix:// -c model_reasoning_effort=medium <prompt>`.
Each test started its own foreground app-server in a deliberately different
working directory. Loaded-thread discovery and thread/read both confirmed that
the TUI was attached and its recorded cwd equaled the explicit -C directory.
This covers the older wrong-cwd limitation for this exact argv, without implying
that every explicit-remote variant is safe.

On both versions, a plain `codex` TUI launch with an absent isolated socket created
the daemon socket, and its initialized protocol answered thread/loaded/list.
Use plain `codex` to bring up a missing daemon; `--remote unix://` is for connecting
to an already running daemon, including a pinned detached-thread resume. The
launcher checks a connectable socket and probes flags in a bounded interactive
shell in its own session. Real-launcher checks with and without a controlling
terminal passed locally and on both saved machines. This covers non-interactive
SSH PATH, aliases and terminal job control without using a real herdr pane. If either prerequisite is absent, its effort launch retains
the previous arguments and may be detached; doctor names the operator repair.
Only owned temporary processes and homes were cleaned up. No shared daemon was
restarted and no existing session received input.

## Codex 0.167.0 steering and mail hygiene

A disposable local 0.167.0 app-server was started with `--listen unix://<temp>`
inside a temporary `CODEX_HOME`, with only a copied `auth.json`. It used the
server's default available model, `gpt-6.1-sol`. No installed service or existing
thread was changed. The opt-in test is `TestDisposableSteerProbe`; set
`HERD_MSG_STEER_PROBE_BINARY` to the installed binary to repeat it.

The tiny initial input was `Run sleep 8, then reply OK. Do nothing else.` with
`clientUserMessageId: mail-probe-start`. A metadata read observed `active`.
`turn/steer` with the exact expected turn ID, the input `After sleeping reply
STEER_OK instead.`, and `clientUserMessageId: mail-probe-steer` returned that turn
ID with no error. After completion, both `thread/items/list` and a history read
contained user messages with client IDs `mail-probe-start` and `mail-probe-steer`.
The final assistant text was `STEER_OK`. Descending item pagination also worked.
The owned server and temporary credential copy were removed after the test.

The initial probe named `gpt-6.1-luna`, used in earlier experiments, but the
account rejected it as unsupported. That turn failed before steering could be
exercised. The successful run used the available server default. These results
do not explain the missing receipts from the six earlier backlog recipients.
A version string alone cannot establish what happened to their inputs.

Automatic delivery now waits for an idle Codex thread on every supported version.
It does not use `turn/steer` even on 0.157.1, 0.166.0 or 0.167.0, where earlier
or current probes verified it. The minimum handshake version remains 0.157.1;
unknown thread status or receipt page shapes cannot authorize acceptance. Active
metadata reads no longer request full histories. A JSON-RPC error is a definite
rejection with its code and message retained in the attempt proof. A failed
transport after a write remains uncertain. There is no fallback from a rejected
steer or start to another send method.

Codex history lookup keeps an attempt cursor, checks at most two pages per call,
and repeats only the final page while waiting for an asynchronous receipt.
The immediate post-start lookup on 0.167.0 checks newest items first. Malformed pagination stops with an error. On verified 0.157.1 and 0.166.0
servers that reject pagination as unsupported, one legacy history read per
attempt per process remains allowed, subject to the 32 MiB frame limit. A cached
check prevents repeated full-history fallback. A delayed receipt on such a
legacy server may remain unproven and expire.
The broker backs off receipt checks exponentially and expires unproven delivery
after one hour by default, or earlier when the recipient is offline with no live
successor. A gone thread also ends the lookup. Expiry is not proof of non-delivery.

Muse snapshots its transcript file identity and offset before starting the native
send. The broker commits that offset with the attempt before starting native input,
so a crash before result commit still retains the starting point. Checks scan at
most 4 MiB and retain partial records across budgets and appends, up to the existing
16 MiB record limit. A partial pre-send record is skipped through its newline.
The driver caps retained partial-record buffers at 64 MiB. Rotation or truncation restarts a bounded scan. A regression
test seeks directly to an appended receipt after a sparse 537 MiB history, using
a new driver instance. Other tests cover bounded progress, partial appends, file replacement, a
6 MiB record spanning two checks, and an offset inside a pre-send record.
A subprocess crash test exits after native acceptance and before result commit,
then confirms the receipt after broker restart without repeating the send. Historical attempts lacking an offset start at the beginning and
remain subject to the broker's confirmation deadline.

A copied desktop mailbox reproduced 111.96% of one CPU core over 20 seconds
using an external synthetic app-server with 9 MiB receipt pages and no matching
receipt. Production receipt lookup accounted for 31.33% of sampled CPU; mailbox
snapshot loading accounted for 40.05%. Tick writes could wake the scheduler
immediately, and unavailable recipients repeatedly opened archive-wide updates.
With the cleanup and scheduling fixes, the same isolated workload measured 1.44%
of one core over 30 seconds after startup cleanup. This measures the broker tick
loop, not the installed service, app-server CPU or live discovery processes.
The test never sends or reads another session's real transcript.

A separate SQLite trigger test isolates the repeated WAL writes. Twenty changes
only to `next_lookup_at` caused twenty deletes and inserts each for `messages`,
`attempts` and `state_history` on the previous build. Neither dedupe, schema_version
nor write_probe changed. The WAL reached 2,698,632 bytes. The revised store makes
twenty message updates, writes no unchanged attempts or history, and reached
988,832 bytes in that fixture before its successful truncate checkpoint. Payload
sizes and checkpoint timing affect those byte counts; the trigger observations
identify the repeated rows. Slow maintenance also attempts a nonwaiting truncate
checkpoint, while automatic checkpoints and journal size limits bound reuse when
readers permit it.

## SSH route authentication

The additive `peer-route-proof` endpoint uses protocol 1 and accepts the requester's
machine name and a fresh 32-byte random nonce encoded as hex. It returns the
responder's configured machine name and a hex HMAC-SHA256. The HMAC key is the
32 decoded bytes of the link key. Its transcript is the JSON array
`["herd-msg","route-proof-v1",requester,responder,nonce]`. JSON string encoding
separates names unambiguously. The requester checks the expected responder name
and compares decoded MAC bytes with `hmac.Equal`.

The shared SSH call path obtains a fresh proof before sending any established
credential, including `key`, `old_key`, or the reused key in a `peer-install`
offer. This covers the initial reverse target, every enabled saved candidate,
relinks through the same target or a new alias, mail transfers, probes, reverse
requests, doctor probes, and revocation retries. Discovery installs a push-poll route only after
the proof and keyed probe succeed. Challenges contain no link key or sessions.
A proof does not authenticate its requester or authorize mail. Revoked records
may answer challenges so revocation retries can retain their existing semantics;
all subsequent operations still apply the normal link and revocation checks.

Initial setup has no shared secret to prove. The operator explicitly selects a
saved SSH route, strict SSH host authentication establishes that destination,
`peer-info` supplies its declared name, and `peer-install` provisions a fresh key.
An existing active key is reused only after a successful challenge. If the remote
has lost its record, unlink and relink rather than sending that active key again.

The protocol number remains 1 because existing frames and receipt semantics are
unchanged. The new verb is required before sending an established credential.
Older peers cannot answer it: reverse discovery reports a clear `ReverseError`
and stays passive, and other outgoing operations stop before credential frames.
There is no fallback to a keyed probe. Install supporting binaries on both ends.
