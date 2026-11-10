# Security

## Reporting a vulnerability

Please report security problems privately through GitHub's
**Report a vulnerability** button on the repository's Security tab, not in a
public issue. If that button is not available, open an issue that asks for a
private contact and leave out any details. Include the herd-msg version (`herd-msg version`), the harness
versions involved, and steps to reproduce. You should get an acknowledgement
within a week. This is a single-maintainer project, so fixes ship on a best-effort
basis.

## What herd-msg does and does not protect

herd-msg assumes one operator using one OS account on each machine. Read the
[Trust and troubleshooting](README.md#trust-and-troubleshooting) section of the
README before linking machines. In short:

- Any process running as the same OS account can read the mailbox database,
  link credentials and sockets, and can claim to be any sender. Sender identity
  is advisory inside an account.
- Message bodies are stored unencrypted in SQLite and kept for seven days by
  default. Do not send secrets you would not keep in a local file.
- Mail is untrusted input to the receiving agent. An envelope from another agent
  never carries user approval, and a hostile instruction can travel through
  replies.
- Same-machine mail runs automatically. Cross-machine mail is held until the
  operator releases it, unless the operator grants auto-run on that link, which
  delegates the receiving session's shell authority to the linked machine's agents.
- SSH links use non-interactive authentication with strict host-key checking and
  a per-link key. They authenticate the account, not the safety of a message.

Reports that show a bypass of these boundaries are in scope: for example,
cross-machine mail running without an auto-run grant, a linked peer reaching a
mailbox it was not linked to, operator-only commands accepting an agent caller,
or credentials appearing in config, status or logs.
