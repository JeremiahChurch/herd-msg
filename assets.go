// Package assets contains the installable plugin and skill files.
package assets

import "embed"

//go:embed herdr-plugin.toml .claude-plugin/marketplace.json claude-plugin/.claude-plugin/plugin.json claude-plugin/hooks/hooks.json skills/herd-msg/SKILL.md
var Files embed.FS
