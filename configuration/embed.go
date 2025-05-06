package configuration

import "embed"

//go:embed dispatcher.toml
var ConfigFS embed.FS
