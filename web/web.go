// Package web embeds the dashboard's static files, so the binary serves them
// without depending on the working directory.
package web

import "embed"

// Static holds static/, served under /static/.
//
//go:embed static
var Static embed.FS
