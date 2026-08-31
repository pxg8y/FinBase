package web

import "embed"

// Content embeds the frontend HTML, CSS, and JS files.
//go:embed index.html
var Content embed.FS
