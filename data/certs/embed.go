package certs

import "embed"

//go:embed ca-cert.pem
//go:embed client-cert.pem
//go:embed client-key.pem
var CertFS embed.FS
