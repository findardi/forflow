// Package endpoint holds rules for customer endpoints shared by the API and
// the dashboard.
package endpoint

import "net/url"

// ValidURL rejects anything a worker could not POST to. It does not block
// private addresses; that is netguard's job at dial time.
func ValidURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
