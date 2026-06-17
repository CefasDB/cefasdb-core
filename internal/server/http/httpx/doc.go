// Package httpx is the tiny shared vocabulary for the per-resource
// handler packages under internal/api/http. It exposes the two write
// helpers the entire HTTP surface uses (WriteJSON, WriteErr) so each
// resource package depends on httpx instead of either reaching back
// into internal/api or re-inventing its own JSON encoding.
//
// It deliberately has no back-channel into internal/api so the
// import graph stays one-way (internal/api → internal/api/http/httpx,
// never the reverse).
package httpx
