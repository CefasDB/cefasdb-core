package audience

import (
	"encoding/json"
	"strconv"
)

// jsonUnmarshal is named so the audience file can stay
// import-minimal; centralising it here keeps the JSON dependency in
// one place.
func jsonUnmarshal(buf []byte, v any) error { return json.Unmarshal(buf, v) }

func formatFloat(x float64) string { return strconv.FormatFloat(x, 'f', -1, 64) }
