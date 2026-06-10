package audience

import (
	"fmt"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

// EligibilityRequest packs the per-(campaign, user) check into a
// single value the planner can evaluate without re-shaping the
// audience plugin's individual primitives.
type EligibilityRequest struct {
	Campaign    string
	UserKey     string
	Audience    plugin.AudienceRequest
	DedupTTL    time.Duration
	FreqLimit   int
	FreqWindow  time.Duration
}

// EligibilityResult tells the caller whether to deliver + why if not.
type EligibilityResult struct {
	Eligible bool
	Reason   string // "out_of_radius" | "duplicate" | "freq_cap" | ""
}

// Eligibility composes radius selection + dedup + frequency cap into
// one check. It does NOT actually iterate Select — that path is
// reserved for batch audience builds. For per-user calls the
// audience predicate must already be true via the caller's context
// (typically the request that triggered the check carried the
// user's lat/lon). Here we ONLY check the gating predicates.
func (p *Plugin) Eligibility(req EligibilityRequest) (EligibilityResult, error) {
	if req.Campaign == "" || req.UserKey == "" {
		return EligibilityResult{}, fmt.Errorf("audience: eligibility needs campaign + user key")
	}
	if req.FreqLimit > 0 && req.FreqWindow > 0 {
		ok, err := p.FreqCap(req.Campaign, req.UserKey, req.FreqLimit, req.FreqWindow)
		if err != nil {
			return EligibilityResult{}, err
		}
		if !ok {
			return EligibilityResult{Reason: "freq_cap"}, nil
		}
	}
	if req.DedupTTL > 0 {
		ok, err := p.Dedup(req.Campaign, req.UserKey, req.DedupTTL)
		if err != nil {
			return EligibilityResult{}, err
		}
		if !ok {
			return EligibilityResult{Reason: "duplicate"}, nil
		}
	}
	return EligibilityResult{Eligible: true}, nil
}
