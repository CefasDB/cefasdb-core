package compatibility

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Diagnostic codes defined by the compatibility catalog contract.
const (
	DiagnosticSchema   = "CCAT-001"
	DiagnosticIdentity = "CCAT-002"
	DiagnosticRequired = "CCAT-003"
)

// Surface identifies a public interface family.
type Surface string

const (
	SurfaceGRPC Surface = "grpc"
	SurfaceHTTP Surface = "http"
	SurfaceCLI  Surface = "cli"
	SurfaceSDK  Surface = "sdk"
	SurfaceSQL  Surface = "sql"
)

// Validate reports whether the surface is part of the version-one catalog.
func (s Surface) Validate() error {
	switch s {
	case SurfaceGRPC, SurfaceHTTP, SurfaceCLI, SurfaceSDK, SurfaceSQL:
		return nil
	default:
		return diagnostic(DiagnosticRequired, "surface %q is not one of grpc, http, cli, sdk, or sql", s)
	}
}

// SupportState is the lifecycle state of a compatibility entry.
type SupportState string

const (
	SupportUnassessed   SupportState = "unassessed"
	SupportExperimental SupportState = "experimental"
	SupportUnsupported  SupportState = "unsupported"
	SupportSupported    SupportState = "supported"
	SupportDeprecated   SupportState = "deprecated"
	SupportRemoved      SupportState = "removed"
)

// Validate reports whether the support state belongs to the catalog enum.
func (s SupportState) Validate() error {
	switch s {
	case SupportUnassessed, SupportExperimental, SupportUnsupported, SupportSupported, SupportDeprecated, SupportRemoved:
		return nil
	default:
		return diagnostic(DiagnosticRequired, "support state %q is invalid", s)
	}
}

var (
	entryIDPattern                = regexp.MustCompile(`^[a-z][a-z0-9-]*([.][a-z][a-z0-9-]*)+$`)
	mutableComparisonAliasPattern = regexp.MustCompile(
		`(?i)^(latest|current|stable|head|main|master|nightly|snapshot|edge)$`,
	)
	nonExactComparisonVersionPattern = regexp.MustCompile(
		`(?i)(?:^|[._+\-])x(?:$|[._+\-])|[<>=~^*|,]|\s`,
	)
)

// EntryID is the stable, release-independent identity of an operation.
type EntryID struct {
	value string
}

// NewEntryID validates and constructs an EntryID.
func NewEntryID(value string) (EntryID, error) {
	id := EntryID{value: value}
	if err := id.Validate(); err != nil {
		return EntryID{}, err
	}
	return id, nil
}

// MustEntryID constructs an EntryID or panics. It is intended for tests and fixtures.
func MustEntryID(value string) EntryID {
	id, err := NewEntryID(value)
	if err != nil {
		panic(err)
	}
	return id
}

// String returns the canonical identity.
func (id EntryID) String() string {
	return id.value
}

// Validate enforces the exact version-one EntryID grammar.
func (id EntryID) Validate() error {
	if !entryIDPattern.MatchString(id.value) {
		return diagnostic(DiagnosticIdentity, "entry id %q must match %s", id.value, entryIDPattern.String())
	}
	return nil
}

// MarshalText preserves the canonical identity at serialization boundaries.
func (id EntryID) MarshalText() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return []byte(id.value), nil
}

// UnmarshalText validates an EntryID read at a serialization boundary.
func (id *EntryID) UnmarshalText(text []byte) error {
	parsed, err := NewEntryID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

var (
	grpcBindingPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:[.][A-Za-z_][A-Za-z0-9_]*)*/[A-Za-z_][A-Za-z0-9_]*$`)
	httpMethodPattern  = regexp.MustCompile(`^[A-Z]+$`)
	sdkBindingPattern  = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._~-]*[A-Za-z0-9])?(?:/[A-Za-z0-9](?:[A-Za-z0-9._~-]*[A-Za-z0-9])?)*[.][(][*]?[A-Za-z_][A-Za-z0-9_]*[)][.][A-Za-z_][A-Za-z0-9_]*$`)
	sqlBindingPattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_-]*( [A-Z][A-Z0-9_-]*)*$`)
)

// BindingID is a canonical, surface-qualified interface operation identity.
type BindingID struct {
	value string
}

// NewBindingID validates and constructs a BindingID.
func NewBindingID(value string) (BindingID, error) {
	id := BindingID{value: value}
	if err := id.Validate(); err != nil {
		return BindingID{}, err
	}
	return id, nil
}

// MustBindingID constructs a BindingID or panics. It is intended for tests and fixtures.
func MustBindingID(value string) BindingID {
	id, err := NewBindingID(value)
	if err != nil {
		panic(err)
	}
	return id
}

// String returns the canonical surface-qualified identity.
func (id BindingID) String() string {
	return id.value
}

// Surface returns the surface encoded by the canonical prefix.
func (id BindingID) Surface() Surface {
	prefix, _, ok := strings.Cut(id.value, ":")
	if !ok {
		return ""
	}
	return Surface(prefix)
}

// Validate enforces a known lower-case surface prefix and canonical surface form.
func (id BindingID) Validate() error {
	if id.value == "" || strings.TrimSpace(id.value) != id.value || containsControlWhitespace(id.value) {
		return diagnostic(DiagnosticIdentity, "binding id %q is empty or contains non-canonical whitespace", id.value)
	}
	prefix, rest, ok := strings.Cut(id.value, ":")
	if !ok || rest == "" {
		return diagnostic(DiagnosticIdentity, "binding id %q must be surface-qualified", id.value)
	}
	surface := Surface(prefix)
	if err := surface.Validate(); err != nil {
		return diagnostic(DiagnosticIdentity, "binding id %q has an invalid surface prefix", id.value)
	}

	switch surface {
	case SurfaceGRPC:
		if !grpcBindingPattern.MatchString(rest) {
			return diagnostic(DiagnosticIdentity, "binding id %q must use grpc:<service>/<method>", id.value)
		}
	case SurfaceHTTP:
		method, path, found := strings.Cut(rest, ":")
		if !found || !httpMethodPattern.MatchString(method) || !strings.HasPrefix(path, "/") || strings.Contains(path, " ") {
			return diagnostic(DiagnosticIdentity, "binding id %q must use http:<METHOD>:/<path>", id.value)
		}
	case SurfaceCLI:
		if strings.TrimSpace(rest) != rest || strings.Contains(rest, "  ") {
			return diagnostic(DiagnosticIdentity, "binding id %q must use single-space CLI tokens", id.value)
		}
	case SurfaceSDK:
		if !sdkBindingPattern.MatchString(rest) {
			return diagnostic(DiagnosticIdentity, "binding id %q must use sdk:<import-path>.(<receiver>).<method>", id.value)
		}
	case SurfaceSQL:
		if !sqlBindingPattern.MatchString(rest) {
			return diagnostic(DiagnosticIdentity, "binding id %q must use an upper-case SQL statement family", id.value)
		}
	}
	return nil
}

// MarshalText preserves the canonical binding identity at serialization boundaries.
func (id BindingID) MarshalText() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return []byte(id.value), nil
}

// UnmarshalText validates a BindingID read at a serialization boundary.
func (id *BindingID) UnmarshalText(text []byte) error {
	parsed, err := NewBindingID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// GlossaryRef pins the domain language used by one registry.
type GlossaryRef struct {
	InitiativeID string
	Revision     string
	Terms        map[string]string
}

// Validate requires an explicit initiative, immutable revision, and non-empty terms.
func (g GlossaryRef) Validate() error {
	if err := required("glossary initiativeId", g.InitiativeID); err != nil {
		return err
	}
	if err := required("glossary revision", g.Revision); err != nil {
		return err
	}
	if len(g.Terms) == 0 {
		return diagnostic(DiagnosticRequired, "glossary terms must not be empty")
	}
	for term, definition := range g.Terms {
		if strings.TrimSpace(term) == "" || strings.TrimSpace(definition) == "" {
			return diagnostic(DiagnosticRequired, "glossary terms must have non-empty names and definitions")
		}
		if strings.TrimSpace(term) != term || strings.TrimSpace(definition) != definition {
			return diagnostic(DiagnosticRequired, "glossary term %q is not canonical", term)
		}
	}
	return nil
}

// ComparisonTarget names the exact external product version used for comparison.
type ComparisonTarget struct {
	ID           string
	Product      string
	ExactVersion string
	Source       string
}

// Validate rejects aliases and incomplete comparison identities.
func (t ComparisonTarget) Validate() error {
	for name, value := range map[string]string{
		"comparison target id":      t.ID,
		"comparison target product": t.Product,
		"comparison exactVersion":   t.ExactVersion,
		"comparison target source":  t.Source,
	} {
		if err := required(name, value); err != nil {
			return err
		}
	}
	if mutableComparisonAliasPattern.MatchString(t.ExactVersion) {
		return diagnostic(
			DiagnosticRequired,
			"comparison target %q must use an exact version, not mutable alias %q",
			t.ID,
			t.ExactVersion,
		)
	}
	if nonExactComparisonVersionPattern.MatchString(t.ExactVersion) {
		return diagnostic(
			DiagnosticRequired,
			"comparison target %q must use an exact version, not range %q",
			t.ID,
			t.ExactVersion,
		)
	}
	return nil
}

// CompatibilityPolicy contains accountable-owner catalog policy values.
type CompatibilityPolicy struct {
	SchemaVersion           int
	ComparisonTargets       []ComparisonTarget
	MinimumNoticeReleases   uint
	EmergencyRemovalAllowed bool
}

// Validate enforces version one, exact unique targets, and a nonzero notice window.
func (p CompatibilityPolicy) Validate() error {
	if p.SchemaVersion != 1 {
		return diagnostic(DiagnosticSchema, "policy schemaVersion must be 1, got %d", p.SchemaVersion)
	}
	if p.MinimumNoticeReleases == 0 {
		return diagnostic(DiagnosticRequired, "minimumNoticeReleases must be greater than zero")
	}
	if len(p.ComparisonTargets) == 0 {
		return diagnostic(DiagnosticRequired, "comparisonTargets must not be empty")
	}
	seen := make(map[string]struct{}, len(p.ComparisonTargets))
	for _, target := range p.ComparisonTargets {
		if err := target.Validate(); err != nil {
			return err
		}
		if _, exists := seen[target.ID]; exists {
			return diagnostic(DiagnosticIdentity, "duplicate comparison target id %q", target.ID)
		}
		seen[target.ID] = struct{}{}
	}
	return nil
}

// BehaviorError describes one declared error result.
type BehaviorError struct {
	Code        string
	Condition   string
	Description string
}

// Validate requires a stable code and explicit error semantics.
func (e BehaviorError) Validate() error {
	if err := required("behavior error code", e.Code); err != nil {
		return err
	}
	if err := optional("behavior error condition", e.Condition); err != nil {
		return err
	}
	if err := optional("behavior error description", e.Description); err != nil {
		return err
	}
	if strings.TrimSpace(e.Condition) == "" && strings.TrimSpace(e.Description) == "" {
		return diagnostic(DiagnosticRequired, "behavior error %q must declare condition or description", e.Code)
	}
	return nil
}

// PaginationRule declares whether and how results paginate.
type PaginationRule struct {
	Mode          string
	RequestToken  string
	ResponseToken string
	Description   string
}

// Validate requires an explicit mode, including none when pagination is inapplicable.
func (p PaginationRule) Validate() error {
	if err := required("pagination mode", p.Mode); err != nil {
		return err
	}
	if p.Mode == "none" && (p.RequestToken != "" || p.ResponseToken != "") {
		return diagnostic(DiagnosticRequired, "pagination mode none cannot declare token fields")
	}
	return nil
}

// StreamingRule declares the public streaming shape.
type StreamingRule struct {
	Mode        string
	Description string
}

// Validate enforces the streaming enum used by surface reconciliation.
func (s StreamingRule) Validate() error {
	switch s.Mode {
	case "none", "client", "server", "bidi":
		return nil
	default:
		return diagnostic(DiagnosticRequired, "streaming mode %q is invalid", s.Mode)
	}
}

// LimitRule declares one named compatibility limit.
type LimitRule struct {
	Name        string
	Value       string
	Description string
}

// Validate requires both the limit identity and its explicit value.
func (l LimitRule) Validate() error {
	if err := required("limit name", l.Name); err != nil {
		return err
	}
	if err := required("limit value", l.Value); err != nil {
		return err
	}
	return nil
}

// Deviation describes a binding-specific difference from its comparison target.
type Deviation struct {
	Summary     string
	Description string
}

// Validate requires a concise or detailed statement of the deviation.
func (d Deviation) Validate() error {
	if err := optional("deviation summary", d.Summary); err != nil {
		return err
	}
	if err := optional("deviation description", d.Description); err != nil {
		return err
	}
	if strings.TrimSpace(d.Summary) == "" && strings.TrimSpace(d.Description) == "" {
		return diagnostic(DiagnosticRequired, "deviation must declare summary or description")
	}
	return nil
}

// Deprecation records replacement, notice, removal floor, and migration guidance.
type Deprecation struct {
	ReplacementEntryID     EntryID
	NoticeRelease          string
	RemovalEarliestRelease string
	Migration              string
}

// Validate requires all deprecation metadata and a valid replacement identity.
func (d Deprecation) Validate() error {
	if err := d.ReplacementEntryID.Validate(); err != nil {
		return err
	}
	if err := required("deprecation noticeRelease", d.NoticeRelease); err != nil {
		return err
	}
	if err := required("deprecation removalEarliestRelease", d.RemovalEarliestRelease); err != nil {
		return err
	}
	if err := required("deprecation migration", d.Migration); err != nil {
		return err
	}
	return nil
}

// BehaviorContract declares each behavior dimension for an entry or binding override.
type BehaviorContract struct {
	Result      string
	Errors      []BehaviorError
	Pagination  PaginationRule
	Streaming   StreamingRule
	Consistency []string
	Limits      []LimitRule
}

// Validate requires an explicit value for every behavior dimension.
func (b BehaviorContract) Validate() error {
	if err := required("behavior result", b.Result); err != nil {
		return err
	}
	if len(b.Errors) == 0 {
		return diagnostic(DiagnosticRequired, "behavior errors must not be empty")
	}
	for _, behaviorError := range b.Errors {
		if err := behaviorError.Validate(); err != nil {
			return err
		}
	}
	if err := b.Pagination.Validate(); err != nil {
		return err
	}
	if err := b.Streaming.Validate(); err != nil {
		return err
	}
	if len(b.Consistency) == 0 {
		return diagnostic(DiagnosticRequired, "behavior consistency must not be empty")
	}
	for _, consistency := range b.Consistency {
		if err := required("behavior consistency value", consistency); err != nil {
			return err
		}
	}
	if len(b.Limits) == 0 {
		return diagnostic(DiagnosticRequired, "behavior limits must not be empty")
	}
	for _, limit := range b.Limits {
		if err := limit.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// InterfaceBinding maps a concrete public binding to an entry.
type InterfaceBinding struct {
	BindingID        BindingID
	Surface          Surface
	BehaviorOverride *BehaviorContract
	Deviations       []Deviation
}

// Validate enforces identity, enum, prefix agreement, and complete overrides.
func (b InterfaceBinding) Validate() error {
	if err := b.BindingID.Validate(); err != nil {
		return err
	}
	if err := b.Surface.Validate(); err != nil {
		return bindingDiagnostic(b.BindingID, "surface", err)
	}
	if b.BindingID.Surface() != b.Surface {
		return bindingDiagnostic(
			b.BindingID,
			"surface",
			diagnostic(DiagnosticIdentity, "surface %q does not match canonical prefix %q", b.Surface, b.BindingID.Surface()),
		)
	}
	if b.BehaviorOverride != nil {
		if err := b.BehaviorOverride.Validate(); err != nil {
			return bindingDiagnostic(b.BindingID, "behaviorOverride", err)
		}
	}
	for index, deviation := range b.Deviations {
		if err := deviation.Validate(); err != nil {
			return bindingDiagnostic(b.BindingID, fmt.Sprintf("deviation[%d]", index), err)
		}
	}
	return nil
}

// CompatibilityEntry is one stable operation promise and its public bindings.
type CompatibilityEntry struct {
	ID                    EntryID
	Operation             string
	Owner                 string
	State                 SupportState
	FirstSupportedRelease *string
	Bindings              []InterfaceBinding
	Behavior              BehaviorContract
	ComparisonTargetID    *string
	EvidenceCaseIDs       []string
	Deprecation           *Deprecation
}

// Validate checks entry-local invariants. Cross-entry references are checked by WorkingRegistry.
func (e CompatibilityEntry) Validate() error {
	if err := e.ID.Validate(); err != nil {
		return err
	}
	if err := required("entry operation", e.Operation); err != nil {
		return err
	}
	if err := required("entry owner", e.Owner); err != nil {
		return err
	}
	if err := e.State.Validate(); err != nil {
		return err
	}
	if len(e.Bindings) == 0 {
		return diagnostic(DiagnosticRequired, "entry %q must declare at least one binding", e.ID)
	}
	seenBindings := make(map[string]struct{}, len(e.Bindings))
	hasDeviation := false
	for _, binding := range e.Bindings {
		if err := binding.Validate(); err != nil {
			return err
		}
		key := binding.BindingID.String()
		if _, exists := seenBindings[key]; exists {
			return diagnostic(DiagnosticIdentity, "entry %q has duplicate binding id %q", e.ID, key)
		}
		seenBindings[key] = struct{}{}
		hasDeviation = hasDeviation || len(binding.Deviations) > 0
	}
	if err := e.Behavior.Validate(); err != nil {
		return err
	}

	switch e.State {
	case SupportSupported, SupportDeprecated, SupportRemoved:
		if e.FirstSupportedRelease == nil {
			return diagnostic(DiagnosticRequired, "entry %q state %q requires firstSupportedRelease", e.ID, e.State)
		}
		if err := required("entry firstSupportedRelease", *e.FirstSupportedRelease); err != nil {
			return err
		}
	}
	if e.State == SupportSupported && len(e.EvidenceCaseIDs) == 0 {
		return diagnostic(DiagnosticRequired, "supported entry %q must declare evidenceCaseIds", e.ID)
	}
	seenCases := make(map[string]struct{}, len(e.EvidenceCaseIDs))
	for _, caseID := range e.EvidenceCaseIDs {
		if err := required("evidence case id", caseID); err != nil {
			return err
		}
		if _, exists := seenCases[caseID]; exists {
			return diagnostic(DiagnosticIdentity, "entry %q has duplicate evidence case id %q", e.ID, caseID)
		}
		seenCases[caseID] = struct{}{}
	}
	if e.ComparisonTargetID != nil {
		if err := required("comparisonTargetId", *e.ComparisonTargetID); err != nil {
			return err
		}
	} else if hasDeviation {
		return diagnostic(DiagnosticRequired, "entry %q has deviations but no comparisonTargetId", e.ID)
	}

	needsDeprecation := e.State == SupportDeprecated || e.State == SupportRemoved
	if needsDeprecation && e.Deprecation == nil {
		return diagnostic(DiagnosticRequired, "entry %q state %q requires deprecation data", e.ID, e.State)
	}
	if !needsDeprecation && e.Deprecation != nil {
		return diagnostic(DiagnosticRequired, "entry %q state %q cannot carry deprecation data", e.ID, e.State)
	}
	if e.Deprecation != nil {
		if err := e.Deprecation.Validate(); err != nil {
			return err
		}
		if e.Deprecation.ReplacementEntryID == e.ID {
			return diagnostic(DiagnosticRequired, "entry %q cannot replace itself", e.ID)
		}
	}
	return nil
}

// InventoryExclusion is an approved reason that a binding is not cataloged.
type InventoryExclusion struct {
	BindingID  BindingID
	ReasonCode string
	Rationale  string
	Owner      string
}

// Validate requires a canonical binding and accountable exclusion metadata.
func (e InventoryExclusion) Validate() error {
	if err := e.BindingID.Validate(); err != nil {
		return err
	}
	if err := required("inventory exclusion reasonCode", e.ReasonCode); err != nil {
		return err
	}
	if err := required("inventory exclusion rationale", e.Rationale); err != nil {
		return err
	}
	if err := required("inventory exclusion owner", e.Owner); err != nil {
		return err
	}
	return nil
}

// WorkingRegistry is the mutable authoring aggregate. Normalize returns a detached copy.
type WorkingRegistry struct {
	SchemaVersion       int
	Glossary            GlossaryRef
	Policy              CompatibilityPolicy
	Entries             []CompatibilityEntry
	InventoryExclusions []InventoryExclusion
}

// Validate checks aggregate invariants without changing the registry.
func (r WorkingRegistry) Validate() error {
	if r.SchemaVersion != 1 {
		return diagnostic(DiagnosticSchema, "registry schemaVersion must be 1, got %d", r.SchemaVersion)
	}
	if err := r.Glossary.Validate(); err != nil {
		return err
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	if len(r.Entries) == 0 {
		return diagnostic(DiagnosticRequired, "registry entries must not be empty")
	}

	targets := make(map[string]struct{}, len(r.Policy.ComparisonTargets))
	for _, target := range r.Policy.ComparisonTargets {
		targets[target.ID] = struct{}{}
	}
	entryIDs := make(map[string]struct{}, len(r.Entries))
	bindingIDs := make(map[string]struct{})
	for _, entry := range r.Entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		entryID := entry.ID.String()
		if _, exists := entryIDs[entryID]; exists {
			return diagnostic(DiagnosticIdentity, "duplicate entry id %q", entryID)
		}
		entryIDs[entryID] = struct{}{}
		if entry.ComparisonTargetID != nil {
			if _, exists := targets[*entry.ComparisonTargetID]; !exists {
				return diagnostic(DiagnosticRequired, "entry %q references unknown comparison target %q", entry.ID, *entry.ComparisonTargetID)
			}
		}
		for _, binding := range entry.Bindings {
			bindingID := binding.BindingID.String()
			if _, exists := bindingIDs[bindingID]; exists {
				return diagnostic(DiagnosticIdentity, "duplicate binding id %q", bindingID)
			}
			bindingIDs[bindingID] = struct{}{}
		}
	}
	for _, entry := range r.Entries {
		if entry.Deprecation == nil {
			continue
		}
		replacementID := entry.Deprecation.ReplacementEntryID.String()
		if _, exists := entryIDs[replacementID]; !exists {
			return diagnostic(
				DiagnosticRequired,
				"entry %q references unknown replacement entry %q",
				entry.ID,
				replacementID,
			)
		}
	}

	exclusions := make(map[string]struct{}, len(r.InventoryExclusions))
	for _, exclusion := range r.InventoryExclusions {
		if err := exclusion.Validate(); err != nil {
			return err
		}
		bindingID := exclusion.BindingID.String()
		if _, exists := exclusions[bindingID]; exists {
			return diagnostic(DiagnosticIdentity, "duplicate inventory exclusion binding id %q", bindingID)
		}
		if _, exists := bindingIDs[bindingID]; exists {
			return diagnostic(DiagnosticIdentity, "binding id %q cannot be both an entry binding and an exclusion", bindingID)
		}
		exclusions[bindingID] = struct{}{}
	}
	return nil
}

// Normalize returns a deep-enough detached copy with contract-defined lexical ordering.
func (r WorkingRegistry) Normalize() WorkingRegistry {
	normalized := WorkingRegistry{
		SchemaVersion: r.SchemaVersion,
		Glossary: GlossaryRef{
			InitiativeID: r.Glossary.InitiativeID,
			Revision:     r.Glossary.Revision,
			Terms:        cloneStringMap(r.Glossary.Terms),
		},
		Policy: CompatibilityPolicy{
			SchemaVersion:           r.Policy.SchemaVersion,
			ComparisonTargets:       append([]ComparisonTarget(nil), r.Policy.ComparisonTargets...),
			MinimumNoticeReleases:   r.Policy.MinimumNoticeReleases,
			EmergencyRemovalAllowed: r.Policy.EmergencyRemovalAllowed,
		},
		Entries:             make([]CompatibilityEntry, len(r.Entries)),
		InventoryExclusions: append([]InventoryExclusion(nil), r.InventoryExclusions...),
	}

	for index, entry := range r.Entries {
		normalized.Entries[index] = cloneEntry(entry)
	}
	sort.Slice(normalized.Policy.ComparisonTargets, func(i, j int) bool {
		return normalized.Policy.ComparisonTargets[i].ID < normalized.Policy.ComparisonTargets[j].ID
	})
	sort.Slice(normalized.Entries, func(i, j int) bool {
		return normalized.Entries[i].ID.String() < normalized.Entries[j].ID.String()
	})
	for index := range normalized.Entries {
		entry := &normalized.Entries[index]
		sort.Slice(entry.Bindings, func(i, j int) bool {
			return entry.Bindings[i].BindingID.String() < entry.Bindings[j].BindingID.String()
		})
		sort.Strings(entry.EvidenceCaseIDs)
	}
	sort.Slice(normalized.InventoryExclusions, func(i, j int) bool {
		return normalized.InventoryExclusions[i].BindingID.String() < normalized.InventoryExclusions[j].BindingID.String()
	})
	return normalized
}

// SourceLocation identifies one source stream coordinate.
type SourceLocation struct {
	Source string
	Line   int
	Column int
}

// Diagnostic is a source-aware catalog validation error.
type Diagnostic struct {
	Code      string
	Message   string
	Location  SourceLocation
	EntryID   string
	BindingID string
}

// Error returns a stable human-readable diagnostic.
func (d Diagnostic) Error() string {
	prefix := d.Code
	if d.Location.Source != "" {
		prefix += " " + d.Location.Source
		if d.Location.Line > 0 {
			prefix += fmt.Sprintf(":%d", d.Location.Line)
			if d.Location.Column > 0 {
				prefix += fmt.Sprintf(":%d", d.Location.Column)
			}
		}
	}
	if d.EntryID != "" {
		prefix += " entry=" + d.EntryID
	}
	if d.BindingID != "" {
		prefix += " binding=" + d.BindingID
	}
	if prefix == "" {
		return d.Message
	}
	return prefix + ": " + d.Message
}

func diagnostic(code, format string, args ...any) error {
	return Diagnostic{Code: code, Message: fmt.Sprintf(format, args...)}
}

func bindingDiagnostic(bindingID BindingID, context string, err error) error {
	diagnosticValue, ok := err.(Diagnostic)
	if !ok {
		return Diagnostic{
			Code:      DiagnosticSchema,
			Message:   fmt.Sprintf("%s: %v", context, err),
			BindingID: bindingID.String(),
		}
	}
	diagnosticValue.Message = context + ": " + diagnosticValue.Message
	diagnosticValue.BindingID = bindingID.String()
	return diagnosticValue
}

func containsControlWhitespace(value string) bool {
	for _, character := range value {
		if character == rune(9) || character == rune(10) || character == rune(13) {
			return true
		}
	}
	return false
}

func required(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return diagnostic(DiagnosticRequired, "%s must not be empty", name)
	}
	if strings.TrimSpace(value) != value {
		return diagnostic(DiagnosticRequired, "%s must not have leading or trailing whitespace", name)
	}
	return nil
}

func optional(name, value string) error {
	if value == "" {
		return nil
	}
	return required(name, value)
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneEntry(entry CompatibilityEntry) CompatibilityEntry {
	cloned := entry
	cloned.FirstSupportedRelease = cloneString(entry.FirstSupportedRelease)
	cloned.ComparisonTargetID = cloneString(entry.ComparisonTargetID)
	cloned.Bindings = make([]InterfaceBinding, len(entry.Bindings))
	for index, binding := range entry.Bindings {
		cloned.Bindings[index] = binding
		cloned.Bindings[index].Deviations = append([]Deviation(nil), binding.Deviations...)
		if binding.BehaviorOverride != nil {
			override := cloneBehavior(*binding.BehaviorOverride)
			cloned.Bindings[index].BehaviorOverride = &override
		}
	}
	cloned.Behavior = cloneBehavior(entry.Behavior)
	cloned.EvidenceCaseIDs = append([]string(nil), entry.EvidenceCaseIDs...)
	if entry.Deprecation != nil {
		deprecation := *entry.Deprecation
		cloned.Deprecation = &deprecation
	}
	return cloned
}

func cloneBehavior(behavior BehaviorContract) BehaviorContract {
	cloned := behavior
	cloned.Errors = append([]BehaviorError(nil), behavior.Errors...)
	cloned.Consistency = append([]string(nil), behavior.Consistency...)
	cloned.Limits = append([]LimitRule(nil), behavior.Limits...)
	return cloned
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
