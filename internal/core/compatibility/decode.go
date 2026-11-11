package compatibility

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// DecodeRegistry decodes, merges, validates, and normalizes the registry and
// policy authoring documents. It never returns a partial registry.
func DecodeRegistry(registry io.Reader, policy io.Reader) (WorkingRegistry, error) {
	registrySource := readerName(registry, "registry.yaml")
	policySource := readerName(policy, "policy.yaml")

	registryDocument, registryNode, err := decodeYAMLDocument[registryYAML](registry, registrySource)
	if err != nil {
		return WorkingRegistry{}, err
	}
	if registryDocument.InventoryExclusions == nil {
		return WorkingRegistry{}, newLocatedDiagnostic(
			DiagnosticRequired,
			registrySource,
			fieldValueOrParent(documentRoot(registryNode), "inventoryExclusions"),
			"inventoryExclusions is required and must be an array",
		)
	}
	policyDocument, policyNode, err := decodeYAMLDocument[policyYAML](policy, policySource)
	if err != nil {
		return WorkingRegistry{}, err
	}
	if policyDocument.EmergencyRemovalAllowed == nil {
		return WorkingRegistry{}, newLocatedDiagnostic(
			DiagnosticRequired,
			policySource,
			fieldValueOrParent(documentRoot(policyNode), "emergencyRemovalAllowed"),
			"emergencyRemovalAllowed is required and must be a boolean",
		)
	}

	working, err := mergeDocuments(registryDocument, policyDocument)
	if err != nil {
		return WorkingRegistry{}, locateConversionError(
			err,
			registrySource,
			policySource,
			registryNode,
			policyNode,
			registryDocument,
			policyDocument,
		)
	}
	if err := validateDecodedRegistry(
		working,
		registrySource,
		policySource,
		registryNode,
		policyNode,
		registryDocument,
		policyDocument,
	); err != nil {
		return WorkingRegistry{}, err
	}
	return working.Normalize(), nil
}

type registryYAML struct {
	SchemaVersion       int             `yaml:"schemaVersion"`
	Glossary            glossaryYAML    `yaml:"glossary"`
	Entries             []entryYAML     `yaml:"entries"`
	InventoryExclusions []inventoryYAML `yaml:"inventoryExclusions"`
}

type policyYAML struct {
	SchemaVersion           int              `yaml:"schemaVersion"`
	ComparisonTargets       []comparisonYAML `yaml:"comparisonTargets"`
	MinimumNoticeReleases   uint             `yaml:"minimumNoticeReleases"`
	EmergencyRemovalAllowed *bool            `yaml:"emergencyRemovalAllowed"`
}

type glossaryYAML struct {
	InitiativeID string            `yaml:"initiativeId"`
	Revision     string            `yaml:"revision"`
	Terms        map[string]string `yaml:"terms"`
}

type comparisonYAML struct {
	ID           string `yaml:"id"`
	Product      string `yaml:"product"`
	ExactVersion string `yaml:"exactVersion"`
	Source       string `yaml:"source"`
}

type entryYAML struct {
	ID                    string           `yaml:"id"`
	Operation             string           `yaml:"operation"`
	Owner                 string           `yaml:"owner"`
	State                 SupportState     `yaml:"state"`
	FirstSupportedRelease *string          `yaml:"firstSupportedRelease"`
	Bindings              []bindingYAML    `yaml:"bindings"`
	Behavior              behaviorYAML     `yaml:"behavior"`
	ComparisonTargetID    *string          `yaml:"comparisonTargetId"`
	EvidenceCaseIDs       []string         `yaml:"evidenceCaseIds"`
	Deprecation           *deprecationYAML `yaml:"deprecation"`
}

type bindingYAML struct {
	BindingID        string          `yaml:"bindingId"`
	Surface          Surface         `yaml:"surface"`
	BehaviorOverride *behaviorYAML   `yaml:"behaviorOverride"`
	Deviations       []deviationYAML `yaml:"deviations"`
}

type behaviorYAML struct {
	Result      string              `yaml:"result"`
	Errors      []behaviorErrorYAML `yaml:"errors"`
	Pagination  paginationYAML      `yaml:"pagination"`
	Streaming   streamingYAML       `yaml:"streaming"`
	Consistency []string            `yaml:"consistency"`
	Limits      []limitYAML         `yaml:"limits"`
}

type behaviorErrorYAML struct {
	Code        string `yaml:"code"`
	Condition   string `yaml:"condition"`
	Description string `yaml:"description"`
}

type paginationYAML struct {
	Mode          string `yaml:"mode"`
	RequestToken  string `yaml:"requestToken"`
	ResponseToken string `yaml:"responseToken"`
	Description   string `yaml:"description"`
}

type streamingYAML struct {
	Mode        string `yaml:"mode"`
	Description string `yaml:"description"`
}

type limitYAML struct {
	Name        string `yaml:"name"`
	Value       string `yaml:"value"`
	Description string `yaml:"description"`
}

type deviationYAML struct {
	Summary     string `yaml:"summary"`
	Description string `yaml:"description"`
}

type deprecationYAML struct {
	ReplacementEntryID     string `yaml:"replacementEntryId"`
	NoticeRelease          string `yaml:"noticeRelease"`
	RemovalEarliestRelease string `yaml:"removalEarliestRelease"`
	Migration              string `yaml:"migration"`
}

type inventoryYAML struct {
	BindingID  string `yaml:"bindingId"`
	ReasonCode string `yaml:"reasonCode"`
	Rationale  string `yaml:"rationale"`
	Owner      string `yaml:"owner"`
}

func decodeYAMLDocument[T any](reader io.Reader, source string) (T, *yaml.Node, error) {
	var zero T
	if reader == nil {
		return zero, nil, newLocatedDiagnostic(
			DiagnosticRequired,
			source,
			nil,
			"a YAML reader is required",
		)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return zero, nil, newLocatedDiagnostic(
			DiagnosticSchema,
			source,
			nil,
			fmt.Sprintf("read YAML: %v", err),
		)
	}

	nodeDecoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := nodeDecoder.Decode(&document); err != nil {
		return zero, &document, yamlDiagnostic(source, &document, err)
	}
	if documentRoot(&document) == nil {
		return zero, &document, newLocatedDiagnostic(
			DiagnosticSchema,
			source,
			&document,
			"expected exactly one non-empty YAML document",
		)
	}

	var trailing yaml.Node
	switch err := nodeDecoder.Decode(&trailing); {
	case err == nil:
		location := documentRoot(&trailing)
		if location == nil {
			location = &trailing
		}
		return zero, &document, newLocatedDiagnostic(
			DiagnosticSchema,
			source,
			location,
			"expected exactly one YAML document; found trailing document",
		)
	case !errors.Is(err, io.EOF):
		return zero, &document, yamlDiagnostic(source, &trailing, err)
	}

	strictDecoder := yaml.NewDecoder(bytes.NewReader(data))
	strictDecoder.KnownFields(true)
	var decoded T
	if err := strictDecoder.Decode(&decoded); err != nil {
		return zero, &document, yamlDiagnostic(source, &document, err)
	}
	return decoded, &document, nil
}

func mergeDocuments(registry registryYAML, policy policyYAML) (WorkingRegistry, error) {
	working := WorkingRegistry{
		SchemaVersion: registry.SchemaVersion,
		Glossary: GlossaryRef{
			InitiativeID: registry.Glossary.InitiativeID,
			Revision:     registry.Glossary.Revision,
			Terms:        cloneStringMap(registry.Glossary.Terms),
		},
		Policy: CompatibilityPolicy{
			SchemaVersion:           policy.SchemaVersion,
			ComparisonTargets:       make([]ComparisonTarget, len(policy.ComparisonTargets)),
			MinimumNoticeReleases:   policy.MinimumNoticeReleases,
			EmergencyRemovalAllowed: *policy.EmergencyRemovalAllowed,
		},
		Entries:             make([]CompatibilityEntry, len(registry.Entries)),
		InventoryExclusions: make([]InventoryExclusion, len(registry.InventoryExclusions)),
	}
	for index, target := range policy.ComparisonTargets {
		working.Policy.ComparisonTargets[index] = ComparisonTarget{
			ID:           target.ID,
			Product:      target.Product,
			ExactVersion: target.ExactVersion,
			Source:       target.Source,
		}
	}
	for index, encoded := range registry.Entries {
		entry, err := decodeEntry(encoded)
		if err != nil {
			return WorkingRegistry{}, err
		}
		working.Entries[index] = entry
	}
	for index, encoded := range registry.InventoryExclusions {
		bindingID, err := NewBindingID(encoded.BindingID)
		if err != nil {
			return WorkingRegistry{}, err
		}
		working.InventoryExclusions[index] = InventoryExclusion{
			BindingID:  bindingID,
			ReasonCode: encoded.ReasonCode,
			Rationale:  encoded.Rationale,
			Owner:      encoded.Owner,
		}
	}
	return working, nil
}

func decodeEntry(encoded entryYAML) (CompatibilityEntry, error) {
	entryID, err := NewEntryID(encoded.ID)
	if err != nil {
		return CompatibilityEntry{}, err
	}
	entry := CompatibilityEntry{
		ID:                    entryID,
		Operation:             encoded.Operation,
		Owner:                 encoded.Owner,
		State:                 encoded.State,
		FirstSupportedRelease: cloneString(encoded.FirstSupportedRelease),
		Bindings:              make([]InterfaceBinding, len(encoded.Bindings)),
		Behavior:              decodeBehavior(encoded.Behavior),
		ComparisonTargetID:    cloneString(encoded.ComparisonTargetID),
		EvidenceCaseIDs:       append([]string(nil), encoded.EvidenceCaseIDs...),
	}
	for index, encodedBinding := range encoded.Bindings {
		bindingID, err := NewBindingID(encodedBinding.BindingID)
		if err != nil {
			return CompatibilityEntry{}, err
		}
		binding := InterfaceBinding{
			BindingID:  bindingID,
			Surface:    encodedBinding.Surface,
			Deviations: make([]Deviation, len(encodedBinding.Deviations)),
		}
		if encodedBinding.BehaviorOverride != nil {
			override := decodeBehavior(*encodedBinding.BehaviorOverride)
			binding.BehaviorOverride = &override
		}
		for deviationIndex, encodedDeviation := range encodedBinding.Deviations {
			binding.Deviations[deviationIndex] = Deviation{
				Summary:     encodedDeviation.Summary,
				Description: encodedDeviation.Description,
			}
		}
		entry.Bindings[index] = binding
	}
	if encoded.Deprecation != nil {
		replacement, err := NewEntryID(encoded.Deprecation.ReplacementEntryID)
		if err != nil {
			return CompatibilityEntry{}, err
		}
		entry.Deprecation = &Deprecation{
			ReplacementEntryID:     replacement,
			NoticeRelease:          encoded.Deprecation.NoticeRelease,
			RemovalEarliestRelease: encoded.Deprecation.RemovalEarliestRelease,
			Migration:              encoded.Deprecation.Migration,
		}
	}
	return entry, nil
}

func decodeBehavior(encoded behaviorYAML) BehaviorContract {
	behavior := BehaviorContract{
		Result: encoded.Result,
		Errors: make([]BehaviorError, len(encoded.Errors)),
		Pagination: PaginationRule{
			Mode:          encoded.Pagination.Mode,
			RequestToken:  encoded.Pagination.RequestToken,
			ResponseToken: encoded.Pagination.ResponseToken,
			Description:   encoded.Pagination.Description,
		},
		Streaming: StreamingRule{
			Mode:        encoded.Streaming.Mode,
			Description: encoded.Streaming.Description,
		},
		Consistency: append([]string(nil), encoded.Consistency...),
		Limits:      make([]LimitRule, len(encoded.Limits)),
	}
	for index, encodedError := range encoded.Errors {
		behavior.Errors[index] = BehaviorError{
			Code:        encodedError.Code,
			Condition:   encodedError.Condition,
			Description: encodedError.Description,
		}
	}
	for index, encodedLimit := range encoded.Limits {
		behavior.Limits[index] = LimitRule{
			Name:        encodedLimit.Name,
			Value:       encodedLimit.Value,
			Description: encodedLimit.Description,
		}
	}
	return behavior
}

func validateDecodedRegistry(
	working WorkingRegistry,
	registrySource string,
	policySource string,
	registryDocument *yaml.Node,
	policyDocument *yaml.Node,
	registryEncoded registryYAML,
	policyEncoded policyYAML,
) error {
	registryRoot := documentRoot(registryDocument)
	policyRoot := documentRoot(policyDocument)

	if working.SchemaVersion != 1 {
		return locateModelError(
			working.Validate(),
			registrySource,
			fieldValue(registryRoot, "schemaVersion"),
			"",
			"",
		)
	}
	if err := working.Glossary.Validate(); err != nil {
		return locateModelError(
			err,
			registrySource,
			glossaryErrorNode(registryRoot, err),
			"",
			"",
		)
	}
	if err := working.Policy.Validate(); err != nil {
		return locateModelError(
			err,
			policySource,
			policyErrorNode(policyRoot, policyEncoded, err),
			"",
			"",
		)
	}
	if len(working.Entries) == 0 {
		return locateModelError(
			working.Validate(),
			registrySource,
			fieldValue(registryRoot, "entries"),
			"",
			"",
		)
	}
	entriesNode := fieldValue(registryRoot, "entries")
	for index, entry := range working.Entries {
		if err := entry.Validate(); err != nil {
			entryNode := sequenceItem(entriesNode, index)
			return locateModelError(
				err,
				registrySource,
				entryErrorNode(entryNode, registryEncoded.Entries[index], err),
				entry.ID.String(),
				bindingFromError(registryEncoded.Entries[index], err),
			)
		}
	}
	exclusionsNode := fieldValue(registryRoot, "inventoryExclusions")
	for index, exclusion := range working.InventoryExclusions {
		if err := exclusion.Validate(); err != nil {
			exclusionNode := sequenceItem(exclusionsNode, index)
			return locateModelError(
				err,
				registrySource,
				inventoryErrorNode(exclusionNode, err),
				"",
				exclusion.BindingID.String(),
			)
		}
	}
	if err := working.Validate(); err != nil {
		source, node, entryID, bindingID := aggregateErrorLocation(
			err,
			registrySource,
			policySource,
			registryDocument,
			policyDocument,
			registryEncoded,
			policyEncoded,
		)
		return locateModelError(err, source, node, entryID, bindingID)
	}
	return nil
}

func locateConversionError(
	err error,
	registrySource string,
	policySource string,
	registryDocument *yaml.Node,
	policyDocument *yaml.Node,
	registryEncoded registryYAML,
	policyEncoded policyYAML,
) error {
	_ = policyEncoded
	message := err.Error()
	registryRoot := documentRoot(registryDocument)
	for index, entry := range registryEncoded.Entries {
		entryNode := sequenceItem(fieldValue(registryRoot, "entries"), index)
		if strings.Contains(message, strconv.Quote(entry.ID)) {
			return locateModelError(err, registrySource, fieldValue(entryNode, "id"), entry.ID, "")
		}
		for bindingIndex, binding := range entry.Bindings {
			if strings.Contains(message, strconv.Quote(binding.BindingID)) {
				bindingNode := sequenceItem(fieldValue(entryNode, "bindings"), bindingIndex)
				return locateModelError(err, registrySource, fieldValue(bindingNode, "bindingId"), entry.ID, binding.BindingID)
			}
		}
		if entry.Deprecation != nil && strings.Contains(message, strconv.Quote(entry.Deprecation.ReplacementEntryID)) {
			deprecationNode := fieldValue(entryNode, "deprecation")
			return locateModelError(
				err,
				registrySource,
				fieldValue(deprecationNode, "replacementEntryId"),
				entry.ID,
				"",
			)
		}
	}
	for index, exclusion := range registryEncoded.InventoryExclusions {
		if strings.Contains(message, strconv.Quote(exclusion.BindingID)) {
			exclusionNode := sequenceItem(fieldValue(registryRoot, "inventoryExclusions"), index)
			return locateModelError(
				err,
				registrySource,
				fieldValue(exclusionNode, "bindingId"),
				"",
				exclusion.BindingID,
			)
		}
	}
	return locateModelError(err, policySource, documentRoot(policyDocument), "", "")
}

func aggregateErrorLocation(
	err error,
	registrySource string,
	policySource string,
	registryDocument *yaml.Node,
	policyDocument *yaml.Node,
	registryEncoded registryYAML,
	policyEncoded policyYAML,
) (string, *yaml.Node, string, string) {
	message := err.Error()
	if strings.Contains(message, "comparison target") && !strings.Contains(message, "entry ") {
		return policySource, policyErrorNode(documentRoot(policyDocument), policyEncoded, err), "", ""
	}
	registryRoot := documentRoot(registryDocument)
	if strings.Contains(message, "duplicate entry id") {
		for index, entry := range registryEncoded.Entries {
			if strings.Contains(message, strconv.Quote(entry.ID)) {
				entryNode := sequenceItem(fieldValue(registryRoot, "entries"), index)
				if scalarOccurrence(registryDocument, entry.ID, 2) != nil {
					return registrySource, scalarOccurrence(registryDocument, entry.ID, 2), entry.ID, ""
				}
				return registrySource, fieldValue(entryNode, "id"), entry.ID, ""
			}
		}
	}
	if strings.Contains(message, "binding id") {
		for _, entry := range registryEncoded.Entries {
			for _, binding := range entry.Bindings {
				if strings.Contains(message, strconv.Quote(binding.BindingID)) {
					node, entryID := duplicateBindingLocation(registryRoot, registryEncoded, binding.BindingID)
					return registrySource, node, entryID, binding.BindingID
				}
			}
		}
		for _, exclusion := range registryEncoded.InventoryExclusions {
			if strings.Contains(message, strconv.Quote(exclusion.BindingID)) {
				node, entryID := duplicateBindingLocation(registryRoot, registryEncoded, exclusion.BindingID)
				return registrySource, node, entryID, exclusion.BindingID
			}
		}
	}
	if strings.Contains(message, "unknown comparison target") {
		for index, entry := range registryEncoded.Entries {
			if entry.ComparisonTargetID != nil && strings.Contains(message, strconv.Quote(*entry.ComparisonTargetID)) {
				entryNode := sequenceItem(fieldValue(registryRoot, "entries"), index)
				return registrySource, fieldValue(entryNode, "comparisonTargetId"), entry.ID, ""
			}
		}
	}
	if strings.Contains(message, "unknown replacement entry") {
		for index, entry := range registryEncoded.Entries {
			if entry.Deprecation != nil &&
				strings.Contains(message, strconv.Quote(entry.Deprecation.ReplacementEntryID)) {
				entryNode := sequenceItem(fieldValue(registryRoot, "entries"), index)
				deprecationNode := fieldValue(entryNode, "deprecation")
				return registrySource, fieldValue(deprecationNode, "replacementEntryId"), entry.ID, ""
			}
		}
	}
	return registrySource, registryRoot, "", ""
}

func duplicateBindingLocation(root *yaml.Node, encoded registryYAML, bindingID string) (*yaml.Node, string) {
	var firstNode *yaml.Node
	firstEntryID := ""
	occurrence := 0
	entriesNode := fieldValue(root, "entries")
	for entryIndex, entry := range encoded.Entries {
		entryNode := sequenceItem(entriesNode, entryIndex)
		bindingsNode := fieldValue(entryNode, "bindings")
		for bindingIndex, binding := range entry.Bindings {
			if binding.BindingID != bindingID {
				continue
			}
			node := fieldValueOrParent(sequenceItem(bindingsNode, bindingIndex), "bindingId")
			occurrence++
			if occurrence == 1 {
				firstNode = node
				firstEntryID = entry.ID
			}
			if occurrence == 2 {
				return node, entry.ID
			}
		}
	}
	exclusionsNode := fieldValue(root, "inventoryExclusions")
	for exclusionIndex, exclusion := range encoded.InventoryExclusions {
		if exclusion.BindingID != bindingID {
			continue
		}
		node := fieldValueOrParent(sequenceItem(exclusionsNode, exclusionIndex), "bindingId")
		occurrence++
		if occurrence == 1 {
			firstNode = node
			firstEntryID = ""
		}
		if occurrence == 2 {
			return node, ""
		}
	}
	return firstNode, firstEntryID
}

func glossaryErrorNode(root *yaml.Node, err error) *yaml.Node {
	glossary := fieldValue(root, "glossary")
	switch {
	case strings.Contains(err.Error(), "initiativeId"):
		return fieldValueOrParent(glossary, "initiativeId")
	case strings.Contains(err.Error(), "revision"):
		return fieldValueOrParent(glossary, "revision")
	case strings.Contains(err.Error(), "terms"):
		return fieldValueOrParent(glossary, "terms")
	default:
		return glossary
	}
}

func policyErrorNode(root *yaml.Node, encoded policyYAML, err error) *yaml.Node {
	message := err.Error()
	switch {
	case strings.Contains(message, "schemaVersion"):
		return fieldValueOrParent(root, "schemaVersion")
	case strings.Contains(message, "minimumNoticeReleases"):
		return fieldValueOrParent(root, "minimumNoticeReleases")
	case strings.Contains(message, "emergencyRemovalAllowed"):
		return fieldValueOrParent(root, "emergencyRemovalAllowed")
	case strings.Contains(message, "comparisonTargets"):
		return fieldValueOrParent(root, "comparisonTargets")
	}
	targetsNode := fieldValue(root, "comparisonTargets")
	for index, target := range encoded.ComparisonTargets {
		if strings.Contains(message, strconv.Quote(target.ID)) ||
			strings.Contains(message, strconv.Quote(target.ExactVersion)) {
			targetNode := sequenceItem(targetsNode, index)
			switch {
			case strings.Contains(message, "exactVersion"), strings.Contains(message, "latest"):
				return fieldValueOrParent(targetNode, "exactVersion")
			case strings.Contains(message, "product"):
				return fieldValueOrParent(targetNode, "product")
			case strings.Contains(message, "source"):
				return fieldValueOrParent(targetNode, "source")
			default:
				return fieldValueOrParent(targetNode, "id")
			}
		}
	}
	return root
}

func entryErrorNode(entryNode *yaml.Node, encoded entryYAML, err error) *yaml.Node {
	message := err.Error()
	bindingsNode := fieldValue(entryNode, "bindings")
	if bindingID := bindingFromError(encoded, err); bindingID != "" {
		for index, binding := range encoded.Bindings {
			if binding.BindingID == bindingID {
				return bindingErrorNode(sequenceItem(bindingsNode, index), message)
			}
		}
	}
	switch {
	case strings.Contains(message, "entry id"):
		return fieldValueOrParent(entryNode, "id")
	case strings.Contains(message, "entry operation"):
		return fieldValueOrParent(entryNode, "operation")
	case strings.Contains(message, "entry owner"):
		return fieldValueOrParent(entryNode, "owner")
	case strings.Contains(message, "support state"):
		return fieldValueOrParent(entryNode, "state")
	case strings.Contains(message, "firstSupportedRelease"):
		return fieldValueOrParent(entryNode, "firstSupportedRelease")
	case strings.Contains(message, "evidence"):
		return fieldValueOrParent(entryNode, "evidenceCaseIds")
	case strings.Contains(message, "comparisonTargetId"), strings.Contains(message, "comparison target"):
		return fieldValueOrParent(entryNode, "comparisonTargetId")
	case strings.Contains(message, "deprecation"):
		deprecationNode := fieldValue(entryNode, "deprecation")
		switch {
		case strings.Contains(message, "noticeRelease"):
			return fieldValueOrParent(deprecationNode, "noticeRelease")
		case strings.Contains(message, "removalEarliestRelease"):
			return fieldValueOrParent(deprecationNode, "removalEarliestRelease")
		case strings.Contains(message, "migration"):
			return fieldValueOrParent(deprecationNode, "migration")
		default:
			return deprecationNode
		}
	case strings.Contains(message, "behavior"):
		behaviorNode := fieldValue(entryNode, "behavior")
		return behaviorErrorNode(behaviorNode, message)
	case strings.Contains(message, "binding"), strings.Contains(message, "surface"), strings.Contains(message, "deviation"):
		return bindingsNode
	default:
		return entryNode
	}
}

func bindingErrorNode(bindingNode *yaml.Node, message string) *yaml.Node {
	switch {
	case strings.Contains(message, "behaviorOverride"):
		return behaviorErrorNode(fieldValueOrParent(bindingNode, "behaviorOverride"), message)
	case strings.Contains(message, "deviation["):
		deviationsNode := fieldValue(bindingNode, "deviations")
		matches := deviationIndexPattern.FindStringSubmatch(message)
		if len(matches) == 2 {
			index, err := strconv.Atoi(matches[1])
			if err == nil {
				deviationNode := sequenceItem(deviationsNode, index)
				if strings.Contains(message, "summary") {
					return fieldValueOrParent(deviationNode, "summary")
				}
				if strings.Contains(message, "description") {
					return fieldValueOrParent(deviationNode, "description")
				}
				return deviationNode
			}
		}
		return fieldValueOrParent(bindingNode, "deviations")
	case strings.Contains(message, "surface"):
		return fieldValueOrParent(bindingNode, "surface")
	default:
		return fieldValueOrParent(bindingNode, "bindingId")
	}
}

func behaviorErrorNode(behaviorNode *yaml.Node, message string) *yaml.Node {
	switch {
	case strings.Contains(message, "result"):
		return fieldValueOrParent(behaviorNode, "result")
	case strings.Contains(message, "errors"), strings.Contains(message, "error "):
		return fieldValueOrParent(behaviorNode, "errors")
	case strings.Contains(message, "pagination"):
		return fieldValueOrParent(behaviorNode, "pagination")
	case strings.Contains(message, "streaming"):
		return fieldValueOrParent(behaviorNode, "streaming")
	case strings.Contains(message, "consistency"):
		return fieldValueOrParent(behaviorNode, "consistency")
	case strings.Contains(message, "limit"):
		return fieldValueOrParent(behaviorNode, "limits")
	default:
		return behaviorNode
	}
}

func inventoryErrorNode(exclusionNode *yaml.Node, err error) *yaml.Node {
	message := err.Error()
	switch {
	case strings.Contains(message, "binding"):
		return fieldValueOrParent(exclusionNode, "bindingId")
	case strings.Contains(message, "reasonCode"):
		return fieldValueOrParent(exclusionNode, "reasonCode")
	case strings.Contains(message, "rationale"):
		return fieldValueOrParent(exclusionNode, "rationale")
	case strings.Contains(message, "owner"):
		return fieldValueOrParent(exclusionNode, "owner")
	default:
		return exclusionNode
	}
}

func bindingFromError(entry entryYAML, err error) string {
	var diagnosticValue Diagnostic
	if errors.As(err, &diagnosticValue) && diagnosticValue.BindingID != "" {
		return diagnosticValue.BindingID
	}
	for _, binding := range entry.Bindings {
		if strings.Contains(err.Error(), strconv.Quote(binding.BindingID)) {
			return binding.BindingID
		}
	}
	return ""
}

func yamlDiagnostic(source string, document *yaml.Node, err error) error {
	message := err.Error()
	code := DiagnosticSchema
	if strings.Contains(message, DiagnosticIdentity) {
		code = DiagnosticIdentity
	} else if strings.Contains(message, DiagnosticRequired) {
		code = DiagnosticRequired
	}
	node := yamlErrorNode(document, message)
	entryID, bindingID := yamlDiagnosticContext(document, node)
	return Diagnostic{
		Code:      code,
		Message:   fmt.Sprintf("invalid YAML: %v", err),
		Location:  sourceLocation(source, node),
		EntryID:   entryID,
		BindingID: bindingID,
	}
}

func yamlDiagnosticContext(document *yaml.Node, target *yaml.Node) (string, string) {
	entries := fieldValue(documentRoot(document), "entries")
	if entries == nil || entries.Kind != yaml.SequenceNode {
		return "", ""
	}
	for _, entry := range entries.Content {
		if !containsYAMLNode(entry, target) {
			continue
		}
		entryID := scalarFieldValue(entry, "id")
		bindings := fieldValue(entry, "bindings")
		if bindings != nil && bindings.Kind == yaml.SequenceNode {
			for _, binding := range bindings.Content {
				if containsYAMLNode(binding, target) {
					return entryID, scalarFieldValue(binding, "bindingId")
				}
			}
		}
		return entryID, ""
	}
	return "", ""
}

func scalarFieldValue(mapping *yaml.Node, name string) string {
	value := fieldValue(mapping, name)
	if value == nil || value.Kind != yaml.ScalarNode {
		return ""
	}
	return value.Value
}

func containsYAMLNode(root *yaml.Node, target *yaml.Node) bool {
	if root == nil || target == nil {
		return false
	}
	if root == target {
		return true
	}
	for _, child := range root.Content {
		if containsYAMLNode(child, target) {
			return true
		}
	}
	return false
}

var (
	yamlUnknownFieldPattern = regexp.MustCompile("line ([0-9]+): field ([^ ]+) not found")
	yamlLinePattern         = regexp.MustCompile("line ([0-9]+)")
	deviationIndexPattern   = regexp.MustCompile(`deviation\[([0-9]+)\]`)
)

func yamlErrorNode(document *yaml.Node, message string) *yaml.Node {
	if matches := yamlUnknownFieldPattern.FindStringSubmatch(message); len(matches) == 3 {
		line, _ := strconv.Atoi(matches[1])
		if node := keyOccurrence(document, matches[2], line); node != nil {
			return node
		}
	}
	line := 0
	if matches := yamlLinePattern.FindStringSubmatch(message); len(matches) == 2 {
		line, _ = strconv.Atoi(matches[1])
	}
	if line > 0 {
		if node := nodeAtLine(document, line); node != nil {
			return node
		}
		return &yaml.Node{Line: line, Column: 1}
	}
	return documentRoot(document)
}

func locateModelError(err error, source string, node *yaml.Node, entryID string, bindingID string) error {
	var diagnosticValue Diagnostic
	if errors.As(err, &diagnosticValue) {
		diagnosticValue.Location = sourceLocation(source, node)
		diagnosticValue.EntryID = entryID
		diagnosticValue.BindingID = bindingID
		return diagnosticValue
	}
	return Diagnostic{
		Code:      DiagnosticSchema,
		Message:   err.Error(),
		Location:  sourceLocation(source, node),
		EntryID:   entryID,
		BindingID: bindingID,
	}
}

func newLocatedDiagnostic(code string, source string, node *yaml.Node, message string) error {
	return Diagnostic{
		Code:     code,
		Message:  message,
		Location: sourceLocation(source, node),
	}
}

func sourceLocation(source string, node *yaml.Node) SourceLocation {
	location := SourceLocation{Source: source, Line: 1, Column: 1}
	if node != nil {
		if node.Line > 0 {
			location.Line = node.Line
		}
		if node.Column > 0 {
			location.Column = node.Column
		}
	}
	return location
}

func readerName(reader io.Reader, fallback string) string {
	if named, ok := reader.(interface{ Name() string }); ok {
		if name := named.Name(); name != "" {
			return name
		}
	}
	return fallback
}

func documentRoot(document *yaml.Node) *yaml.Node {
	if document == nil {
		return nil
	}
	if document.Kind == yaml.DocumentNode {
		if len(document.Content) == 0 {
			return nil
		}
		return document.Content[0]
	}
	return document
}

func fieldValue(mapping *yaml.Node, name string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == name {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func fieldValueOrParent(mapping *yaml.Node, name string) *yaml.Node {
	if node := fieldValue(mapping, name); node != nil {
		return node
	}
	return mapping
}

func sequenceItem(sequence *yaml.Node, index int) *yaml.Node {
	if sequence == nil || sequence.Kind != yaml.SequenceNode || index < 0 || index >= len(sequence.Content) {
		return sequence
	}
	return sequence.Content[index]
}

func keyOccurrence(node *yaml.Node, key string, line int) *yaml.Node {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			if keyNode.Value == key && (line == 0 || keyNode.Line == line) {
				return keyNode
			}
		}
	}
	for _, child := range node.Content {
		if found := keyOccurrence(child, key, line); found != nil {
			return found
		}
	}
	return nil
}

func scalarOccurrence(node *yaml.Node, value string, occurrence int) *yaml.Node {
	if node == nil || occurrence <= 0 {
		return nil
	}
	count := 0
	var visit func(*yaml.Node) *yaml.Node
	visit = func(current *yaml.Node) *yaml.Node {
		if current.Kind == yaml.ScalarNode && current.Value == value {
			count++
			if count == occurrence {
				return current
			}
		}
		for _, child := range current.Content {
			if found := visit(child); found != nil {
				return found
			}
		}
		return nil
	}
	return visit(node)
}

func nodeAtLine(node *yaml.Node, line int) *yaml.Node {
	if node == nil {
		return nil
	}
	if node.Line == line {
		return node
	}
	for _, child := range node.Content {
		if found := nodeAtLine(child, line); found != nil {
			return found
		}
	}
	return nil
}
