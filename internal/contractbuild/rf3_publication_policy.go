package contractbuild

import "fmt"

const rf3PartsPerMillion = uint64(1_000_000)

func validateRF3DecisionPolicy(
	binding RF3ProtectedEvidence,
	policy RF3DecisionPolicyV1,
	published RF3PublishedAcceptanceV1,
) error {
	if err := validateRF3DecisionPolicyIdentity(binding, policy, published); err != nil {
		return err
	}
	if err := validateRF3DecisionPolicyProfiles(binding, policy, published); err != nil {
		return err
	}
	if err := validateRF3DecisionPolicyConfiguration(policy, published); err != nil {
		return err
	}
	if err := validateRF3DecisionPolicyTiming(policy.ApprovalTiming); err != nil {
		return err
	}
	if err := validateRF3DecisionPolicyRoles(policy.RequiredRoles); err != nil {
		return err
	}
	if err := validateRF3PolicyDecisionProjection(policy.Decisions, published.Acceptance); err != nil {
		return err
	}
	return nil
}

func validateRF3DecisionPolicyIdentity(
	binding RF3ProtectedEvidence,
	policy RF3DecisionPolicyV1,
	published RF3PublishedAcceptanceV1,
) error {
	expected := binding.Expectation
	if policy.DocumentType != rf3DecisionPolicyDocumentType ||
		policy.SchemaVersion != "1.0" ||
		policy.PolicyID != expected.PolicyID ||
		policy.TaskID != rf3ApprovalTaskID ||
		policy.ContractID != expected.ContractID {
		return fmt.Errorf("decision policy identity does not match publication")
	}
	if policy.PolicyID != published.Publication.Review.DecisionPolicy.PolicyID ||
		policy.ContractID != published.Acceptance.ContractID {
		return fmt.Errorf("decision policy does not match the loaded acceptance root")
	}
	if policy.SourceRevision == "" ||
		policy.Proposal.Path == "" ||
		policy.Proposal.Revision == 0 ||
		policy.SelectedPreparation.Path == "" {
		return fmt.Errorf("decision policy source provenance is incomplete")
	}
	for label, digest := range map[string]string{
		"proposal":             policy.Proposal.SHA256,
		"proposal selection":   policy.Proposal.SelectionSHA256,
		"selected preparation": policy.SelectedPreparation.SHA256,
	} {
		if err := validateRF3RawSHA256(digest); err != nil {
			return fmt.Errorf("decision policy %s digest: %w", label, err)
		}
	}
	return nil
}

func validateRF3DecisionPolicyProfiles(
	binding RF3ProtectedEvidence,
	policy RF3DecisionPolicyV1,
	published RF3PublishedAcceptanceV1,
) error {
	expected := binding.Expectation
	if err := validateRF3DecisionPolicyProfile(
		"workload",
		policy.Profiles.Workload,
		expected.WorkloadProfileID,
		expected.WorkloadDigest,
		published.Acceptance.References.Workload.Path,
	); err != nil {
		return err
	}
	if err := validateRF3DecisionPolicyProfile(
		"topology",
		policy.Profiles.Topology,
		expected.TopologyProfileID,
		expected.TopologyDigest,
		published.Acceptance.References.Topology.Path,
	); err != nil {
		return err
	}
	return nil
}

func validateRF3DecisionPolicyProfile(
	label string,
	profile RF3DecisionPolicyProfileV1,
	expectedID string,
	expectedDigest string,
	referencePath string,
) error {
	digest, err := rf3PrefixedDigest(profile.SHA256)
	if err != nil {
		return fmt.Errorf("decision policy %s digest: %w", label, err)
	}
	expectedPath := "contracts/rf3-acceptance/" + referencePath
	if profile.ProfileID != expectedID ||
		digest != expectedDigest ||
		profile.Path != expectedPath {
		return fmt.Errorf("decision policy %s profile does not match publication", label)
	}
	return nil
}

func validateRF3DecisionPolicyConfiguration(
	policy RF3DecisionPolicyV1,
	published RF3PublishedAcceptanceV1,
) error {
	configuration := policy.ConfigurationInputApproval
	digest, err := rf3PrefixedDigest(configuration.SHA256)
	if err != nil {
		return fmt.Errorf("decision policy configuration digest: %w", err)
	}
	if configuration.Path != published.Publication.PlatformInputs.Evidence ||
		digest != published.Publication.PlatformInputs.Digest ||
		configuration.Scope != rf3ConfigurationInputScope {
		return fmt.Errorf("decision policy configuration input does not match publication")
	}
	return nil
}

func validateRF3DecisionPolicyTiming(timing RF3DecisionPolicyTimingV1) error {
	external, err := parseRF3Timestamp("decision policy external minimum", timing.ExternalMinimum)
	if err != nil {
		return err
	}
	frozen, err := parseRF3Timestamp("decision policy profiles frozen at", timing.ProfilesFrozenAt)
	if err != nil {
		return err
	}
	effective, err := parseRF3Timestamp(
		"decision policy effective not before",
		timing.EffectiveNotBefore,
	)
	if err != nil {
		return err
	}
	if effective.Before(external) || effective.Before(frozen) {
		return fmt.Errorf("decision policy approval timing precedes a required boundary")
	}
	return nil
}

func validateRF3DecisionPolicyRoles(roles []string) error {
	expected := []string{"product-owner", "database-operator", "platform-owner"}
	if !reflectRF3Strings(roles, expected) {
		return fmt.Errorf("decision policy required roles are invalid")
	}
	return nil
}

func validateRF3PolicyDecisionProjection(
	decisions RF3DecisionPolicyDecisionsV1,
	acceptance RF3AcceptanceDocumentV1,
) error {
	if decisions.WorkloadAndServingTargets == "" ||
		decisions.SchemaV1Applicability == "" {
		return fmt.Errorf("decision policy applicability decisions are incomplete")
	}
	variance := decisions.Variance
	if variance.Method != acceptance.Variance.Method ||
		variance.Population != acceptance.Variance.Population ||
		variance.Aggregation != acceptance.Variance.Aggregation ||
		variance.MinimumRuns != acceptance.Variance.MinimumRuns.Value ||
		variance.TolerancePPM != acceptance.Variance.Tolerance.Value {
		return fmt.Errorf("decision policy variance does not match acceptance root")
	}
	if variance.Formula == "" ||
		validateRF3StringSet("variance metrics", variance.Metrics) != nil ||
		validateRF3StringSet("variance incomplete conditions", variance.IncompleteWhen) != nil {
		return fmt.Errorf("decision policy variance procedure is incomplete")
	}
	if err := validateRF3PolicyCapacity(decisions.Capacity, acceptance.CapacityMethod); err != nil {
		return err
	}
	return nil
}

func validateRF3PolicyCapacity(
	capacity RF3DecisionPolicyCapacityV1,
	projection RF3CapacityMethodV1,
) error {
	if capacity.Method != projection.Method ||
		capacity.PeakUnit != projection.PeakUnit ||
		capacity.ObservationWindowMSPerPoint != projection.ObservationWindow.Value ||
		capacity.SaturationSignal != projection.SaturationSignal || capacity.Rounding != projection.Rounding ||
		capacity.HeadroomPPM != projection.Headroom.Value {
		return fmt.Errorf("decision policy capacity method does not match acceptance root")
	}
	if capacity.AcceptedPeak == 0 ||
		capacity.FirstProbe <= capacity.AcceptedPeak ||
		capacity.MaximumFloorRoundedMidpoints == 0 ||
		capacity.HeadroomFormula == "" ||
		capacity.RequiredRoundedSafeCapacity == 0 ||
		capacity.PassRule == "" ||
		capacity.IncompleteWhen == "" {
		return fmt.Errorf("decision policy capacity procedure is incomplete")
	}
	if err := validateRF3RequiredSafeCapacity(capacity); err != nil {
		return err
	}
	return nil
}

func validateRF3RequiredSafeCapacity(capacity RF3DecisionPolicyCapacityV1) error {
	if capacity.HeadroomPPM >= rf3PartsPerMillion {
		return fmt.Errorf("decision policy headroom must be below one million ppm")
	}
	if capacity.AcceptedPeak > ^uint64(0)/rf3PartsPerMillion {
		return fmt.Errorf("decision policy accepted peak overflows capacity calculation")
	}
	denominator := rf3PartsPerMillion - capacity.HeadroomPPM
	numerator := capacity.AcceptedPeak * rf3PartsPerMillion
	if numerator > ^uint64(0)-(denominator-1) {
		return fmt.Errorf("decision policy safe capacity calculation overflows")
	}
	required := (numerator + denominator - 1) / denominator
	if capacity.RequiredRoundedSafeCapacity != required ||
		capacity.FirstProbe < required {
		return fmt.Errorf("decision policy required safe capacity is inconsistent")
	}
	return nil
}
