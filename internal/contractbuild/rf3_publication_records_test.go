package contractbuild

const (
	publishedRF3ReviewManifestLocator   = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/manifest.json"
	publishedRF3DecisionPolicyLocator   = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json"
	publishedRF3PlatformInputsLocator   = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/input-approval-plan-implementation--1--platform-d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14.json"
	publishedRF3ProductApprovalLocator  = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-product-owner-approval-v1.json"
	publishedRF3DatabaseApprovalLocator = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-database-operator-approval-v1.json"
	publishedRF3PlatformApprovalLocator = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-platform-owner-approval-v1.json"
)

func publishedRF3ProtectedDocuments() map[string][]byte {
	return map[string][]byte{
		publishedRF3ReviewManifestLocator: []byte(`{
  "status": "proposed",
  "proposalSha256": "06e8030e680872c9add4f09e42d83ece4d484614d0f768958fd640ebb9d31898",
  "documents": [
    {
      "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/workload-v1.json",
      "name": "workload-v1.json",
      "sha256": "10520db10f6a8eb77d804845bd2f60fcfb667b2960f9afa463838e0e2ecb6743"
    },
    {
      "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/topology-v1.json",
      "name": "topology-v1.json",
      "sha256": "7b808cb2a20017543f0ee559b93da561e7b0df243951dea2337fdd97f0568824"
    },
    {
      "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/rf3-p1-decision-policy-v1.json",
      "name": "rf3-p1-decision-policy-v1.json",
      "sha256": "18d13a6f2398c3e28324219c997c8f2af950546c0c99d74809423efac79a5784"
    },
    {
      "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/rf3-p1-product-owner-approval-v1.json",
      "name": "rf3-p1-product-owner-approval-v1.json",
      "sha256": "766d53b98fe951df80ceeb1da77af9170c1f0b3265dd4907d6a608fd37dc388c"
    },
    {
      "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/rf3-p1-database-operator-approval-v1.json",
      "name": "rf3-p1-database-operator-approval-v1.json",
      "sha256": "3ae2728d2604866d3cd0a4ad4a6114d6a371af573e1a4451b0b949c86538921d"
    },
    {
      "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/rf3-p1-platform-owner-approval-v1.json",
      "name": "rf3-p1-platform-owner-approval-v1.json",
      "sha256": "6280db3d715898fd78bf77d5fe29606ac0dfcb1ad66a6450438d5d9212ece9b2"
    }
  ],
  "decisions": [
    "Product owner: approve or reject workload-and-serving-targets, variance-tolerance, headroom-target, and schema-v1-applicability against both frozen profile IDs and digests plus decision policy 57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049.",
    "Database operator: approve or reject execution-controls, scenario-and-recovery-semantics, variance-method, capacity-search, and schema-v1-applicability against both frozen profile IDs and digests plus decision policy 57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049.",
    "Platform owner: issue the final topology-profile approval for topology-resources, failure-domains, durable-storage-class, and schema-v1-applicability against both frozen profile IDs and digests plus decision policy 57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049; the existing configuration-input approval is not this decision."
  ]
}
`),
		publishedRF3DecisionPolicyLocator: []byte(`{
  "documentType": "rf3-decision-policy",
  "schemaVersion": "1.0",
  "policyId": "rf3.decision-policy.rf3-p1.v1",
  "taskId": "TASK-RF3-P1-APPROVAL",
  "contractId": "rf3.acceptance.rf3-p1.v1",
  "sourceRevision": "e02bebb7671ee70f5a3440fe57782c7202fc3b97",
  "proposal": {
    "path": "codetask/initiatives/INIT-2026-002-rf3-readiness/RF3-P1-PROPOSAL.md",
    "sha256": "df2f815f5833d632c9de75d342255aeb8e17acbf2c371c67031803fbbfb46f09",
    "selectionSha256": "512d93885b19bca44df735ee4b6a1f6e3f17dc171ac5d200d2c62bd075db2169",
    "revision": 11
  },
  "selectedPreparation": {
    "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/inputs-plan-implementation--1--publish.md",
    "sha256": "06e8030e680872c9add4f09e42d83ece4d484614d0f768958fd640ebb9d31898"
  },
  "configurationInputApproval": {
    "path": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/input-approval-plan-implementation--1--platform-d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14.json",
    "sha256": "1a345953d6c7be8adca1b74a45868094188e3dde2a92e19eabb4c6f91a4ce082",
    "scope": "configuration-inputs"
  },
  "profiles": {
    "workload": {
      "profileId": "rf3.workload.synthetic-mixed-one-kops.v1",
      "path": "contracts/rf3-acceptance/workload-v1.json",
      "sha256": "d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66"
    },
    "topology": {
      "profileId": "rf3.topology.three-node-standard.v1",
      "path": "contracts/rf3-acceptance/topology-v1.json",
      "sha256": "33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78"
    }
  },
  "decisions": {
    "workloadAndServingTargets": "proposal-revision-11-exact-values",
    "schemaV1Applicability": "default rule gates healthy-baseline and six one-domain faults; two-node-failure uses safe rejection and shared recovery followed by recovered baseline",
    "variance": {
      "method": "relative-range-ppm",
      "population": "three-complete-runs-at-identical-inputs",
      "aggregation": "per-profile-and-scenario",
      "metrics": [
        "throughput",
        "p99",
        "recovery"
      ],
      "formula": "ceil((max-min)/median*1000000)",
      "minimumRuns": 3,
      "tolerancePpm": 100000,
      "incompleteWhen": [
        "missing-or-incomparable-set",
        "zero-median"
      ]
    },
    "capacity": {
      "method": "bounded-binary-search",
      "peakUnit": "operations-per-second",
      "acceptedPeak": 1000,
      "firstProbe": 3000,
      "maximumFloorRoundedMidpoints": 7,
      "observationWindowMsPerPoint": 300000,
      "saturationSignal": "first-approved-limit-breach",
      "rounding": "down-to-whole-unit",
      "headroomPpm": 300000,
      "headroomFormula": "1-(acceptedPeak/safeCapacity)",
      "requiredRoundedSafeCapacity": 1429,
      "passRule": "tested-passing-low-gte-1429",
      "incompleteWhen": "after-eight-points tested low is below 1429 or the bracket straddles 1429"
    }
  },
  "requiredRoles": [
    "product-owner",
    "database-operator",
    "platform-owner"
  ],
  "approvalTiming": {
    "externalMinimum": "2026-11-22T07:20:41Z",
    "profilesFrozenAt": "2026-11-22T08:21:40Z",
    "effectiveNotBefore": "2026-11-22T08:21:40Z"
  }
}
`),
		publishedRF3PlatformInputsLocator: []byte(`{
  "kind": "configuration-input-approval",
  "runId": "RUN-e345cdf3-13d3-432e-8912-1f37f742825d",
  "taskId": "TASK-RF3-P1-APPROVAL",
  "initiativeId": "INIT-2026-002-rf3-readiness",
  "stepId": "plan-implementation--1--platform",
  "proposalSha256": "257aa946516f75aba311bee44e73c08fb7db0a4dff2888b058d106bd532dc340",
  "manifestPath": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--platform-material/78da327aa7470c9f9379c85eb6b60756a68f39011bec224245e2ff75b249052f/manifest.json",
  "manifestSha256": "d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14",
  "candidatePath": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--platform-material/78da327aa7470c9f9379c85eb6b60756a68f39011bec224245e2ff75b249052f/platform-input-authorization.json",
  "candidateSha256": "c073db4e1d0bfffb6b497e95fd3905b29907171631cd28de3c6371d806fde721",
  "configurationPath": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/planned-design-plan-implementation--1--prepare-e251fecf3c973240503d3f6768b61ae8ecdd3f6bbd937160291d5c90d7d2dd52.md",
  "configurationSha256": "e251fecf3c973240503d3f6768b61ae8ecdd3f6bbd937160291d5c90d7d2dd52",
  "inputSetSha256": "31e4b7682b830f12cac031f5572c8608434ccea0572edf0412f92c69fa0a6ce0",
  "taskScopeSha256": "d28834bf4bb8beb180b35496a1e9e456dd6c3302ebbc14e3e91986557ed48555",
  "proposalRevision": 11,
  "values": {
    "failureDomains": [
      "rf3q-zone-a",
      "rf3q-zone-b",
      "rf3q-zone-c"
    ],
    "storageClass": "cefasdb-rf3-durable-rwo-v1"
  },
  "scope": "configuration-inputs",
  "issuerRole": "platform-owner",
  "issuer": "github:432802 (@osvaldoandrade)",
  "organization": "CefasDB",
  "organizationId": 292745299,
  "authoritySource": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/input-approval-plan-implementation--1--platform-d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14.json#ownerRegistration",
  "ownerRegistration": {
    "scope": "this-input-review",
    "team": "team-cefasdb",
    "assignee": "github:432802 (@osvaldoandrade)",
    "registeredBy": "github:432802 (@osvaldoandrade)",
    "organizationMembership": "https://api.github.com/orgs/CefasDB/memberships/osvaldoandrade",
    "registrySha256": "6279dbcbf580a6b68f9ff628165b015d0aff3dfb0a5cffcab403a6b4bc9c606c"
  },
  "method": "direct",
  "at": "2026-11-22T07:12:17.368Z"
}`),
		publishedRF3ProductApprovalLocator: []byte(`{
  "kind": "rf3-profile-approval",
  "schemaVersion": "1.0",
  "decision": "approve",
  "approver": "github:432802 (@osvaldoandrade)",
  "approvedAt": "2026-11-22T16:09:32.817Z",
  "contractId": "rf3.acceptance.rf3-p1.v1",
  "workloadProfileId": "rf3.workload.synthetic-mixed-one-kops.v1",
  "workloadDigest": "d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66",
  "topologyProfileId": "rf3.topology.three-node-standard.v1",
  "topologyDigest": "33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78",
  "decisionPolicyId": "rf3.decision-policy.rf3-p1.v1",
  "decisionPolicyPath": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json",
  "decisionPolicyDigest": "57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
  "decisionPolicyRecordId": "ledgerdb:codetask_documents/codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json#sha256=57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
  "role": "product-owner",
  "applicableDecisionIds": [
    "workload-and-serving-targets",
    "variance-tolerance",
    "headroom-target",
    "schema-v1-applicability"
  ],
  "runId": "RUN-e345cdf3-13d3-432e-8912-1f37f742825d",
  "taskId": "TASK-RF3-P1-APPROVAL",
  "initiativeId": "INIT-2026-002-rf3-readiness",
  "stepId": "plan-implementation--1--publish",
  "taskScopeSha256": "d28834bf4bb8beb180b35496a1e9e456dd6c3302ebbc14e3e91986557ed48555",
  "manifestSha256": "706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740",
  "proposalSha256": "06e8030e680872c9add4f09e42d83ece4d484614d0f768958fd640ebb9d31898",
  "authoritySource": "codetask/initiatives/INIT-2026-002-rf3-readiness/initiative.yaml",
  "authorityRecord": {
    "issuerId": 432802,
    "organizationId": 292745299,
    "repository": "CefasDB/cefasdb-core",
    "repositoryId": 1264487109,
    "initiativeId": "INIT-2026-002-rf3-readiness",
    "taskId": "TASK-RF3-P1-APPROVAL",
    "registrySha256": "06593f568a26809202dac99ca6d48e0659284bef952fbbac39f79980234786df",
    "distinctPrincipals": false,
    "manualOnly": false,
    "role": "product-owner",
    "team": "team-cefasdb",
    "registration": "direct-final-review-only",
    "organizationMembership": "https://api.github.com/orgs/CefasDB/memberships/osvaldoandrade",
    "organizationTeams": "https://api.github.com/orgs/CefasDB/teams",
    "teamInventorySha256": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
    "method": "direct-role-registration",
    "organization": "CefasDB",
    "scope": "this-final-profile-review",
    "registeredBy": "github:432802 (@osvaldoandrade)",
    "manifestSha256": "706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740"
  }
}
`),
		publishedRF3DatabaseApprovalLocator: []byte(`{
  "kind": "rf3-profile-approval",
  "schemaVersion": "1.0",
  "decision": "approve",
  "approver": "github:432802 (@osvaldoandrade)",
  "approvedAt": "2026-11-22T16:09:32.817Z",
  "contractId": "rf3.acceptance.rf3-p1.v1",
  "workloadProfileId": "rf3.workload.synthetic-mixed-one-kops.v1",
  "workloadDigest": "d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66",
  "topologyProfileId": "rf3.topology.three-node-standard.v1",
  "topologyDigest": "33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78",
  "decisionPolicyId": "rf3.decision-policy.rf3-p1.v1",
  "decisionPolicyPath": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json",
  "decisionPolicyDigest": "57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
  "decisionPolicyRecordId": "ledgerdb:codetask_documents/codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json#sha256=57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
  "role": "database-operator",
  "applicableDecisionIds": [
    "execution-controls",
    "scenario-and-recovery-semantics",
    "variance-method",
    "capacity-search",
    "schema-v1-applicability"
  ],
  "runId": "RUN-e345cdf3-13d3-432e-8912-1f37f742825d",
  "taskId": "TASK-RF3-P1-APPROVAL",
  "initiativeId": "INIT-2026-002-rf3-readiness",
  "stepId": "plan-implementation--1--publish",
  "taskScopeSha256": "d28834bf4bb8beb180b35496a1e9e456dd6c3302ebbc14e3e91986557ed48555",
  "manifestSha256": "706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740",
  "proposalSha256": "06e8030e680872c9add4f09e42d83ece4d484614d0f768958fd640ebb9d31898",
  "authoritySource": "codetask/initiatives/INIT-2026-002-rf3-readiness/initiative.yaml",
  "authorityRecord": {
    "issuerId": 432802,
    "organizationId": 292745299,
    "repository": "CefasDB/cefasdb-core",
    "repositoryId": 1264487109,
    "initiativeId": "INIT-2026-002-rf3-readiness",
    "taskId": "TASK-RF3-P1-APPROVAL",
    "registrySha256": "06593f568a26809202dac99ca6d48e0659284bef952fbbac39f79980234786df",
    "distinctPrincipals": false,
    "manualOnly": false,
    "role": "database-operator",
    "team": "team-cefasdb",
    "registration": "direct-final-review-only",
    "organizationMembership": "https://api.github.com/orgs/CefasDB/memberships/osvaldoandrade",
    "organizationTeams": "https://api.github.com/orgs/CefasDB/teams",
    "teamInventorySha256": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
    "method": "direct-role-registration",
    "organization": "CefasDB",
    "scope": "this-final-profile-review",
    "registeredBy": "github:432802 (@osvaldoandrade)",
    "manifestSha256": "706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740"
  }
}
`),
		publishedRF3PlatformApprovalLocator: []byte(`{
  "kind": "rf3-profile-approval",
  "schemaVersion": "1.0",
  "decision": "approve",
  "approver": "github:432802 (@osvaldoandrade)",
  "approvedAt": "2026-11-22T16:09:32.817Z",
  "contractId": "rf3.acceptance.rf3-p1.v1",
  "workloadProfileId": "rf3.workload.synthetic-mixed-one-kops.v1",
  "workloadDigest": "d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66",
  "topologyProfileId": "rf3.topology.three-node-standard.v1",
  "topologyDigest": "33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78",
  "decisionPolicyId": "rf3.decision-policy.rf3-p1.v1",
  "decisionPolicyPath": "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json",
  "decisionPolicyDigest": "57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
  "decisionPolicyRecordId": "ledgerdb:codetask_documents/codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json#sha256=57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
  "role": "platform-owner",
  "applicableDecisionIds": [
    "topology-resources",
    "failure-domains",
    "durable-storage-class",
    "schema-v1-applicability"
  ],
  "runId": "RUN-e345cdf3-13d3-432e-8912-1f37f742825d",
  "taskId": "TASK-RF3-P1-APPROVAL",
  "initiativeId": "INIT-2026-002-rf3-readiness",
  "stepId": "plan-implementation--1--publish",
  "taskScopeSha256": "d28834bf4bb8beb180b35496a1e9e456dd6c3302ebbc14e3e91986557ed48555",
  "manifestSha256": "706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740",
  "proposalSha256": "06e8030e680872c9add4f09e42d83ece4d484614d0f768958fd640ebb9d31898",
  "authoritySource": "codetask/initiatives/INIT-2026-002-rf3-readiness/initiative.yaml",
  "authorityRecord": {
    "issuerId": 432802,
    "organizationId": 292745299,
    "repository": "CefasDB/cefasdb-core",
    "repositoryId": 1264487109,
    "initiativeId": "INIT-2026-002-rf3-readiness",
    "taskId": "TASK-RF3-P1-APPROVAL",
    "registrySha256": "06593f568a26809202dac99ca6d48e0659284bef952fbbac39f79980234786df",
    "distinctPrincipals": false,
    "manualOnly": false,
    "role": "platform-owner",
    "team": "team-cefasdb",
    "registration": "direct-final-review-only",
    "organizationMembership": "https://api.github.com/orgs/CefasDB/memberships/osvaldoandrade",
    "organizationTeams": "https://api.github.com/orgs/CefasDB/teams",
    "teamInventorySha256": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
    "method": "direct-role-registration",
    "organization": "CefasDB",
    "scope": "this-final-profile-review",
    "registeredBy": "github:432802 (@osvaldoandrade)",
    "manifestSha256": "706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740"
  }
}
`),
	}
}
