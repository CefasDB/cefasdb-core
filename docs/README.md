# CEFAS docs

Operator + integrator documentation for the plugin-architected CEFAS
described by epics #97–#104. Start with [architecture](architecture.md)
for the 30k-foot view, then follow the link that matches what you're
building.

| Doc | When to read |
|---|---|
| [Architecture overview](architecture.md) | First-time orientation — server, storage, planner, plugin registry, request lifecycle. |
| [Core vs plugin boundaries](boundaries.md) | "Where does X live?" Look up any feature and find out whether it's core or plugin. |
| [Plugin authoring guide](plugin-authoring.md) | Writing a new plugin (index, distance, estimator, or audience). |
| [Index plugin examples](index-plugin-examples.md) | Worked examples of every shipped index plugin — bloom, trigram, geohash, … |
| [Distance operator examples](distance-examples.md) | Worked examples of every shipped distance plugin — levenshtein, cosine, haversine, … |
| [Geo audience workflow](geo-audience-workflow.md) | End-to-end ads workflow: campaign → audience → dedup → freqcap → aggregate. |
| [Ads audience privacy model](ads-privacy.md) | The guarantees CEFAS makes about audience data — no raw exports, min-group-size, etc. |
| [CLI command reference](cli-reference.md) | Every `cefas` subcommand with flags + an example. |
