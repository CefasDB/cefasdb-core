# CLI command reference

Every `cefas` subcommand with its required flags and one example.
Global flags (`--endpoint`, `--profile`, `--insecure`, `--output`,
`--token`, `--token-file`, `--ca`, `--timeout`) are omitted; they
work on every command.

For deeper docs:

- [index-plugin-examples](index-plugin-examples.md) — every index
  plugin with create-index snippets.
- [distance-examples](distance-examples.md) — every distance operator.
- [geo-audience-workflow](geo-audience-workflow.md) — end-to-end ads
  flow.

## Table management

| Command | Purpose |
|---|---|
| `list-tables` | List every table. |
| `describe-table --table-name T` | Schema + indexes + TTL. |
| `create-table --table-name T --attribute-definitions ... --key-schema ...` | Create a table. |
| `delete-table --table-name T` | Drop a table. |
| `update-time-to-live --table-name T --time-to-live-specification '{"Enabled":true,"AttributeName":"expires_at"}'` | Enable / disable TTL. |
| `describe-time-to-live --table-name T` | Show TTL status. |

```bash
cefas create-table \
  --table-name Users \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
```

## Item CRUD

| Command | Purpose |
|---|---|
| `put-item --table-name T --item '{...}'` | Insert / overwrite. |
| `get-item --table-name T --key '{...}'` | Read by primary key. |
| `update-item --table-name T --key '{...}' --update-expression "SET ..."` | Mutate an existing row. |
| `delete-item --table-name T --key '{...}'` | Remove a row. |

```bash
cefas put-item \
  --table-name Users \
  --item '{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"},"name":{"S":"Ova"}}'
```

## Query, scan, batch, transact, PartiQL

| Command | Purpose |
|---|---|
| `query --table-name T --where "..."` | Partition-key + optional filter. |
| `scan --table-name T [--filter-expression "..."]` | Full-table streaming. |
| `batch-get-item --request-items '{...}'` | Multi-key fetch. |
| `batch-write-item --request-items '{...}'` | Multi-mutation batch. |
| `transact-get-items --transact-items '[...]'` | Atomic multi-item read. |
| `transact-write-items --transact-items '[...]'` | Atomic multi-item write. |
| `execute-statement --statement "..." [--parameters '[...]']` | PartiQL. |

```bash
cefas query --table-name Merchants --where "levenshtein(name, 'habibs') <= 2"
```

## Plugin-backed indexes

| Command | Purpose |
|---|---|
| `create-index --table T --name N --type <plugin> [--field F --config '{...}']` | Create a plugin-backed index. |
| `describe-index --table T --name N` | Show the registered descriptor. |
| `rebuild-index --table T --name N` | Re-seed from current table contents. |

```bash
cefas create-index \
  --table Merchants \
  --name merchant_name_trigram \
  --type trigram \
  --field name
```

## Planner observability

| Command | Purpose |
|---|---|
| `explain --table T --where "..." [--format text\|json]` | Render the plan tree. |
| `top-k --table T --by 'op(field, :bind)' --k K --query '{...}'` | Ranked search via any distance plugin. |

```bash
cefas top-k \
  --table Documents \
  --by "cosine(embedding, :query)" \
  --k 20 \
  --query '{"L":[{"N":"0.1"},{"N":"0.2"},{"N":"0.3"}]}'
```

## Plugins

| Command | Purpose |
|---|---|
| `list-plugins [--kind index\|distance\|estimator\|audience]` | Enumerate registered plugins. |
| `describe-plugin --name N` | One-plugin descriptor + status. |

```bash
cefas list-plugins --kind distance
cefas describe-plugin --name trigram
```

## Cohorts

| Command | Purpose |
|---|---|
| `cohort create --table T --cohort N --field F [--where "..." --binds '{...}']` | Build a Roaring-bitmap cohort. |
| `cohort estimate --table T --field F [--where "..." --binds '{...}']` | HLL cardinality. |

```bash
cefas cohort create \
  --table Users \
  --cohort high_value \
  --field user_id \
  --where "spend >= :floor" \
  --binds '{":floor":{"N":"1000"}}'
```

## Ads / audience

| Command | Purpose |
|---|---|
| `geo audience --table T --center "lat,lon" --radius 1500m [--index N --limit K]` | Geo + Haversine selection. |
| `dedup put --scope S --key K --ttl 7d` | Record a dedup hit. |
| `freqcap check --scope S --key K --limit N --window 7d` | Increment + check cap. |
| `aggregate --table T --group-by a,b --metrics m1,m2 --min-group-size N` | Privacy-aware group-by. |

```bash
cefas geo audience \
  --center "-23.9608,-46.3336" \
  --radius 1500m \
  --active-within 30m
```

## Backups + restore

| Command | Purpose |
|---|---|
| `create-backup --backup-name N [--table-name T ...]` | Admin-named pebble checkpoint. |
| `list-backups` | List every admin-named backup. |
| `restore-table-from-backup --backup-name N --source-table-name S --target-table-name T` | Recreate a table from a backup. |

```bash
cefas create-backup --backup-name nightly --table-name Users
cefas list-backups
cefas restore-table-from-backup \
  --backup-name nightly \
  --source-table-name Users \
  --target-table-name Users_restored
```

## Cluster

| Command | Purpose |
|---|---|
| `cluster status` | Mode, self ID, leader. |
| `cluster add-voter --id N --addr A` | Raft membership add. |
| `cluster remove-server --id N` | Raft membership remove. |

```bash
cefas cluster status
```
