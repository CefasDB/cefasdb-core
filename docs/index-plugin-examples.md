# Index plugin examples

Every shipped index plugin paired with the CLI commands you'd run to
exercise it. Assume `cefas-server` is running and `cefas` points at
it via `--insecure --endpoint 127.0.0.1:9090` (omitted from the
snippets for brevity).

## bloom

Membership filter. False positives configurable; no deletes (use
`cbloom` when delete matters).

```bash
cefas create-index \
  --table Users \
  --name email_bloom \
  --type bloom \
  --config '{"field":"email","m":16384,"k":6}'

cefas describe-index --table Users --name email_bloom
cefas rebuild-index  --table Users --name email_bloom
```

## cbloom (Counting Bloom)

Same surface as Bloom, supports `Delete`.

```bash
cefas create-index \
  --table Sessions \
  --name session_cbloom \
  --type cbloom \
  --config '{"field":"session_id","m":4096,"k":5,"width":4}'
```

## cuckoo

Membership filter with deletes and (typically) a lower false-positive
rate than Bloom at the same size.

```bash
cefas create-index \
  --table Orders \
  --name order_cuckoo \
  --type cuckoo \
  --config '{"field":"order_id","buckets":2048,"fingerprint_bits":12}'
```

## roaring

Cohort index over a numeric attribute. Pairs with `cohort create`.

```bash
cefas cohort create \
  --table Users \
  --cohort high_value \
  --field user_id \
  --where "spend >= :floor" \
  --binds '{":floor":{"N":"1000"}}'
```

## hll (HyperLogLog)

Cardinality estimator. Use via `cohort estimate`.

```bash
cefas cohort estimate --table Events --field user_id
```

## cms (Count-Min Sketch)

Frequency estimator. Server-side; surfaced through
`cohort estimate` with explicit frequency mode in a follow-up.

## radix (prefix tree)

Sorted-prefix index for completion + autocomplete.

```bash
cefas create-index \
  --table Cities --name name_prefix --type radix \
  --config '{"field":"name"}'
```

## trigram

Inverted index over 3-rune shingles. Pairs with
`levenshtein` / `damerau` for fuzzy matching.

```bash
cefas create-index \
  --table Merchants \
  --name merchant_name_trigram \
  --type trigram \
  --field name

cefas query \
  --table-name Merchants \
  --where "levenshtein(name, 'habibs') <= 2"

cefas explain \
  --table Merchants \
  --where "levenshtein(name, 'habibs') <= 2"
```

## minhash

K MinHash signatures + LSH banding over set-valued attributes.
Pairs with `jaccard` for set similarity.

```bash
cefas create-index \
  --table Users --name tag_sim --type minhash \
  --config '{"field":"tags","k":128,"r":8}'
```

## simhash

64-bit SimHash with bucketed Hamming probing for near-duplicate
detection. Pairs with `hamming`.

```bash
cefas create-index \
  --table Docs --name dedupe --type simhash \
  --config '{"field":"body","prefix_bits":16,"max_radius":3}'
```

## vectorlsh

Random-projection LSH for cosine / euclidean nearest-neighbour.
Pairs with `cosine`, `euclidean`, `manhattan` (post-filter).

```bash
cefas create-index \
  --table Documents \
  --name emb_lsh \
  --type vectorlsh \
  --config '{"field":"embedding","dim":768,"sketches":8,"bits_per_sketch":12}'

cefas top-k \
  --table Documents \
  --by "cosine(embedding, :query)" \
  --k 20 \
  --query '{"L":[{"N":"0.12"}, {"N":"-0.04"} /* … 768-dim vector … */ ]}'
```

## geohash

Spatial prefix index over `{lat, lon}` maps. Required by the audience
plugin's `Select` (see [geo audience workflow](geo-audience-workflow.md)).

```bash
cefas create-index \
  --table Stores \
  --name loc_geo \
  --type geohash \
  --config '{"field":"loc","precision":7}'

cefas geo audience \
  --table Stores \
  --center "-23.9608,-46.3336" \
  --radius 1500m
```

## Explain output

`explain` returns the plan tree (text by default; `--format json` for
machine-readable):

```bash
$ cefas explain --table Users --where "levenshtein(name, 'ova') <= 1"
- Query table=Users predicate="levenshtein(name, 'ova') <= 1"
  - ScanTable [plugin=core] Users
```

v1 emits a synthetic plan tree; deeper SQL-planner integration is the
follow-up that lets explain surface `CandidateSet [plugin=trigram] cost=…`
under the top-level Query node.
