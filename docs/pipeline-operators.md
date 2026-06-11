# Pipeline operators

## MMR diversification

Use `LIMIT` as the retrieval fan-out and `DIVERSIFY ... TO` as the final
slate size:

```sql
SELECT id, title
FROM docs
ORDER BY emb ANN OF [0.12, 0.44, 0.90]
LIMIT 100
DIVERSIFY BY mmr(lambda=0.7) TO 10;
```

`lambda=1` preserves relevance order. `lambda=0` maximizes diversity after
the first pick. The distance metric is resolved from the ANN index for the
ordered field, matching the TopK and Rerank RPCs.

## Recommend

`Recommend` composes retrieval, optional SQL filtering, optional MMR, and
dedup/frequency cap stages in one call. Each response includes stage timing
and reason codes.

```bash
cefas recommend \
  --table docs \
  --by "cosine(emb, :q)" \
  --query '{"V":{"values":[0.12,0.44,0.90],"dim":3}}' \
  --candidate-limit 100 \
  --filter "region = 'us'" \
  --lambda 0.7 \
  --limit 10
```

## Next best action

`nba decide` filters disabled actions, chooses among eligible bandit arms,
applies an optional cap, and writes a TTL decision record under the internal
`__decisions__` namespace. `nba reward` can use the decision id so the reward
updates the original selected arm.

```bash
cefas nba decide \
  --bandit-id offers \
  --user-id u123 \
  --actions A,B,C \
  --fallback fallback \
  --decision-ttl-seconds 86400

cefas nba reward \
  --decision-id <decision_id> \
  --reward 1
```
