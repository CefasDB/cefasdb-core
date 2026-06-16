# SQL Analytical Smoke

These examples document the V1 analytical SQL forms supported through
`cefas execute-statement`.

```bash
cefas execute-statement \
  --statement "SELECT id FROM Users ALLOW SCAN WHERE status = 'active' ORDER BY score DESC LIMIT 10"
```

```bash
cefas execute-statement \
  --statement "SELECT status, COUNT(*) FROM Users ALLOW SCAN GROUP BY status"
```

```bash
cefas execute-statement \
  --statement "SELECT u.id, o.order_id FROM Users u INNER JOIN Orders o ON u.id = o.user_id ALLOW SCAN LIMIT 25"
```

Expected guardrails:

```bash
cefas execute-statement \
  --statement "SELECT id FROM Users WHERE status = 'active' LIMIT 10"
```

The query above is rejected because non-key analytical reads require
`ALLOW SCAN`.

```bash
cefas execute-statement \
  --statement "SELECT u.id FROM Users u INNER JOIN Orders o ON u.id <> o.user_id ALLOW SCAN LIMIT 10"
```

The query above is rejected because V1 joins support equality only.
