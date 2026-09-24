# Simulator ↔ billing-service realtime

The fleet simulator is a usage source: every accepted delivery counts toward
metered billing.

- `internal/billing.ConfigFromEnv()` reads `BILLING_URL`, `BILLING_API_KEY`,
  `BILLING_ORG_ID` (empty URL = disabled, local runs unaffected).
- `main.go` increments a counter on every accepted event/batch and flushes it
  every 60s with `POST /v1/usage` (`Idempotency-Key: org:sim:YYYYMMDDHH:MM`).
- Failures are logged and re-queued for the next tick; they never stop the fleet.

Run with billing:

```bash
BILLING_URL=http://localhost:8081 BILLING_API_KEY=<key> BILLING_ORG_ID=<org-uuid> go run .
```
