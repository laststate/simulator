// Package billing reports simulator throughput to billing-service in realtime.
//
// Env: BILLING_URL, BILLING_API_KEY, BILLING_ORG_ID. Empty URL = disabled.
// Reports are idempotent per hour (key org:sim:YYYYMMDDHH) so restarts do not
// double-bill. Failures are logged and retried on the next tick — they never
// stop the fleet.
package billing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Config for the live link.
type Config struct {
	URL    string
	APIKey string
	OrgID  string
	Client *http.Client
}

// ConfigFromEnv loads the link.
func ConfigFromEnv() Config {
	return Config{
		URL:    strings.TrimRight(strings.TrimSpace(os.Getenv("BILLING_URL")), "/"),
		APIKey: strings.TrimSpace(os.Getenv("BILLING_API_KEY")),
		OrgID:  strings.TrimSpace(os.Getenv("BILLING_ORG_ID")),
		Client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Enabled reports whether reports should be sent.
func (c Config) Enabled() bool { return c.URL != "" && c.APIKey != "" && c.OrgID != "" }

// Report sends one usage delta. key should be stable per period.
func (c Config) Report(ctx context.Context, events int64, key string) error {
	if !c.Enabled() {
		return nil
	}
	body, _ := json.Marshal(map[string]any{
		"organization_id": c.OrgID,
		"metrics":         map[string]int64{"events": events},
		"idempotency_key": key,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/v1/usage", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("billing: usage HTTP %d", resp.StatusCode)
	}
	return nil
}

// HourKey returns the idempotency key for the current hour.
func HourKey(org string) string {
	return org + ":sim:" + time.Now().UTC().Format("2006010215")
}
