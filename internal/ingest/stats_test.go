package ingest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestStatsFallbackToPostgresAfterRestart verifies that account statistics
// are returned from the durable Postgres aggregate when the in-memory cache
// is empty — as happens after every deployment or process restart.
//
// Before the fix, Stats() read only the in-memory cache (which starts empty
// on every boot), so GET /accounts/{id}/stats always returned zeros until
// new webhooks arrived. The durable totals in Postgres were ignored.
func TestStatsFallbackToPostgresAfterRestart(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Seed Postgres with durable stats as if webhooks had been processed
	// before a deployment/restart.
	if err := st.IncrementAccountStats(ctx, accountID, 60); err != nil {
		t.Fatalf("seed stats: %v", err)
	}
	if err := st.IncrementAccountStats(ctx, accountID, 40); err != nil {
		t.Fatalf("seed stats: %v", err)
	}

	// The in-memory cache is empty (fresh NewCache in testutil.NewServer).
	// Request stats through the HTTP endpoint — should reflect Postgres.
	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	var body struct {
		AccountID        string `json:"account_id"`
		CallCount        int64  `json:"call_count"`
		TotalDurationSec int64  `json:"total_duration_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.CallCount != 2 {
		t.Fatalf("call_count = %d, want 2", body.CallCount)
	}
	if body.TotalDurationSec != 100 {
		t.Fatalf("total_duration_sec = %d, want 100", body.TotalDurationSec)
	}

	// A second request should be served from the now-populated cache
	// (same result, proving the back-fill worked).
	resp2, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get (cached): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	var body2 struct {
		CallCount        int64 `json:"call_count"`
		TotalDurationSec int64 `json:"total_duration_sec"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&body2); err != nil {
		t.Fatalf("decode (cached): %v", err)
	}
	if body2.CallCount != 2 || body2.TotalDurationSec != 100 {
		t.Fatalf("cached response: call_count=%d duration=%d, want 2/100",
			body2.CallCount, body2.TotalDurationSec)
	}
}
