package ingest_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestRecordingProcessedAfterRequestReturns verifies that background
// recording processing completes even though the HTTP request context
// is cancelled when the handler returns.
//
// Before the fix, processRecording used the request context (r.Context()).
// That context was cancelled as soon as the HTTP handler returned the
// response, causing MarkRecordingProcessed to fail silently with
// "context canceled". Recordings were never marked processed.
func TestRecordingProcessedAfterRequestReturns(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// The recording work takes ~50ms (recordingWork constant).
	// Wait enough time for it to complete.
	time.Sleep(200 * time.Millisecond)

	var processed bool
	row := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true; " +
			"background processing likely failed due to request context cancellation")
	}
}
