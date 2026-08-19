// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks in-flight background recording goroutines so that
	// Shutdown can wait for them to finish before the process exits.
	wg sync.WaitGroup
}

// New builds a Service.
func New(
	s *store.Store,
	c *stats.Cache,
	rdb *redis.Client,
	log *slog.Logger,
) *Service {
	return &Service{
		store: s,
		cache: c,
		rdb:   rdb,
		log:   log,
	}
}

// Shutdown waits for all in-flight recording goroutines to finish.
// It is called after the HTTP server stops accepting new requests.
func (s *Service) Shutdown() {
	s.wg.Wait()
}

// Stats returns the totals for an account. It serves from the in-memory
// cache when possible and falls back to the durable Postgres aggregate
// on a cache miss (e.g. after a deployment or process restart).
func (s *Service) Stats(ctx context.Context, accountID string) (stats.AccountStats, error) {
	if st, ok := s.cache.Get(accountID); ok {
		return st, nil
	}

	// Cache miss — read the durable copy from Postgres.
	durable, err := s.store.AccountStats(ctx, accountID)
	if err != nil {
		return stats.AccountStats{}, err
	}

	result := stats.AccountStats{
		CallCount:        durable.CallCount,
		TotalDurationSec: durable.TotalDurationSec,
	}

	// Back-fill the cache so subsequent reads are served from memory.
	if result.CallCount > 0 || result.TotalDurationSec > 0 {
		s.cache.Set(accountID, result)
	}

	return result, nil
}

// Ingest stores a delivery and kicks off processing.
// Processing runs asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// ProcessEvent atomically:
	// 1. inserts the event,
	// 2. upserts the call,
	// 3. updates account statistics.
	//
	// PostgreSQL's UNIQUE(event_id) constraint makes duplicate deliveries
	// safe even when they arrive concurrently.
	inserted, err := s.store.ProcessEvent(ctx, rec)
	if err != nil {
		return err
	}

	// Duplicate delivery. The event was already fully processed.
	if !inserted {
		s.log.Info(
			"duplicate delivery ignored",
			"event_id",
			evt.EventID,
		)
		return nil
	}

	// Update the in-memory aggregate only after the durable transaction
	// succeeds.
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	//
	// The goroutine uses context.Background() instead of the HTTP request
	// ctx, because the request context is cancelled as soon as the handler
	// returns — which is before the recording work finishes.
	//
	// s.wg tracks in-flight goroutines so Shutdown() can wait for them.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.processRecording(context.Background(), rec); err != nil {
				s.log.Error(
					"recording processing failed",
					"event_id", rec.EventID,
					"call_id", rec.CallID,
					"account_id", rec.AccountID,
					"err", err,
				)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(
	ctx context.Context,
	rec store.Event,
) error {
	time.Sleep(recordingWork)

	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
