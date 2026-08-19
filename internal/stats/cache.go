// Package stats keeps a hot-path, in-memory view of per-account call totals.
//
// The durable copy of these numbers lives in Postgres; this cache exists so
// the stats endpoint does not hit the database on every read.
package stats

import "sync"

// AccountStats is a point-in-time view of one account's totals.
type AccountStats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Cache holds per-account running totals.
type Cache struct {
	mu sync.RWMutex
	m  map[string]*AccountStats
}

// NewCache returns an empty cache.
func NewCache() *Cache {
	return &Cache{m: make(map[string]*AccountStats)}
}

// Get returns a snapshot of an account's totals and a boolean indicating
// whether the account was present in the cache. On a miss the caller
// should fall back to the durable store.
func (c *Cache) Get(accountID string) (AccountStats, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.m[accountID]
	if !ok {
		return AccountStats{}, false
	}
	return *s, true
}

// Set populates the cache entry for an account. It is used to back-fill
// the cache after a read-through from Postgres.
func (c *Cache) Set(accountID string, st AccountStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.m[accountID] = &AccountStats{
		CallCount:        st.CallCount,
		TotalDurationSec: st.TotalDurationSec,
	}
}

// Record folds one completed call into an account's running totals.
func (c *Cache) Record(accountID string, durationSec int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, ok := c.m[accountID]
	if !ok {
		s = &AccountStats{}
		c.m[accountID] = s
	}
	s.CallCount++
	s.TotalDurationSec += int64(durationSec)
}
