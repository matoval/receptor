//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ansible/receptor/pkg/logger"
	"github.com/ansible/receptor/pkg/randstr"
)

// Lease represents an in-memory lease token with lifecycle management.
type Lease struct {
	JobID     string    // Job identifier that the lease is for
	Token     string    // Random token string for validation
	ExpiresAt time.Time // When the lease expires
	Consumed  bool      // Whether the lease has been used for a work submission
	CreatedAt time.Time // When the lease was created
}

// IsExpired returns true if the lease has passed its expiration time.
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// CanBeConsumed returns true if the lease is valid and not yet consumed.
func (l *Lease) CanBeConsumed() bool {
	return !l.IsExpired() && !l.Consumed
}

// LeaseManager handles the lifecycle of lease tokens for work submission control.
// It provides thread-safe operations for creating, validating, and consuming leases
// while enforcing capacity limits to provide backpressure.
type LeaseManager struct {
	// Lease storage - protected by mutex
	leases        map[string]*Lease // keyed by token for fast lookup during validation
	leasesByJobID map[string]*Lease // keyed by job_id for idempotent requests
	mutex         sync.RWMutex

	// Configuration
	maxRunningJobs       int  // Maximum concurrent running jobs
	maxOutstandingLeases int  // Maximum outstanding leases (0 = same as maxRunningJobs)
	leaseTTLMs          int  // Default lease TTL in milliseconds
	draining            bool // Whether the worker is draining (not accepting new leases)

	// Capacity tracking
	runningJobs       int // Current count of running jobs
	outstandingLeases int // Current count of outstanding (unconsumed) leases

	// Dependencies
	logger *logger.ReceptorLogger
	ctx    context.Context
	cancel context.CancelFunc
}

// LeaseManagerConfig contains configuration options for the lease manager.
type LeaseManagerConfig struct {
	MaxRunningJobs       int  `yaml:"max_running_jobs"`
	MaxOutstandingLeases int  `yaml:"max_outstanding_leases"` // 0 means same as MaxRunningJobs
	LeaseTTLMs          int  `yaml:"lease_ttl_ms"`
	Draining            bool `yaml:"draining"`
}

// DefaultLeaseManagerConfig returns a configuration with reasonable defaults.
func DefaultLeaseManagerConfig() *LeaseManagerConfig {
	return &LeaseManagerConfig{
		MaxRunningJobs:       10,
		MaxOutstandingLeases: 0, // Will be set to same as MaxRunningJobs
		LeaseTTLMs:          10000, // 10 seconds
		Draining:            false,
	}
}

// NewLeaseManager creates a new lease manager with the given configuration.
func NewLeaseManager(config *LeaseManagerConfig, logger *logger.ReceptorLogger) *LeaseManager {
	if config == nil {
		config = DefaultLeaseManagerConfig()
	}

	// If MaxOutstandingLeases is 0, set it to the same as MaxRunningJobs
	maxOutstandingLeases := config.MaxOutstandingLeases
	if maxOutstandingLeases == 0 {
		maxOutstandingLeases = config.MaxRunningJobs
	}

	ctx, cancel := context.WithCancel(context.Background())

	lm := &LeaseManager{
		leases:               make(map[string]*Lease),
		leasesByJobID:        make(map[string]*Lease),
		maxRunningJobs:       config.MaxRunningJobs,
		maxOutstandingLeases: maxOutstandingLeases,
		leaseTTLMs:          config.LeaseTTLMs,
		draining:            config.Draining,
		runningJobs:         0,
		outstandingLeases:   0,
		logger:              logger,
		ctx:                 ctx,
		cancel:              cancel,
	}

	// Start background cleanup goroutine
	go lm.expiredLeaseCleanupLoop()

	return lm
}

// RequestLease attempts to create a new lease for the given job ID.
// Returns the lease if successful, or an error with specific reason codes:
// - "DRAINING" if the worker is draining
// - "BUSY" if at capacity
// If a valid unconsumed lease already exists for the job ID, it returns
// the existing lease (idempotent behavior).
func (lm *LeaseManager) RequestLease(jobID string, ttlMs int) (*Lease, error) {
	if ttlMs <= 0 {
		ttlMs = lm.leaseTTLMs
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Clean up expired leases first
	lm.cleanupExpiredLeasesLocked()

	// Check if draining
	if lm.draining {
		return nil, fmt.Errorf("DRAINING")
	}

	// Check if we have an existing valid lease for this job ID (idempotent)
	if existingLease, exists := lm.leasesByJobID[jobID]; exists {
		if existingLease.CanBeConsumed() {
			lm.logger.Debug("Returning existing lease for job %s: %s", jobID, existingLease.Token)
			return existingLease, nil
		}
		// Clean up the expired/consumed lease
		lm.removeLeaseLocked(existingLease.Token)
	}

	// Check capacity - running jobs + outstanding leases must be under limit
	if lm.runningJobs+lm.outstandingLeases >= lm.maxRunningJobs {
		lm.logger.Debug("Lease denied for job %s: at capacity (%d running + %d outstanding >= %d max)",
			jobID, lm.runningJobs, lm.outstandingLeases, lm.maxRunningJobs)
		return nil, fmt.Errorf("BUSY")
	}

	// Create new lease
	token := randstr.RandomString(16) // 16 character random token
	expiresAt := time.Now().Add(time.Duration(ttlMs) * time.Millisecond)
	lease := &Lease{
		JobID:     jobID,
		Token:     token,
		ExpiresAt: expiresAt,
		Consumed:  false,
		CreatedAt: time.Now(),
	}

	// Store the lease
	lm.leases[token] = lease
	lm.leasesByJobID[jobID] = lease
	lm.outstandingLeases++

	lm.logger.Debug("Created lease for job %s: token=%s, expires=%v",
		jobID, token, expiresAt)

	return lease, nil
}

// ValidateAndConsumeLease validates a lease token and marks it as consumed atomically.
// Returns nil if successful, or an error with specific reason codes:
// - "LEASE_INVALID" if the token doesn't exist
// - "LEASE_EXPIRED" if the token is expired
// - "LEASE_USED" if the token was already consumed
// - "LEASE_INVALID" if the job ID doesn't match
func (lm *LeaseManager) ValidateAndConsumeLease(jobID, token string) error {
	if token == "" {
		return fmt.Errorf("LEASE_INVALID")
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Find the lease first, before cleaning up expired leases
	lease, exists := lm.leases[token]
	if !exists {
		// Clean up expired leases now that we know this token doesn't exist
		lm.cleanupExpiredLeasesLocked()
		lm.logger.Debug("Lease validation failed for job %s: token %s not found", jobID, token)
		return fmt.Errorf("LEASE_INVALID")
	}

	// Check if expired
	if lease.IsExpired() {
		lm.logger.Debug("Lease validation failed for job %s: token %s expired", jobID, token)
		lm.removeLeaseLocked(token) // Clean up expired lease
		return fmt.Errorf("LEASE_EXPIRED")
	}

	// Check if already consumed
	if lease.Consumed {
		lm.logger.Debug("Lease validation failed for job %s: token %s already used", jobID, token)
		return fmt.Errorf("LEASE_USED")
	}

	// Check job ID match
	if lease.JobID != jobID {
		lm.logger.Debug("Lease validation failed: token %s is for job %s, not %s",
			token, lease.JobID, jobID)
		return fmt.Errorf("LEASE_INVALID")
	}

	// Consume the lease
	lease.Consumed = true
	lm.outstandingLeases--

	lm.logger.Debug("Consumed lease for job %s: token=%s", jobID, token)

	return nil
}

// IncrementRunningJobs increases the running job count.
// This should be called when a work unit starts execution.
func (lm *LeaseManager) IncrementRunningJobs() {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.runningJobs++
	lm.logger.Debug("Incremented running jobs to %d", lm.runningJobs)
}

// DecrementRunningJobs decreases the running job count.
// This should be called when a work unit completes execution.
func (lm *LeaseManager) DecrementRunningJobs() {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	if lm.runningJobs > 0 {
		lm.runningJobs--
	}
	lm.logger.Debug("Decremented running jobs to %d", lm.runningJobs)
}

// ResetRunningJobsCount resets the running jobs count to a specific value.
// This should be called during startup to sync the count with actual job states.
func (lm *LeaseManager) ResetRunningJobsCount(count int) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.runningJobs = count
	lm.logger.Debug("Reset running jobs count to %d", lm.runningJobs)
}

// GetStats returns current lease manager statistics.
func (lm *LeaseManager) GetStats() (runningJobs, outstandingLeases, maxRunning, maxOutstanding int) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()
	return lm.runningJobs, lm.outstandingLeases, lm.maxRunningJobs, lm.maxOutstandingLeases
}

// SetDraining sets the draining state. When draining, no new leases will be granted.
func (lm *LeaseManager) SetDraining(draining bool) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.draining = draining
	lm.logger.Info("Set draining mode to %v", draining)
}

// IsDraining returns the current draining state.
func (lm *LeaseManager) IsDraining() bool {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()
	return lm.draining
}

// Shutdown gracefully shuts down the lease manager, stopping background goroutines.
func (lm *LeaseManager) Shutdown() {
	lm.cancel()
}

// expiredLeaseCleanupLoop runs periodically to clean up expired leases.
func (lm *LeaseManager) expiredLeaseCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lm.mutex.Lock()
			lm.cleanupExpiredLeasesLocked()
			lm.mutex.Unlock()
		case <-lm.ctx.Done():
			lm.logger.Debug("Lease cleanup loop shutting down")
			return
		}
	}
}

// cleanupExpiredLeasesLocked removes expired leases. Must be called with mutex held.
func (lm *LeaseManager) cleanupExpiredLeasesLocked() {
	now := time.Now()
	var expiredTokens []string

	// Find expired leases
	for token, lease := range lm.leases {
		if now.After(lease.ExpiresAt) {
			expiredTokens = append(expiredTokens, token)
		}
	}

	// Remove expired leases
	for _, token := range expiredTokens {
		lease := lm.leases[token]
		lm.removeLeaseLocked(token)
		lm.logger.Debug("Cleaned up expired lease for job %s: token=%s", lease.JobID, token)
	}
}

// removeLeaseLocked removes a lease from all tracking maps. Must be called with mutex held.
func (lm *LeaseManager) removeLeaseLocked(token string) {
	if lease, exists := lm.leases[token]; exists {
		delete(lm.leases, token)
		delete(lm.leasesByJobID, lease.JobID)
		if !lease.Consumed {
			lm.outstandingLeases--
		}
	}
}