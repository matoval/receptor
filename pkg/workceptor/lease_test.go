//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ansible/receptor/pkg/logger"
)

func TestNewLeaseManager(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	t.Run("with default config", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		if lm == nil {
			t.Fatal("Expected lease manager, got nil")
		}
		if lm.maxRunningJobs != 10 {
			t.Errorf("Expected maxRunningJobs=10, got %d", lm.maxRunningJobs)
		}
		if lm.maxOutstandingLeases != 10 {
			t.Errorf("Expected maxOutstandingLeases=10, got %d", lm.maxOutstandingLeases)
		}
		if lm.leaseTTLMs != 10000 {
			t.Errorf("Expected leaseTTLMs=10000, got %d", lm.leaseTTLMs)
		}
		lm.Shutdown()
	})

	t.Run("with custom config", func(t *testing.T) {
		config := &LeaseManagerConfig{
			MaxRunningJobs:       5,
			MaxOutstandingLeases: 3,
			LeaseTTLMs:          5000,
			Draining:            true,
		}
		lm := NewLeaseManager(config, logger)
		if lm.maxRunningJobs != 5 {
			t.Errorf("Expected maxRunningJobs=5, got %d", lm.maxRunningJobs)
		}
		if lm.maxOutstandingLeases != 3 {
			t.Errorf("Expected maxOutstandingLeases=3, got %d", lm.maxOutstandingLeases)
		}
		if lm.leaseTTLMs != 5000 {
			t.Errorf("Expected leaseTTLMs=5000, got %d", lm.leaseTTLMs)
		}
		if !lm.draining {
			t.Error("Expected draining=true")
		}
		lm.Shutdown()
	})

	t.Run("maxOutstandingLeases defaults to maxRunningJobs", func(t *testing.T) {
		config := &LeaseManagerConfig{
			MaxRunningJobs:       7,
			MaxOutstandingLeases: 0, // 0 means use maxRunningJobs
		}
		lm := NewLeaseManager(config, logger)
		if lm.maxOutstandingLeases != 7 {
			t.Errorf("Expected maxOutstandingLeases=7, got %d", lm.maxOutstandingLeases)
		}
		lm.Shutdown()
	})
}

func TestLeaseBasicLifecycle(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       2,
		MaxOutstandingLeases: 2,
		LeaseTTLMs:          1000, // 1 second for testing
		Draining:            false,
	}
	lm := NewLeaseManager(config, logger)
	defer lm.Shutdown()

	jobID := "test-job-1"

	t.Run("request lease successfully", func(t *testing.T) {
		lease, err := lm.RequestLease(jobID, 1000)
		if err != nil {
			t.Fatalf("Expected successful lease request, got error: %v", err)
		}
		if lease.JobID != jobID {
			t.Errorf("Expected JobID=%s, got %s", jobID, lease.JobID)
		}
		if lease.Token == "" {
			t.Error("Expected non-empty token")
		}
		if lease.Consumed {
			t.Error("Expected lease to be unconsumed")
		}
		if lease.ExpiresAt.IsZero() {
			t.Error("Expected non-zero expiration time")
		}
		if lease.IsExpired() {
			t.Error("Expected lease to not be expired")
		}
		if !lease.CanBeConsumed() {
			t.Error("Expected lease to be consumable")
		}
	})

	t.Run("consume lease successfully", func(t *testing.T) {
		lease, err := lm.RequestLease("test-job-2", 1000)
		if err != nil {
			t.Fatalf("Failed to request lease: %v", err)
		}

		err = lm.ValidateAndConsumeLease("test-job-2", lease.Token)
		if err != nil {
			t.Errorf("Expected successful lease consumption, got error: %v", err)
		}

		// Check that lease is now consumed
		if !lease.Consumed {
			t.Error("Expected lease to be consumed after validation")
		}
	})
}

func TestLeaseTTLExpiry(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       2,
		MaxOutstandingLeases: 2,
		LeaseTTLMs:          100, // 100ms for fast testing
		Draining:            false,
	}
	lm := NewLeaseManager(config, logger)
	defer lm.Shutdown()

	jobID := "test-job-ttl"

	// Request a lease
	lease, err := lm.RequestLease(jobID, 100)
	if err != nil {
		t.Fatalf("Failed to request lease: %v", err)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Try to consume expired lease - should fail with LEASE_EXPIRED
	err = lm.ValidateAndConsumeLease(jobID, lease.Token)
	if err == nil {
		t.Fatal("Expected lease consumption to fail due to expiration")
	}
	if err.Error() != "LEASE_EXPIRED" {
		t.Errorf("Expected LEASE_EXPIRED error, got: %s", err.Error())
	}
}

func TestLeaseSingleUse(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	lm := NewLeaseManager(nil, logger)
	defer lm.Shutdown()

	jobID := "test-job-single-use"

	// Request and consume lease
	lease, err := lm.RequestLease(jobID, 5000)
	if err != nil {
		t.Fatalf("Failed to request lease: %v", err)
	}

	err = lm.ValidateAndConsumeLease(jobID, lease.Token)
	if err != nil {
		t.Fatalf("Failed to consume lease: %v", err)
	}

	// Try to consume same lease again - should fail with LEASE_USED
	err = lm.ValidateAndConsumeLease(jobID, lease.Token)
	if err == nil {
		t.Fatal("Expected second lease consumption to fail")
	}
	if err.Error() != "LEASE_USED" {
		t.Errorf("Expected LEASE_USED error, got: %s", err.Error())
	}
}

func TestLeaseIdempotentRequests(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	lm := NewLeaseManager(nil, logger)
	defer lm.Shutdown()

	jobID := "test-job-idempotent"

	// First request
	lease1, err := lm.RequestLease(jobID, 5000)
	if err != nil {
		t.Fatalf("Failed to request first lease: %v", err)
	}

	// Second request for same job ID - should return same lease
	lease2, err := lm.RequestLease(jobID, 5000)
	if err != nil {
		t.Fatalf("Failed to request second lease: %v", err)
	}

	if lease1.Token != lease2.Token {
		t.Errorf("Expected same token for idempotent request: %s vs %s", lease1.Token, lease2.Token)
	}
	if lease1.JobID != lease2.JobID {
		t.Errorf("Expected same JobID: %s vs %s", lease1.JobID, lease2.JobID)
	}
	if lease1.ExpiresAt != lease2.ExpiresAt {
		t.Errorf("Expected same expiration time: %v vs %v", lease1.ExpiresAt, lease2.ExpiresAt)
	}

	// After consumption, new request should get new lease
	err = lm.ValidateAndConsumeLease(jobID, lease1.Token)
	if err != nil {
		t.Fatalf("Failed to consume lease: %v", err)
	}

	lease3, err := lm.RequestLease(jobID, 5000)
	if err != nil {
		t.Fatalf("Failed to request third lease: %v", err)
	}

	if lease3.Token == lease1.Token {
		t.Error("Expected different token after consumption")
	}
}

func TestLeaseCapacityLimits(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       2,
		MaxOutstandingLeases: 2,
		LeaseTTLMs:          5000,
		Draining:            false,
	}
	lm := NewLeaseManager(config, logger)
	defer lm.Shutdown()

	// Simulate 1 running job
	lm.runningJobs = 1

	t.Run("can request when under capacity", func(t *testing.T) {
		lease, err := lm.RequestLease("job1", 5000)
		if err != nil {
			t.Errorf("Expected lease to be granted when under capacity, got error: %v", err)
		}
		if lease == nil {
			t.Fatal("Expected lease to be granted")
		}
		// Now we have 1 running + 1 outstanding = 2 (at capacity)
	})

	t.Run("deny when at capacity", func(t *testing.T) {
		_, err := lm.RequestLease("job2", 5000)
		if err == nil {
			t.Fatal("Expected lease to be denied when at capacity")
		}
		if err.Error() != "BUSY" {
			t.Errorf("Expected BUSY error, got: %s", err.Error())
		}
	})

	t.Run("capacity available after consumption", func(t *testing.T) {
		// Consume the outstanding lease (this decrements outstanding count)
		lease, _ := lm.leasesByJobID["job1"]
		err := lm.ValidateAndConsumeLease("job1", lease.Token)
		if err != nil {
			t.Fatalf("Failed to consume lease: %v", err)
		}

		// Now we have capacity for new lease
		lease2, err := lm.RequestLease("job3", 5000)
		if err != nil {
			t.Errorf("Expected lease to be granted after consumption, got error: %v", err)
		}
		if lease2 == nil {
			t.Error("Expected lease to be granted")
		}
	})
}

func TestLeaseDrainingMode(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       2,
		MaxOutstandingLeases: 2,
		LeaseTTLMs:          5000,
		Draining:            true, // Start in draining mode
	}
	lm := NewLeaseManager(config, logger)
	defer lm.Shutdown()

	t.Run("deny lease when draining", func(t *testing.T) {
		_, err := lm.RequestLease("job1", 5000)
		if err == nil {
			t.Fatal("Expected lease to be denied when draining")
		}
		if err.Error() != "DRAINING" {
			t.Errorf("Expected DRAINING error, got: %s", err.Error())
		}
	})

	t.Run("allow lease after disabling draining", func(t *testing.T) {
		lm.SetDraining(false)
		lease, err := lm.RequestLease("job2", 5000)
		if err != nil {
			t.Errorf("Expected lease to be granted after disabling draining, got error: %v", err)
		}
		if lease == nil {
			t.Error("Expected lease to be granted")
		}
	})

	t.Run("deny after re-enabling draining", func(t *testing.T) {
		lm.SetDraining(true)
		_, err := lm.RequestLease("job3", 5000)
		if err == nil {
			t.Fatal("Expected lease to be denied when draining re-enabled")
		}
		if err.Error() != "DRAINING" {
			t.Errorf("Expected DRAINING error, got: %s", err.Error())
		}
	})
}

func TestLeaseValidationErrors(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	lm := NewLeaseManager(nil, logger)
	defer lm.Shutdown()

	t.Run("empty token", func(t *testing.T) {
		err := lm.ValidateAndConsumeLease("job1", "")
		if err == nil || err.Error() != "LEASE_INVALID" {
			t.Errorf("Expected LEASE_INVALID for empty token, got: %v", err)
		}
	})

	t.Run("nonexistent token", func(t *testing.T) {
		err := lm.ValidateAndConsumeLease("job1", "nonexistent-token")
		if err == nil || err.Error() != "LEASE_INVALID" {
			t.Errorf("Expected LEASE_INVALID for nonexistent token, got: %v", err)
		}
	})

	t.Run("job ID mismatch", func(t *testing.T) {
		lease, err := lm.RequestLease("job1", 5000)
		if err != nil {
			t.Fatalf("Failed to request lease: %v", err)
		}

		// Try to validate with different job ID
		err = lm.ValidateAndConsumeLease("job2", lease.Token)
		if err == nil || err.Error() != "LEASE_INVALID" {
			t.Errorf("Expected LEASE_INVALID for job ID mismatch, got: %v", err)
		}
	})
}

func TestRunningJobsTracking(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	lm := NewLeaseManager(nil, logger)
	defer lm.Shutdown()

	// Test increment/decrement
	lm.IncrementRunningJobs()
	running, _, _, _ := lm.GetStats()
	if running != 1 {
		t.Errorf("Expected running jobs=1, got %d", running)
	}

	lm.IncrementRunningJobs()
	running, _, _, _ = lm.GetStats()
	if running != 2 {
		t.Errorf("Expected running jobs=2, got %d", running)
	}

	lm.DecrementRunningJobs()
	running, _, _, _ = lm.GetStats()
	if running != 1 {
		t.Errorf("Expected running jobs=1 after decrement, got %d", running)
	}

	// Test that decrement doesn't go below 0
	lm.DecrementRunningJobs()
	lm.DecrementRunningJobs() // Should not go below 0
	running, _, _, _ = lm.GetStats()
	if running != 0 {
		t.Errorf("Expected running jobs=0 (not negative), got %d", running)
	}
}

func TestConcurrentLeaseAccess(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       50,
		MaxOutstandingLeases: 50,
		LeaseTTLMs:          5000,
		Draining:            false,
	}
	lm := NewLeaseManager(config, logger)
	defer lm.Shutdown()

	// Test concurrent lease requests - should not exceed limits
	const numGoroutines = 10
	const leasesPerGoroutine = 5

	leases := make(chan *Lease, numGoroutines*leasesPerGoroutine)
	errors := make(chan error, numGoroutines*leasesPerGoroutine)

	// Use sync.WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineNum int) {
			defer wg.Done()
			for j := 0; j < leasesPerGoroutine; j++ {
				jobID := fmt.Sprintf("job-%d-%d", routineNum, j)
				lease, err := lm.RequestLease(jobID, 5000)
				if err != nil {
					errors <- err
				} else {
					leases <- lease
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete, then close channels
	wg.Wait()
	close(leases)
	close(errors)

	totalLeases := 0
	for range leases {
		totalLeases++
	}

	totalErrors := 0
	for range errors {
		totalErrors++
	}

	if totalLeases+totalErrors != numGoroutines*leasesPerGoroutine {
		t.Errorf("Expected %d total operations, got %d leases + %d errors",
			numGoroutines*leasesPerGoroutine, totalLeases, totalErrors)
	}

	// All requests should succeed since we have capacity
	if totalErrors > 0 {
		t.Errorf("Expected no errors with sufficient capacity, got %d errors", totalErrors)
	}
}