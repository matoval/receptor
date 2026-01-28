//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/ansible/receptor/pkg/logger"
	"github.com/ansible/receptor/pkg/netceptor"
)

func TestValidateLeaseForSubmit(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	t.Run("no validation when enforcement disabled", func(t *testing.T) {
		// Create workceptor without lease manager (enforcement disabled)
		w := &Workceptor{
			leaseEnforcementEnabled: false,
			leaseManager:            nil,
			nc:                      &mockNetceptor{logger: logger},
		}

		err := w.ValidateLeaseForSubmit("job1", "")
		if err != nil {
			t.Errorf("Expected no validation when enforcement disabled, got error: %v", err)
		}

		err = w.ValidateLeaseForSubmit("job1", "any-token")
		if err != nil {
			t.Errorf("Expected no validation when enforcement disabled, got error: %v", err)
		}
	})

	t.Run("LEASE_REQUIRED when no token provided", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseEnforcementEnabled: true,
			leaseManager:            lm,
			nc:                      &mockNetceptor{logger: logger},
		}

		err := w.ValidateLeaseForSubmit("job1", "")
		if err == nil {
			t.Fatal("Expected error when no token provided")
		}
		if err != ErrLeaseRequired {
			t.Errorf("Expected ErrLeaseRequired, got %v", err)
		}
	})

	t.Run("LEASE_INVALID for nonexistent token", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseEnforcementEnabled: true,
			leaseManager:            lm,
			nc:                      &mockNetceptor{logger: logger},
		}

		err := w.ValidateLeaseForSubmit("job1", "nonexistent-token")
		if err == nil {
			t.Fatal("Expected error for nonexistent token")
		}
		if err != ErrLeaseInvalid {
			t.Errorf("Expected ErrLeaseInvalid, got %v", err)
		}
	})

	t.Run("LEASE_EXPIRED for expired token", func(t *testing.T) {
		config := &LeaseManagerConfig{
			MaxRunningJobs:       10,
			MaxOutstandingLeases: 10,
			LeaseTTLMs:          1, // 1ms for immediate expiration
			Draining:            false,
		}
		lm := NewLeaseManager(config, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseEnforcementEnabled: true,
			leaseManager:            lm,
			nc:                      &mockNetceptor{logger: logger},
		}

		// Create a lease that will expire immediately
		lease, err := lm.RequestLease("job1", 1)
		if err != nil {
			t.Fatalf("Failed to request lease: %v", err)
		}

		// Wait for expiration (small delay)
		time.Sleep(5 * time.Millisecond)

		err = w.ValidateLeaseForSubmit("job1", lease.Token)
		if err == nil {
			t.Fatal("Expected error for expired token")
		}
		if err != ErrLeaseExpired {
			t.Errorf("Expected ErrLeaseExpired, got %v", err)
		}
	})

	t.Run("LEASE_USED for already consumed token", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseEnforcementEnabled: true,
			leaseManager:            lm,
			nc:                      &mockNetceptor{logger: logger},
		}

		// Create and consume a lease
		lease, err := lm.RequestLease("job1", 10000)
		if err != nil {
			t.Fatalf("Failed to request lease: %v", err)
		}

		err = lm.ValidateAndConsumeLease("job1", lease.Token)
		if err != nil {
			t.Fatalf("Failed to consume lease: %v", err)
		}

		// Try to use the consumed token
		err = w.ValidateLeaseForSubmit("job1", lease.Token)
		if err == nil {
			t.Fatal("Expected error for consumed token")
		}
		if err != ErrLeaseUsed {
			t.Errorf("Expected ErrLeaseUsed, got %v", err)
		}
	})

	t.Run("LEASE_INVALID for job ID mismatch", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseEnforcementEnabled: true,
			leaseManager:            lm,
			nc:                      &mockNetceptor{logger: logger},
		}

		// Create lease for job1
		lease, err := lm.RequestLease("job1", 10000)
		if err != nil {
			t.Fatalf("Failed to request lease: %v", err)
		}

		// Try to use token for different job
		err = w.ValidateLeaseForSubmit("job2", lease.Token)
		if err == nil {
			t.Fatal("Expected error for job ID mismatch")
		}
		if err != ErrLeaseInvalid {
			t.Errorf("Expected ErrLeaseInvalid, got %v", err)
		}
	})

	t.Run("success with valid token", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseEnforcementEnabled: true,
			leaseManager:            lm,
			nc:                      &mockNetceptor{logger: logger},
		}

		// Create a lease
		lease, err := lm.RequestLease("job1", 10000)
		if err != nil {
			t.Fatalf("Failed to request lease: %v", err)
		}

		// Should validate successfully
		err = w.ValidateLeaseForSubmit("job1", lease.Token)
		if err != nil {
			t.Errorf("Expected successful validation, got error: %v", err)
		}

		// Token should now be consumed
		if !lease.Consumed {
			t.Error("Expected lease to be consumed after successful validation")
		}
	})
}

func TestRunningJobsIntegration(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	t.Run("increment and decrement running jobs", func(t *testing.T) {
		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		w := &Workceptor{
			leaseManager: lm,
			nc:           &mockNetceptor{logger: logger},
		}

		// Initial count should be 0
		running, _, _, _ := lm.GetStats()
		if running != 0 {
			t.Errorf("Expected initial running jobs=0, got %d", running)
		}

		// Increment
		w.IncrementRunningJobs()
		running, _, _, _ = lm.GetStats()
		if running != 1 {
			t.Errorf("Expected running jobs=1 after increment, got %d", running)
		}

		// Decrement
		w.DecrementRunningJobs()
		running, _, _, _ = lm.GetStats()
		if running != 0 {
			t.Errorf("Expected running jobs=0 after decrement, got %d", running)
		}
	})

	t.Run("no effect when lease manager not set", func(t *testing.T) {
		w := &Workceptor{
			leaseManager: nil,
			nc:           &mockNetceptor{logger: logger},
		}

		// Should not panic when no lease manager
		w.IncrementRunningJobs()
		w.DecrementRunningJobs()
	})
}

func TestLeaseManagerIntegration(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	t.Run("set and get lease manager", func(t *testing.T) {
		w := &Workceptor{
			nc: &mockNetceptor{logger: logger},
		}

		lm := NewLeaseManager(nil, logger)
		defer lm.Shutdown()

		// Initially no lease manager
		if w.GetLeaseManager() != nil {
			t.Error("Expected no initial lease manager")
		}
		if w.IsLeaseEnforcementEnabled() {
			t.Error("Expected lease enforcement to be disabled initially")
		}

		// Set lease manager
		w.SetLeaseManager(lm)

		// Should now have lease manager and enforcement enabled
		if w.GetLeaseManager() != lm {
			t.Error("Expected lease manager to be set")
		}
		if !w.IsLeaseEnforcementEnabled() {
			t.Error("Expected lease enforcement to be enabled after setting manager")
		}

		// Clear lease manager
		w.SetLeaseManager(nil)

		// Should disable enforcement
		if w.GetLeaseManager() != nil {
			t.Error("Expected lease manager to be cleared")
		}
		if w.IsLeaseEnforcementEnabled() {
			t.Error("Expected lease enforcement to be disabled after clearing manager")
		}
	})
}

func TestLeaseErrorConstants(t *testing.T) {
	// Verify error constants match spec requirements
	if ErrLeaseRequired.Error() != "LEASE_REQUIRED" {
		t.Errorf("Expected ErrLeaseRequired='LEASE_REQUIRED', got '%s'", ErrLeaseRequired.Error())
	}
	if ErrLeaseInvalid.Error() != "LEASE_INVALID" {
		t.Errorf("Expected ErrLeaseInvalid='LEASE_INVALID', got '%s'", ErrLeaseInvalid.Error())
	}
	if ErrLeaseExpired.Error() != "LEASE_EXPIRED" {
		t.Errorf("Expected ErrLeaseExpired='LEASE_EXPIRED', got '%s'", ErrLeaseExpired.Error())
	}
	if ErrLeaseUsed.Error() != "LEASE_USED" {
		t.Errorf("Expected ErrLeaseUsed='LEASE_USED', got '%s'", ErrLeaseUsed.Error())
	}
}

// Mock netceptor for testing
type mockNetceptor struct {
	logger *logger.ReceptorLogger
}

func (mn *mockNetceptor) NodeID() string {
	return "test-node"
}

func (mn *mockNetceptor) AddWorkCommand(typeName string, verifySignature bool) error {
	return nil
}

func (mn *mockNetceptor) GetClientTLSConfig(name string, expectedHostName string, expectedHostNameType netceptor.ExpectedHostnameType) (*tls.Config, error) {
	return nil, nil
}

func (mn *mockNetceptor) GetLogger() *logger.ReceptorLogger {
	return mn.logger
}

func (mn *mockNetceptor) DialContext(ctx context.Context, node string, service string, tlscfg *tls.Config) (*netceptor.Conn, error) {
	return nil, nil
}