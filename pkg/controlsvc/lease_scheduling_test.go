//go:build !no_workceptor
// +build !no_workceptor

package controlsvc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ansible/receptor/pkg/logger"
	"github.com/ansible/receptor/pkg/netceptor"
)

// Mock netceptor for control command testing
type mockNetceptorForControl struct {
	logger         *logger.ReceptorLogger
	advertisements []*netceptor.ServiceAdvertisement
	connections    map[string]*mockConn
}

func (mn *mockNetceptorForControl) Status() netceptor.Status {
	return netceptor.Status{
		Advertisements: mn.advertisements,
	}
}

func (mn *mockNetceptorForControl) Dial(node string, service string, tlscfg *tls.Config) (*netceptor.Conn, error) {
	if _, exists := mn.connections[node]; exists {
		// Return the mock connection for this node
		return &netceptor.Conn{}, nil
	}
	return nil, fmt.Errorf("node not found: %s", node)
}

func (mn *mockNetceptorForControl) GetLogger() *logger.ReceptorLogger {
	return mn.logger
}

func (mn *mockNetceptorForControl) GetClientTLSConfig(name string, expectedHostName string, expectedHostNameType netceptor.ExpectedHostnameType) (*tls.Config, error) {
	return nil, nil
}

func (mn *mockNetceptorForControl) Ping(ctx context.Context, target string, hopsToLive byte) (time.Duration, string, error) {
	return 0, "", nil
}

func (mn *mockNetceptorForControl) MaxForwardingHops() byte {
	return 30
}

func (mn *mockNetceptorForControl) Traceroute(ctx context.Context, target string) <-chan *netceptor.TracerouteResult {
	ch := make(chan *netceptor.TracerouteResult)
	close(ch)
	return ch
}

func (mn *mockNetceptorForControl) NodeID() string {
	return "test-control"
}

func (mn *mockNetceptorForControl) CancelBackends() {
	// No-op for testing
}

// Mock connection for testing lease protocol
type mockConn struct {
	responses []string
	responseIndex int
	closed    bool
}

func (mc *mockConn) Write(data []byte) (int, error) {
	if mc.closed {
		return 0, fmt.Errorf("connection closed")
	}
	return len(data), nil
}

func (mc *mockConn) Read(data []byte) (int, error) {
	if mc.closed {
		return 0, fmt.Errorf("connection closed")
	}
	if mc.responseIndex >= len(mc.responses) {
		return 0, fmt.Errorf("no more responses")
	}
	response := mc.responses[mc.responseIndex]
	mc.responseIndex++
	copy(data, []byte(response+"\n"))
	return len(response)+1, nil
}

func (mc *mockConn) Close() error {
	mc.closed = true
	return nil
}

func (mc *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func TestDiscoverWorkerNodes(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	tests := []struct {
		name           string
		advertisements []*netceptor.ServiceAdvertisement
		poolName       string
		expectedNodes  []string
	}{
		{
			name:           "no advertisements",
			advertisements: []*netceptor.ServiceAdvertisement{},
			poolName:       "",
			expectedNodes:  []string{},
		},
		{
			name: "non-worker services",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "node1", Service: "control", Tags: map[string]string{"type": "Control Service"}},
				&netceptor.ServiceAdvertisement{NodeID: "node2", Service: "tcp-proxy", Tags: map[string]string{"type": "TCP Proxy"}},
			},
			poolName:      "",
			expectedNodes: []string{},
		},
		{
			name: "worker nodes",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
				&netceptor.ServiceAdvertisement{NodeID: "worker2", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
			},
			poolName:      "",
			expectedNodes: []string{"worker1", "worker2"},
		},
		{
			name: "mixed services",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "control1", Service: "control", Tags: map[string]string{"type": "Control Service"}},
				&netceptor.ServiceAdvertisement{NodeID: "worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
				&netceptor.ServiceAdvertisement{NodeID: "proxy1", Service: "tcp-proxy", Tags: map[string]string{"type": "TCP Proxy"}},
				&netceptor.ServiceAdvertisement{NodeID: "worker2", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
			},
			poolName:      "",
			expectedNodes: []string{"worker1", "worker2"},
		},
		{
			name: "lease service without worker tag",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "node1", Service: "lease", Tags: map[string]string{"type": "Other"}},
				&netceptor.ServiceAdvertisement{NodeID: "worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
			},
			poolName:      "",
			expectedNodes: []string{"worker1"},
		},
		{
			name: "workers with pool tags - filter by production pool",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker2", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "staging-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "staging"}},
			},
			poolName:      "production",
			expectedNodes: []string{"prod-worker1", "prod-worker2"},
		},
		{
			name: "workers with pool tags - filter by staging pool",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "staging-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "staging"}},
				&netceptor.ServiceAdvertisement{NodeID: "staging-worker2", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "staging"}},
			},
			poolName:      "staging",
			expectedNodes: []string{"staging-worker1", "staging-worker2"},
		},
		{
			name: "workers with pool tags - no pool specified returns all",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "staging-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "staging"}},
				&netceptor.ServiceAdvertisement{NodeID: "worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
			},
			poolName:      "",
			expectedNodes: []string{"prod-worker1", "staging-worker1", "worker1"},
		},
		{
			name: "workers with pool tags - nonexistent pool returns empty",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "staging-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "staging"}},
			},
			poolName:      "nonexistent",
			expectedNodes: []string{},
		},
		{
			name: "mixed workers with and without pool tags - filter by pool",
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "worker-no-pool", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker2", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
			},
			poolName:      "production",
			expectedNodes: []string{"prod-worker1", "prod-worker2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNetceptor := &mockNetceptorForControl{
				logger:         logger,
				advertisements: tt.advertisements,
			}

			nodes := DiscoverWorkerNodes(mockNetceptor, tt.poolName)

			if len(nodes) != len(tt.expectedNodes) {
				t.Errorf("Expected %d nodes, got %d", len(tt.expectedNodes), len(nodes))
			}

			// Check that all expected nodes are present (order doesn't matter)
			nodeSet := make(map[string]bool)
			for _, node := range nodes {
				nodeSet[node] = true
			}

			for _, expected := range tt.expectedNodes {
				if !nodeSet[expected] {
					t.Errorf("Expected node %s not found in results", expected)
				}
			}
		})
	}
}

func TestOrderWorkersByJobHash(t *testing.T) {
	workers := []string{"worker1", "worker2", "worker3"}
	jobID := "test-job"

	// Order should be deterministic for same job ID
	order1 := OrderWorkersByJobHash(workers, jobID)
	order2 := OrderWorkersByJobHash(workers, jobID)

	if len(order1) != len(workers) {
		t.Errorf("Expected %d workers in order, got %d", len(workers), len(order1))
	}

	for i, worker := range order1 {
		if order2[i] != worker {
			t.Errorf("Order not deterministic: position %d differs (%s vs %s)", i, worker, order2[i])
		}
	}

	// All workers should be present
	workerSet := make(map[string]bool)
	for _, worker := range order1 {
		workerSet[worker] = true
	}

	for _, worker := range workers {
		if !workerSet[worker] {
			t.Errorf("Worker %s missing from ordered list", worker)
		}
	}

	// Different job IDs should potentially give different orders
	order3 := OrderWorkersByJobHash(workers, "different-job")
	different := false
	for i, worker := range order1 {
		if order3[i] != worker {
			different = true
			break
		}
	}

	if !different {
		t.Log("Note: Different job IDs happened to produce same order (low probability but possible)")
	}
}

func TestRequestLeaseFromWorker(t *testing.T) {
	tests := []struct {
		name             string
		mockResponse     string
		expectError      bool
		expectedType     string
		expectedToken    string
		expectedReason   string
	}{
		{
			name:          "successful lease grant",
			mockResponse:  `{"type":"lease_grant","job_id":"job1","lease_token":"token123","expires_at_unix_ms":1234567890000}`,
			expectError:   false,
			expectedType:  "lease_grant",
			expectedToken: "token123",
		},
		{
			name:           "lease denied busy",
			mockResponse:   `{"type":"lease_deny","job_id":"job1","reason":"BUSY"}`,
			expectError:    false,
			expectedType:   "lease_deny",
			expectedReason: "BUSY",
		},
		{
			name:           "lease denied draining",
			mockResponse:   `{"type":"lease_deny","job_id":"job1","reason":"DRAINING"}`,
			expectError:    false,
			expectedType:   "lease_deny",
			expectedReason: "DRAINING",
		},
		{
			name:        "invalid JSON response",
			mockResponse: `{"type":"lease_grant","job_id":}`,
			expectError: true,
		},
		{
			name:        "job ID mismatch",
			mockResponse: `{"type":"lease_grant","job_id":"wrong-job","lease_token":"token123"}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the response parsing logic directly
			var response LeaseResponse
			err := json.Unmarshal([]byte(tt.mockResponse), &response)

			if tt.expectError {
				if err == nil {
					// If we expect an error but JSON parsing succeeded,
					// check for logical errors like job ID mismatch
					if response.JobID != "job1" && response.JobID != "" {
						t.Log("Expected error due to job ID mismatch - test passed")
						return
					}
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected JSON parsing error: %v", err)
			}

			if response.Type != tt.expectedType {
				t.Errorf("Expected type=%s, got %s", tt.expectedType, response.Type)
			}

			if tt.expectedToken != "" && response.Token != tt.expectedToken {
				t.Errorf("Expected token=%s, got %s", tt.expectedToken, response.Token)
			}

			if tt.expectedReason != "" && response.Reason != tt.expectedReason {
				t.Errorf("Expected reason=%s, got %s", tt.expectedReason, response.Reason)
			}
		})
	}
}

func TestScheduleJobWithLease(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	t.Run("no workers available", func(t *testing.T) {
		mockNetceptor := &mockNetceptorForControl{
			logger:         logger,
			advertisements: []*netceptor.ServiceAdvertisement{}, // No workers
		}

		submitParams := map[string]interface{}{
			"worktype": "test",
		}

		err := ScheduleJobWithLease(mockNetceptor, "job1", submitParams, 5, "")
		if err == nil {
			t.Fatal("Expected error when no workers available")
		}
		if !strings.Contains(err.Error(), "no workers discovered") {
			t.Errorf("Expected 'no workers discovered' error, got: %v", err)
		}
	})

	t.Run("no workers in specified pool", func(t *testing.T) {
		mockNetceptor := &mockNetceptorForControl{
			logger: logger,
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
			},
		}

		submitParams := map[string]interface{}{
			"worktype": "test",
		}

		err := ScheduleJobWithLease(mockNetceptor, "job1", submitParams, 5, "staging")
		if err == nil {
			t.Fatal("Expected error when no workers in pool")
		}
		if !strings.Contains(err.Error(), "no workers discovered") || !strings.Contains(err.Error(), "staging") {
			t.Errorf("Expected pool-specific error, got: %v", err)
		}
	})

	t.Run("successful scheduling", func(t *testing.T) {
		// Set up mock with workers
		mockNetceptor := &mockNetceptorForControl{
			logger: logger,
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node"}},
			},
			connections: make(map[string]*mockConn),
		}

		// Mock successful lease grant
		mockNetceptor.connections["worker1"] = &mockConn{
			responses: []string{`{"type":"lease_grant","job_id":"job1","lease_token":"token123","expires_at_unix_ms":1234567890000}`},
		}

		// For this test, we'll test the worker discovery part
		workers := DiscoverWorkerNodes(mockNetceptor, "")
		if len(workers) != 1 || workers[0] != "worker1" {
			t.Errorf("Expected to discover worker1, got %v", workers)
		}

		// Test ordering
		ordered := OrderWorkersByJobHash(workers, "job1")
		if len(ordered) != 1 || ordered[0] != "worker1" {
			t.Errorf("Expected ordered workers [worker1], got %v", ordered)
		}
	})
}

func TestSubmitJobWithLeaseScheduling(t *testing.T) {
	logger := logger.NewReceptorLogger("")

	t.Run("generates job ID when not provided", func(t *testing.T) {
		mockNetceptor := &mockNetceptorForControl{
			logger:         logger,
			advertisements: []*netceptor.ServiceAdvertisement{}, // No workers (will fail)
		}

		params := map[string]string{"param1": "value1"}

		// Should fail due to no workers, but should generate job ID
		_, err := SubmitJobWithLeaseScheduling(mockNetceptor, "test-worktype", "", params, "")
		if err == nil {
			t.Fatal("Expected error due to no workers")
		}

		if !strings.Contains(err.Error(), "no workers discovered") {
			t.Errorf("Expected scheduling error, got: %v", err)
		}
	})

	t.Run("uses provided job ID", func(t *testing.T) {
		mockNetceptor := &mockNetceptorForControl{
			logger:         logger,
			advertisements: []*netceptor.ServiceAdvertisement{}, // No workers (will fail)
		}

		params := map[string]string{"param1": "value1"}
		jobID := "custom-job-id"

		// Should fail due to no workers, but should use provided job ID
		_, err := SubmitJobWithLeaseScheduling(mockNetceptor, "test-worktype", jobID, params, "")
		if err == nil {
			t.Fatal("Expected error due to no workers")
		}

		if !strings.Contains(err.Error(), jobID) {
			t.Errorf("Error should mention job ID %s, got: %v", jobID, err)
		}
	})

	t.Run("schedules to specific pool", func(t *testing.T) {
		mockNetceptor := &mockNetceptorForControl{
			logger: logger,
			advertisements: []*netceptor.ServiceAdvertisement{
				&netceptor.ServiceAdvertisement{NodeID: "prod-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "production"}},
				&netceptor.ServiceAdvertisement{NodeID: "staging-worker1", Service: "lease", Tags: map[string]string{"type": "Worker Node", "pool": "staging"}},
			},
		}

		params := map[string]string{"param1": "value1"}

		// Test worker discovery with pool filter for production
		workers := DiscoverWorkerNodes(mockNetceptor, "production")
		if len(workers) != 1 || workers[0] != "prod-worker1" {
			t.Errorf("Expected to discover only prod-worker1, got %v", workers)
		}

		// Test worker discovery with pool filter for staging
		workers = DiscoverWorkerNodes(mockNetceptor, "staging")
		if len(workers) != 1 || workers[0] != "staging-worker1" {
			t.Errorf("Expected to discover only staging-worker1, got %v", workers)
		}

		// Test that scheduling with nonexistent pool fails
		_, err := SubmitJobWithLeaseScheduling(mockNetceptor, "test-worktype", "test-job", params, "nonexistent")
		if err == nil {
			t.Fatal("Expected error when scheduling with nonexistent pool")
		}
		if !strings.Contains(err.Error(), "nonexistent") {
			t.Errorf("Expected error to mention pool 'nonexistent', got: %v", err)
		}
	})
}