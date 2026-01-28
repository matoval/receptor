//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ansible/receptor/pkg/logger"
)

func TestLeaseServiceAdvertisement(t *testing.T) {
	// Test that the service configuration includes the correct tags
	config := &LeaseServiceCfg{
		Service:              "lease",
		MaxRunningJobs:       10,
		MaxOutstandingLeases: 10,
		LeaseTTLMs:          10000,
		Draining:            false,
	}

	// Verify config values
	if config.Service != "lease" {
		t.Errorf("Expected service='lease', got '%s'", config.Service)
	}
	if config.MaxRunningJobs != 10 {
		t.Errorf("Expected MaxRunningJobs=10, got %d", config.MaxRunningJobs)
	}
}

func TestLeaseServiceProtocol(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	leaseManager := NewLeaseManager(nil, logger)
	defer leaseManager.Shutdown()

	tests := []struct {
		name           string
		input          string
		expectedType   string
		expectedReason string
	}{
		{
			name:         "valid lease request",
			input:        `{"type":"lease_request","job_id":"job1","ttl_ms":10000}`,
			expectedType: "lease_grant",
		},
		{
			name:           "invalid JSON",
			input:          `{"type":"lease_request","job_id":}`,
			expectedType:   "lease_deny",
			expectedReason: "INVALID",
		},
		{
			name:           "wrong request type",
			input:          `{"type":"wrong_type","job_id":"job1","ttl_ms":10000}`,
			expectedType:   "lease_deny",
			expectedReason: "INVALID",
		},
		{
			name:           "empty job_id",
			input:          `{"type":"lease_request","job_id":"","ttl_ms":10000}`,
			expectedType:   "lease_deny",
			expectedReason: "INVALID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Start the connection handler in background
			go func() {
				handleLeaseConnection(ctx, server, leaseManager, logger)
			}()

			// Send request
			_, err := client.Write([]byte(tt.input + "\n"))
			if err != nil {
				t.Fatalf("Failed to send request: %v", err)
			}

			// Read response with timeout
			client.SetReadDeadline(time.Now().Add(time.Second))
			scanner := bufio.NewScanner(client)
			if !scanner.Scan() {
				if scanner.Err() != nil {
					t.Fatalf("Scanner error: %v", scanner.Err())
				}
				t.Fatal("Failed to read response")
			}

			var response LeaseResponse
			err = json.Unmarshal(scanner.Bytes(), &response)
			if err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			if response.Type != tt.expectedType {
				t.Errorf("Expected type='%s', got '%s'", tt.expectedType, response.Type)
			}

			if tt.expectedReason != "" && response.Reason != tt.expectedReason {
				t.Errorf("Expected reason='%s', got '%s'", tt.expectedReason, response.Reason)
			}

			if response.Type == "lease_grant" {
				if response.Token == "" {
					t.Error("Expected non-empty token for granted lease")
				}
				if response.ExpiresAt == 0 {
					t.Error("Expected non-zero expiration time")
				}
			}
		})
	}
}

func TestLeaseServiceCapacityDenial(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       1,
		MaxOutstandingLeases: 1,
		LeaseTTLMs:          10000,
		Draining:            false,
	}
	leaseManager := NewLeaseManager(config, logger)
	defer leaseManager.Shutdown()

	// Fill capacity
	leaseManager.runningJobs = 1

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection handler
	go handleLeaseConnection(ctx, server, leaseManager, logger)

	// Send request when at capacity
	request := `{"type":"lease_request","job_id":"job1","ttl_ms":10000}`
	_, err := client.Write([]byte(request + "\n"))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Read response
	scanner := bufio.NewScanner(client)
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}

	var response LeaseResponse
	err = json.Unmarshal(scanner.Bytes(), &response)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response.Type != "lease_deny" {
		t.Errorf("Expected lease_deny, got %s", response.Type)
	}
	if response.Reason != "BUSY" {
		t.Errorf("Expected reason='BUSY', got '%s'", response.Reason)
	}
}

func TestLeaseServiceDrainingMode(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	config := &LeaseManagerConfig{
		MaxRunningJobs:       10,
		MaxOutstandingLeases: 10,
		LeaseTTLMs:          10000,
		Draining:            true, // Start in draining mode
	}
	leaseManager := NewLeaseManager(config, logger)
	defer leaseManager.Shutdown()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection handler
	go handleLeaseConnection(ctx, server, leaseManager, logger)

	// Send request while draining
	request := `{"type":"lease_request","job_id":"job1","ttl_ms":10000}`
	_, err := client.Write([]byte(request + "\n"))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Read response
	scanner := bufio.NewScanner(client)
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}

	var response LeaseResponse
	err = json.Unmarshal(scanner.Bytes(), &response)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response.Type != "lease_deny" {
		t.Errorf("Expected lease_deny, got %s", response.Type)
	}
	if response.Reason != "DRAINING" {
		t.Errorf("Expected reason='DRAINING', got '%s'", response.Reason)
	}
}

func TestLeaseServiceIdempotentRequests(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	leaseManager := NewLeaseManager(nil, logger)
	defer leaseManager.Shutdown()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection handler
	go handleLeaseConnection(ctx, server, leaseManager, logger)

	jobID := "idempotent-job"
	request := fmt.Sprintf(`{"type":"lease_request","job_id":"%s","ttl_ms":10000}`, jobID)

	// Send first request
	_, err := client.Write([]byte(request + "\n"))
	if err != nil {
		t.Fatalf("Failed to send first request: %v", err)
	}

	// Read first response
	scanner := bufio.NewScanner(client)
	if !scanner.Scan() {
		t.Fatal("Failed to read first response")
	}

	var response1 LeaseResponse
	err = json.Unmarshal(scanner.Bytes(), &response1)
	if err != nil {
		t.Fatalf("Failed to unmarshal first response: %v", err)
	}

	if response1.Type != "lease_grant" {
		t.Fatalf("Expected first request to be granted, got %s", response1.Type)
	}

	// Send second request for same job ID
	_, err = client.Write([]byte(request + "\n"))
	if err != nil {
		t.Fatalf("Failed to send second request: %v", err)
	}

	// Read second response
	if !scanner.Scan() {
		t.Fatal("Failed to read second response")
	}

	var response2 LeaseResponse
	err = json.Unmarshal(scanner.Bytes(), &response2)
	if err != nil {
		t.Fatalf("Failed to unmarshal second response: %v", err)
	}

	if response2.Type != "lease_grant" {
		t.Errorf("Expected second request to be granted, got %s", response2.Type)
	}

	// Should return same token
	if response1.Token != response2.Token {
		t.Errorf("Expected same token for idempotent requests: %s vs %s",
			response1.Token, response2.Token)
	}

	if response1.ExpiresAt != response2.ExpiresAt {
		t.Errorf("Expected same expiration time: %d vs %d",
			response1.ExpiresAt, response2.ExpiresAt)
	}
}

func TestLeaseServiceConnectionHandling(t *testing.T) {
	logger := logger.NewReceptorLogger("")
	leaseManager := NewLeaseManager(nil, logger)
	defer leaseManager.Shutdown()

	t.Run("handles connection close gracefully", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start the connection handler
		done := make(chan bool)
		go func() {
			handleLeaseConnection(ctx, server, leaseManager, logger)
			done <- true
		}()

		// Close client connection
		client.Close()

		// Should exit gracefully
		select {
		case <-done:
			// OK, handler exited
		case <-time.After(1 * time.Second):
			t.Error("Handler did not exit after client disconnect")
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		ctx, cancel := context.WithCancel(context.Background())

		// Start the connection handler
		done := make(chan bool)
		go func() {
			handleLeaseConnection(ctx, server, leaseManager, logger)
			done <- true
		}()

		// Cancel context
		cancel()

		// Should exit gracefully
		select {
		case <-done:
			// OK, handler exited
		case <-time.After(1 * time.Second):
			t.Error("Handler did not exit after context cancellation")
		}
	})

	t.Run("handles multiple requests on same connection", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start the connection handler
		go handleLeaseConnection(ctx, server, leaseManager, logger)

		// Send multiple requests
		requests := []string{
			`{"type":"lease_request","job_id":"job1","ttl_ms":10000}`,
			`{"type":"lease_request","job_id":"job2","ttl_ms":10000}`,
		}

		scanner := bufio.NewScanner(client)

		for _, request := range requests {
			_, err := client.Write([]byte(request + "\n"))
			if err != nil {
				t.Fatalf("Failed to send request: %v", err)
			}

			if !scanner.Scan() {
				t.Fatal("Failed to read response")
			}

			var response LeaseResponse
			err = json.Unmarshal(scanner.Bytes(), &response)
			if err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			if response.Type != "lease_grant" {
				t.Errorf("Expected lease_grant, got %s", response.Type)
			}
		}
	})
}