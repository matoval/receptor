//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ansible/receptor/pkg/logger"
	"github.com/ansible/receptor/pkg/netceptor"
)

// NetceptorForLeaseService defines the interface needed from netceptor for the lease service.
type NetceptorForLeaseService interface {
	GetLogger() *logger.ReceptorLogger
	ListenAndAdvertise(service string, tlscfg *tls.Config, tags map[string]string) (*netceptor.Listener, error)
}

// LeaseRequest represents a request for a lease as defined in the spec.
type LeaseRequest struct {
	Type   string `json:"type"`
	JobID  string `json:"job_id"`
	TTLMs  int    `json:"ttl_ms"`
}

// LeaseResponse represents a response to a lease request as defined in the spec.
type LeaseResponse struct {
	Type      string `json:"type"`
	JobID     string `json:"job_id"`
	Token     string `json:"lease_token,omitempty"`
	ExpiresAt int64  `json:"expires_at_unix_ms,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// LeaseService runs the lease service that handles lease requests via newline-delimited JSON
// over Netceptor stream connections. It advertises itself as a "lease" service with
// the "Worker Node" type tag to enable worker discovery. Optionally advertises pool membership.
func LeaseService(ctx context.Context, nc NetceptorForLeaseService, service string,
	tlscfg *tls.Config, pool string, leaseManager *LeaseManager) error {
	logger := nc.GetLogger()

	// Build advertisement tags
	tags := map[string]string{
		"type": "Worker Node",
	}
	if pool != "" {
		tags["pool"] = pool
		logger.Info("Lease service will advertise pool membership: %s", pool)
	}

	// Listen and advertise the lease service with Worker Node tag and optional pool tag
	listener, err := nc.ListenAndAdvertise(service, tlscfg, tags)
	if err != nil {
		return fmt.Errorf("error listening and advertising lease service: %s", err)
	}

	if pool != "" {
		logger.Info("Lease service started on service '%s' (pool: %s)", service, pool)
	} else {
		logger.Info("Lease service started on service '%s'", service)
	}

	// Start the accept loop in a goroutine
	go func() {
		defer func() {
			if listener != nil {
				err := listener.Close()
				if err != nil {
					logger.Error("Error closing lease service listener: %s", err)
				}
			}
			logger.Info("Lease service stopped")
		}()

		for {
			select {
			case <-ctx.Done():
				logger.Debug("Lease service context cancelled, stopping accept loop")
				return
			default:
			}

			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					// Context was cancelled, normal shutdown
					return
				}
				logger.Error("Error accepting connection on lease service: %s", err)
				continue
			}

			// Handle each connection in its own goroutine
			go handleLeaseConnection(ctx, conn, leaseManager, logger)
		}
	}()

	return nil
}

// handleLeaseConnection handles a single lease service connection.
// It reads newline-delimited JSON lease requests and responds with lease grants/denials.
func handleLeaseConnection(ctx context.Context, conn net.Conn, leaseManager *LeaseManager, logger *logger.ReceptorLogger) {
	remoteAddr := conn.RemoteAddr().String()
	logger.Debug("Lease service connection from %s", remoteAddr)

	defer func() {
		if conn != nil {
			err := conn.Close()
			if err != nil {
				logger.Debug("Error closing lease connection from %s: %s", remoteAddr, err)
			}
		}
		logger.Debug("Lease service connection closed for %s", remoteAddr)
	}()

	// Set up timeouts for connection
	err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		logger.Error("Error setting read deadline for lease connection: %s", err)
		return
	}

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 64KB initial, 1MB max

	// Process requests until connection closes or context is cancelled
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read next line (JSON request)
		if !scanner.Scan() {
			err := scanner.Err()
			if err != nil && err != io.EOF {
				logger.Debug("Error reading from lease connection %s: %s", remoteAddr, err)
			}
			return
		}

		line := scanner.Text()
		logger.Debug("Lease request from %s: %s", remoteAddr, line)

		// Parse the JSON request
		var request LeaseRequest
		err := json.Unmarshal([]byte(line), &request)
		if err != nil {
			logger.Error("Invalid JSON in lease request from %s: %s", remoteAddr, err)
			response := LeaseResponse{
				Type:   "lease_deny",
				JobID:  "",
				Reason: "INVALID",
			}
			sendLeaseResponse(conn, &response, logger)
			continue
		}

		// Validate request type
		if request.Type != "lease_request" {
			logger.Error("Invalid request type '%s' from %s", request.Type, remoteAddr)
			response := LeaseResponse{
				Type:   "lease_deny",
				JobID:  request.JobID,
				Reason: "INVALID",
			}
			sendLeaseResponse(conn, &response, logger)
			continue
		}

		// Validate job ID
		if request.JobID == "" {
			logger.Error("Empty job_id in lease request from %s", remoteAddr)
			response := LeaseResponse{
				Type:   "lease_deny",
				JobID:  "",
				Reason: "INVALID",
			}
			sendLeaseResponse(conn, &response, logger)
			continue
		}

		// Process the lease request
		response := processLeaseRequest(&request, leaseManager, logger)
		sendLeaseResponse(conn, response, logger)

		// Reset read deadline for next request
		err = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			logger.Error("Error setting read deadline for lease connection: %s", err)
			return
		}
	}
}

// processLeaseRequest processes a validated lease request and returns the appropriate response.
func processLeaseRequest(request *LeaseRequest, leaseManager *LeaseManager, logger *logger.ReceptorLogger) *LeaseResponse {
	lease, err := leaseManager.RequestLease(request.JobID, request.TTLMs)
	if err != nil {
		logger.Debug("Lease request denied for job %s: %s", request.JobID, err)
		return &LeaseResponse{
			Type:   "lease_deny",
			JobID:  request.JobID,
			Reason: err.Error(), // DRAINING or BUSY
		}
	}

	logger.Debug("Lease granted for job %s: token=%s, expires=%v",
		request.JobID, lease.Token, lease.ExpiresAt)

	return &LeaseResponse{
		Type:      "lease_grant",
		JobID:     request.JobID,
		Token:     lease.Token,
		ExpiresAt: lease.ExpiresAt.UnixMilli(),
	}
}

// sendLeaseResponse sends a JSON lease response followed by a newline.
func sendLeaseResponse(conn net.Conn, response *LeaseResponse, logger *logger.ReceptorLogger) {
	responseBytes, err := json.Marshal(response)
	if err != nil {
		logger.Error("Error marshaling lease response: %s", err)
		return
	}

	// Send response with newline
	_, err = conn.Write(append(responseBytes, '\n'))
	if err != nil {
		logger.Debug("Error writing lease response: %s", err)
		return
	}

	logger.Debug("Sent lease response: %s", string(responseBytes))
}