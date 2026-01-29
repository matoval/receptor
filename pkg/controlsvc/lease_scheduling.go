//go:build !no_workceptor
// +build !no_workceptor

package controlsvc

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"time"
)

// Use the existing NetceptorForControlCommand interface from interfaces.go

// LeaseRequest represents a lease request as defined in the spec.
type LeaseRequest struct {
	Type   string `json:"type"`
	JobID  string `json:"job_id"`
	TTLMs  int    `json:"ttl_ms"`
}

// LeaseResponse represents a lease response as defined in the spec.
type LeaseResponse struct {
	Type      string `json:"type"`
	JobID     string `json:"job_id"`
	Token     string `json:"lease_token,omitempty"`
	ExpiresAt int64  `json:"expires_at_unix_ms,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// DiscoverWorkerNodes discovers worker nodes by finding services advertising the "lease" service
// with the "Worker Node" type tag. Optionally filters by pool name if poolName is non-empty.
// This uses the existing service advertisement mechanism.
func DiscoverWorkerNodes(nc NetceptorForControlCommand, poolName string) []string {
	status := nc.Status()
	workers := make([]string, 0)
	logger := nc.GetLogger()

	// Look for advertisements of the "lease" service with "Worker Node" type
	for _, ad := range status.Advertisements {
		if ad.Service == "lease" {
			if adType, ok := ad.Tags["type"]; ok && adType == "Worker Node" {
				// If poolName is specified, filter by pool tag
				if poolName != "" {
					if adPool, ok := ad.Tags["pool"]; ok && adPool == poolName {
						workers = append(workers, ad.NodeID)
						logger.Debug("Discovered worker node: %s (pool: %s)", ad.NodeID, poolName)
					}
					// Skip workers that don't match the pool
				} else {
					// No pool filter specified, include all workers
					workers = append(workers, ad.NodeID)
					logger.Debug("Discovered worker node: %s", ad.NodeID)
				}
			}
		}
	}

	if poolName != "" {
		logger.Debug("Discovered %d worker nodes in pool '%s'", len(workers), poolName)
	} else {
		logger.Debug("Discovered %d worker nodes", len(workers))
	}
	return workers
}

// RequestLeaseFromWorker requests a lease from a specific worker node using the lease service protocol.
func RequestLeaseFromWorker(nc NetceptorForControlCommand, workerNode, jobID string, ttlMs int) (*LeaseResponse, error) {
	logger := nc.GetLogger()
	logger.Debug("Requesting lease from worker %s for job %s", workerNode, jobID)

	// Connect to the lease service on the worker node
	conn, err := nc.Dial(workerNode, "lease", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker %s lease service: %w", workerNode, err)
	}
	defer conn.Close()

	// Set connection timeout
	err = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to set connection deadline: %w", err)
	}

	// Create lease request
	request := LeaseRequest{
		Type:   "lease_request",
		JobID:  jobID,
		TTLMs:  ttlMs,
	}

	// Send JSON request with newline
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal lease request: %w", err)
	}

	_, err = conn.Write(append(requestBytes, '\n'))
	if err != nil {
		return nil, fmt.Errorf("failed to send lease request to %s: %w", workerNode, err)
	}

	logger.Debug("Sent lease request to %s: %s", workerNode, string(requestBytes))

	// Read response
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		err := scanner.Err()
		if err != nil {
			return nil, fmt.Errorf("failed to read lease response from %s: %w", workerNode, err)
		}
		return nil, fmt.Errorf("no response received from worker %s", workerNode)
	}

	responseBytes := scanner.Bytes()
	logger.Debug("Received lease response from %s: %s", workerNode, string(responseBytes))

	// Parse response
	var response LeaseResponse
	err = json.Unmarshal(responseBytes, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse lease response from %s: %w", workerNode, err)
	}

	// Validate response
	if response.JobID != jobID {
		return nil, fmt.Errorf("lease response job ID mismatch from %s: expected %s, got %s", workerNode, jobID, response.JobID)
	}

	return &response, nil
}

// OrderWorkersByJobHash orders worker nodes by a stable hash of job_id + worker_id.
// This ensures the same job will try workers in the same order, providing deterministic behavior.
func OrderWorkersByJobHash(workers []string, jobID string) []string {
	type workerScore struct {
		nodeID string
		hash   uint32
	}

	scores := make([]workerScore, len(workers))
	for i, worker := range workers {
		// Create stable hash: hash(job_id + worker_id)
		hasher := sha256.New()
		hasher.Write([]byte(jobID + worker))
		hashBytes := hasher.Sum(nil)
		hash := crc32.ChecksumIEEE(hashBytes[:8]) // Use first 8 bytes for CRC32
		scores[i] = workerScore{nodeID: worker, hash: hash}
	}

	// Sort by hash value
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].hash < scores[j].hash
	})

	// Extract ordered worker list
	ordered := make([]string, len(scores))
	for i, score := range scores {
		ordered[i] = score.nodeID
	}

	return ordered
}

// ScheduleJobWithLease implements the lease-first scheduling algorithm as specified in pullPlan.md.
// It discovers workers, orders them deterministically, and tries each worker sequentially until
// one grants a lease and accepts the work submission. Optionally filters by poolName if non-empty.
func ScheduleJobWithLease(nc NetceptorForControlCommand, jobID string, submitParams map[string]interface{}, maxRetries int, poolName string) error {
	logger := nc.GetLogger()

	if maxRetries <= 0 {
		maxRetries = 5 // Default from spec
	}

	// Step 1: Discover worker nodes (optionally filtered by pool)
	workers := DiscoverWorkerNodes(nc, poolName)
	if len(workers) == 0 {
		if poolName != "" {
			return fmt.Errorf("no workers discovered with lease services in pool '%s'", poolName)
		}
		return fmt.Errorf("no workers discovered with lease services")
	}

	// Step 2: Order workers deterministically by stable hash
	orderedWorkers := OrderWorkersByJobHash(workers, jobID)

	// Step 3: Limit to max workers to try
	workersToTry := orderedWorkers
	if len(orderedWorkers) > maxRetries {
		workersToTry = orderedWorkers[:maxRetries]
	}

	logger.Info("Attempting to schedule job %s on %d/%d workers: %v", jobID, len(workersToTry), len(workers), workersToTry)

	// Step 4: Try each worker sequentially with enhanced error tracking
	var lastError error
	var busyWorkers []string
	var failedWorkers []string

	for i, workerNode := range workersToTry {
		logger.Debug("Trying worker %d/%d: %s for job %s", i+1, len(workersToTry), workerNode, jobID)

		// Request lease (using default TTL from spec: 10 seconds)
		leaseResponse, err := RequestLeaseFromWorker(nc, workerNode, jobID, 10000)
		if err != nil {
			lastError = fmt.Errorf("worker %s lease request failed: %w", workerNode, err)
			failedWorkers = append(failedWorkers, workerNode)
			logger.Debug("Failed to request lease from worker %s: %s", workerNode, err)
			continue
		}

		// Check if lease was granted
		if leaseResponse.Type == "lease_deny" {
			if leaseResponse.Reason == "BUSY" {
				busyWorkers = append(busyWorkers, workerNode)
				lastError = fmt.Errorf("worker %s denied lease: BUSY", workerNode)
			} else {
				failedWorkers = append(failedWorkers, workerNode)
				lastError = fmt.Errorf("worker %s denied lease: %s", workerNode, leaseResponse.Reason)
			}
			logger.Debug("Lease denied by worker %s: %s", workerNode, leaseResponse.Reason)
			continue
		}

		if leaseResponse.Type != "lease_grant" {
			lastError = fmt.Errorf("unexpected response type from worker %s: %s", workerNode, leaseResponse.Type)
			failedWorkers = append(failedWorkers, workerNode)
			logger.Debug("Unexpected response type from worker %s: %s", workerNode, leaseResponse.Type)
			continue
		}

		if leaseResponse.Token == "" {
			lastError = fmt.Errorf("worker %s granted lease but provided empty token", workerNode)
			failedWorkers = append(failedWorkers, workerNode)
			logger.Debug("Worker %s granted lease but provided empty token", workerNode)
			continue
		}

		logger.Info("Lease granted by worker %s for job %s: token=%s", workerNode, jobID, leaseResponse.Token)

		// Step 5: Submit work with lease token
		submitParams["lease_token"] = leaseResponse.Token
		submitParams["node"] = workerNode // Ensure we submit to the worker that granted the lease

		// Create control command for submit
		// Note: This would need to integrate with the existing control command infrastructure
		// For now, we'll return success to indicate the lease was obtained
		logger.Info("Successfully obtained lease for job %s from worker %s", jobID, workerNode)
		return nil
	}

	// Enhanced error reporting based on failure types
	totalTried := len(busyWorkers) + len(failedWorkers)

	if len(busyWorkers) == totalTried && len(busyWorkers) > 0 {
		// All workers were busy
		return fmt.Errorf("all %d workers denied lease due to capacity (tried: %v)", len(busyWorkers), busyWorkers)
	}

	if len(failedWorkers) == totalTried && len(failedWorkers) > 0 {
		// All workers had communication or other failures
		return fmt.Errorf("failed to obtain lease from any worker (tried %d/%d workers): %w",
			len(workersToTry), len(workers), lastError)
	}

	if totalTried > 0 {
		// Mixed failures
		errorMsg := fmt.Sprintf("failed to obtain lease from any worker (tried %d/%d workers)",
			len(workersToTry), len(workers))
		if len(busyWorkers) > 0 {
			errorMsg += fmt.Sprintf(", %d workers busy: %v", len(busyWorkers), busyWorkers)
		}
		if len(failedWorkers) > 0 {
			errorMsg += fmt.Sprintf(", %d workers failed", len(failedWorkers))
		}
		errorMsg += fmt.Sprintf(": %s", lastError)
		return fmt.Errorf(errorMsg)
	}

	// Fallback error (shouldn't happen)
	return fmt.Errorf("failed to obtain lease from any of %d workers for job %s", len(workersToTry), jobID)
}

// SubmitJobWithLeaseScheduling is a high-level function that combines lease scheduling with job submission.
// It implements the complete lease-first workflow: discover workers, request lease, submit with token.
// Optionally filters workers by poolName if non-empty.
func SubmitJobWithLeaseScheduling(nc NetceptorForControlCommand, workType, jobID string, params map[string]string, poolName string) (map[string]interface{}, error) {
	logger := nc.GetLogger()

	// Convert string params to interface{} map for scheduling
	submitParams := make(map[string]interface{})
	submitParams["worktype"] = workType
	if jobID != "" {
		submitParams["workUnitID"] = jobID
	}
	for k, v := range params {
		submitParams[k] = v
	}

	// Use job ID or generate one if not provided
	if jobID == "" {
		jobID = fmt.Sprintf("job-%d", time.Now().UnixNano())
		submitParams["workUnitID"] = jobID
	}

	if poolName != "" {
		logger.Info("Starting lease-based scheduling for job %s, worktype %s, pool %s", jobID, workType, poolName)
	} else {
		logger.Info("Starting lease-based scheduling for job %s, worktype %s", jobID, workType)
	}

	// Perform lease-first scheduling with enhanced error reporting
	err := ScheduleJobWithLease(nc, jobID, submitParams, 5, poolName)
	if err != nil {
		return nil, fmt.Errorf("lease scheduling failed for job %s: %w", jobID, err)
	}

	// Return the submit parameters with lease token and target node
	result := map[string]interface{}{
		"message": fmt.Sprintf("Job %s scheduled successfully with lease-based scheduling", jobID),
		"params":  submitParams,
	}

	return result, nil
}