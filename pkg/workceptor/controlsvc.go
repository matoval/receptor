//go:build !no_workceptor
// +build !no_workceptor

package workceptor

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/ansible/receptor/pkg/controlsvc"
)

type workceptorCommandType struct {
	w *Workceptor
}

type workceptorCommand struct {
	w          *Workceptor
	subcommand string
	params     map[string]interface{}
}

func (t *workceptorCommandType) InitFromString(params string) (controlsvc.ControlCommand, error) {
	tokens := strings.Split(params, " ")
	if len(tokens) == 0 {
		return nil, fmt.Errorf("no work subcommand")
	}
	c := &workceptorCommand{
		w:          t.w,
		subcommand: strings.ToLower(tokens[0]),
		params:     make(map[string]interface{}),
	}
	switch c.subcommand {
	case "submit":
		if len(tokens) < 3 {
			return nil, fmt.Errorf("work submit requires a target node and work type")
		}
		c.params["node"] = tokens[1]
		c.params["worktype"] = tokens[2]
		if len(tokens) > 3 {
			c.params["params"] = strings.Join(tokens[3:], " ")
		}
	case "list":
		if len(tokens) > 1 {
			c.params["unitid"] = tokens[1]
		}
	case "status", "cancel", "release", "force-release":
		if len(tokens) < 2 {
			return nil, fmt.Errorf("work %s requires a unit ID", c.subcommand)
		}
		if len(tokens) > 2 {
			return nil, fmt.Errorf("work %s does not take parameters after the unit ID", c.subcommand)
		}
		c.params["unitid"] = tokens[1]
	case "results":
		if len(tokens) < 2 {
			return nil, fmt.Errorf("work results requires a unit ID")
		}
		if len(tokens) > 3 {
			return nil, fmt.Errorf("work results only takes a unit ID and optional start position")
		}
		c.params["unitid"] = tokens[1]
		if len(tokens) > 2 {
			var err error
			c.params["startpos"], err = strconv.ParseInt(tokens[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error converting start position to integer: %s", err)
			}
		} else {
			c.params["startpos"] = int64(0)
		}
	case "submit_auto":
		if len(tokens) < 2 {
			return nil, fmt.Errorf("work submit_auto requires a work type")
		}
		c.params["worktype"] = tokens[1]
		if len(tokens) > 2 {
			c.params["params"] = strings.Join(tokens[2:], " ")
		}
		// Mark as auto-submit to prevent node parameter
		c.params["_auto_submit"] = true
	}

	return c, nil
}

// strFromMap extracts a string from a map[string]interface{}, handling errors.
func strFromMap(config map[string]interface{}, name string) (string, error) {
	value, ok := config[name]
	if !ok {
		return "", fmt.Errorf("field %s missing", name)
	}
	valueStr, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("field %s must be a string", name)
	}

	return valueStr, nil
}

// intFromMap extracts an int64 from a map[string]interface{}, handling errors.
func intFromMap(config map[string]interface{}, name string) (int64, error) {
	value, ok := config[name]
	if !ok {
		return 0, fmt.Errorf("field %s missing", name)
	}
	valueInt, ok := value.(int64)
	if ok {
		return valueInt, nil
	}
	valueFloat, ok := value.(float64)
	if ok {
		return int64(valueFloat), nil
	}
	valueStr, ok := value.(string)
	if ok {
		valueInt, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return valueInt, err
		}
	}

	return 0, fmt.Errorf("field %s value %s is not convertible to an int", name, value)
}

func boolFromMap(config map[string]interface{}, name string) (bool, error) {
	value, ok := config[name]
	if !ok {
		return false, fmt.Errorf("field %s missing", name)
	}
	valueBoolStr, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("field %s must be a string", name)
	}
	if valueBoolStr == "true" {
		return true, nil
	}
	if valueBoolStr == "false" {
		return false, nil
	}

	return false, fmt.Errorf("field %s value %s is not convertible to a bool", name, value)
}

func (t *workceptorCommandType) InitFromJSON(config map[string]interface{}) (controlsvc.ControlCommand, error) {
	subCmd, err := strFromMap(config, "subcommand")
	if err != nil {
		return nil, err
	}
	c := &workceptorCommand{
		w:          t.w,
		subcommand: strings.ToLower(subCmd),
		params:     make(map[string]interface{}),
	}
	switch c.subcommand {
	case "submit":
		for k, v := range config {
			_, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("submit parameters must all be strings and %s is not", k)
			}
			c.params[k] = v
		}
		_, err := strFromMap(c.params, "node")
		if err != nil {
			return nil, err
		}
		_, err = strFromMap(c.params, "worktype")
		if err != nil {
			return nil, err
		}
	case "status", "cancel", "release", "force-release":
		c.params["unitid"], err = strFromMap(config, "unitid")
		if err != nil {
			return nil, err
		}
		signature, err := strFromMap(config, "signature")
		if err == nil {
			c.params["signature"] = signature
		}
	case "list":
		unitID, err := strFromMap(config, "unitid")
		if err == nil {
			c.params["unitid"] = unitID
		}
	case "results":
		c.params["unitid"], err = strFromMap(config, "unitid")
		if err != nil {
			return nil, err
		}
		c.params["startpos"], err = intFromMap(config, "startpos")
		if err != nil {
			return nil, err
		}
		signature, err := strFromMap(config, "signature")
		if err == nil {
			c.params["signature"] = signature
		}
	case "submit_auto":
		workType, err := strFromMap(config, "worktype")
		if err != nil {
			return nil, err
		}
		c.params["worktype"] = workType

		// Explicitly prohibit node parameter
		if _, exists := config["node"]; exists {
			return nil, fmt.Errorf("submit_auto does not accept 'node' parameter - worker is automatically selected")
		}

		// Copy all other parameters except reserved ones
		for k, v := range config {
			if k != "command" && k != "subcommand" && k != "worktype" {
				_, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("submit_auto parameters must all be strings and %s is not", k)
				}
				c.params[k] = v
			}
		}
		c.params["_auto_submit"] = true
	}

	return c, nil
}

func (c *workceptorCommand) processSignature(workType, signature string, connIsUnix, signWork bool) error {
	shouldVerifySignature := c.w.ShouldVerifySignature(workType, signWork)
	if !shouldVerifySignature && signature != "" {
		return fmt.Errorf("work type did not expect a signature")
	}
	if shouldVerifySignature && !connIsUnix {
		err := c.w.VerifySignature(signature)
		if err != nil {
			return err
		}
	}

	return nil
}

func getSignWorkFromStatus(status *StatusFileData) bool {
	red, ok := status.ExtraData.(*RemoteExtraData)
	if ok {
		return red.SignWork
	}

	return false
}

// Worker function called by the control service to process a "work" command.
func (c *workceptorCommand) ControlFunc(ctx context.Context, nc controlsvc.NetceptorForControlCommand, cfo controlsvc.ControlFuncOperations) (map[string]interface{}, error) {
	addr := cfo.RemoteAddr()
	connIsUnix := false
	if addr.Network() == "unix" {
		connIsUnix = true
	}
	switch c.subcommand {
	case "submit":
		workNode, err := strFromMap(c.params, "node")
		if err != nil {
			return nil, err
		}
		workType, err := strFromMap(c.params, "worktype")
		if err != nil {
			return nil, err
		}
		tlsClient, err := strFromMap(c.params, "tlsclient")
		if err != nil {
			tlsClient = "" // optional so don't return
		}
		ttl, err := strFromMap(c.params, "ttl")
		if err != nil {
			ttl = ""
		}
		signWork, err := boolFromMap(c.params, "signwork")
		if err != nil {
			signWork = false
		}
		signature, err := strFromMap(c.params, "signature")
		if err != nil {
			signature = ""
		}
		workUnitID, err := strFromMap(c.params, "workUnitID")
		if err != nil {
			workUnitID = ""
		}
		workParams := make(map[string]string)
		nonParams := []string{"command", "subcommand", "node", "worktype", "tlsclient", "ttl", "signwork", "signature", "workUnitID", "lease_token"}
		inNonParams := func(p string) bool {
			for _, nonparam := range nonParams {
				if p == nonparam {
					return true
				}
			}

			return false
		}
		for k, v := range c.params {
			if ok := inNonParams(k); ok {
				continue
			}
			vStr, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string", k)
			}
			workParams[k] = vStr
		}

		// Extract lease token for validation
		leaseToken, err := strFromMap(c.params, "lease_token")
		if err != nil {
			leaseToken = ""
		}


		// Validate lease if enforcement is enabled
		if c.w.IsLeaseEnforcementEnabled() {
			// Use provided workUnitID or generate a temporary one for validation
			jobID := workUnitID
			if jobID == "" {
				jobID = "temp_" + generateRandomID()
			}
			err = c.w.ValidateLeaseForSubmit(jobID, leaseToken)
			if err != nil {
				return nil, err // Returns specific lease error codes
			}
			// Note: Do not increment here - increment only when work actually starts
			c.w.nc.GetLogger().Debug("Lease validated for job %s", jobID)
		}

		err = c.processSignature(workType, signature, connIsUnix, signWork)
		if err != nil {
			return nil, err
		}
		isLocalHost := strings.EqualFold(workNode, "localhost")
		var worker WorkUnit
		if workNode == nc.NodeID() || isLocalHost {
			if ttl != "" {
				return nil, fmt.Errorf("ttl option is intended for remote work only")
			}
			worker, err = c.w.AllocateUnit(workType, workUnitID, workParams)
		} else {
			// Check if target node has lease service - if so, use lease-based scheduling
			if hasLeaseService(nc, workNode) {
				worker, err = c.allocateRemoteUnitWithLease(nc, workNode, workType, workUnitID, tlsClient, ttl, signWork, workParams)
			} else {
				worker, err = c.w.AllocateRemoteUnit(workNode, workType, workUnitID, tlsClient, ttl, signWork, workParams)
			}
		}
		if err != nil {
			return nil, err
		}
		cfr := make(map[string]interface{})
		cfr["unitid"] = worker.ID()
		stdin, err := os.OpenFile(path.Join(worker.UnitDir(), "stdin"), os.O_CREATE+os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		worker.UpdateBasicStatus(WorkStatePending, "Waiting for Input Data", 0)
		err = cfo.ReadFromConn(fmt.Sprintf("Work unit created with ID %s. Send stdin data and EOF.\n", worker.ID()), stdin, &controlsvc.SocketConnIO{})
		if err != nil {
			worker.UpdateBasicStatus(WorkStateFailed, fmt.Sprintf("Error reading input data: %s", err), 0)

			return nil, err
		}
		err = stdin.Close()
		if err != nil {
			worker.UpdateBasicStatus(WorkStateFailed, fmt.Sprintf("Error reading input data: %s", err), 0)

			return nil, err
		}
		worker.UpdateBasicStatus(WorkStatePending, "Starting Worker", 0)

		// Increment running jobs count when starting work
		c.w.IncrementRunningJobs()

		err = worker.Start()
		if err != nil && !IsPending(err) {
			// Decrement if start failed immediately
			c.w.DecrementRunningJobs()
			worker.UpdateBasicStatus(WorkStateFailed, fmt.Sprintf("Error starting worker: %s", err), 0)

			return cfr, err
		}
		if IsPending(err) {
			cfr["result"] = "Job Submitted"
		} else {
			cfr["result"] = "Job Started"
		}

		return cfr, nil
	case "list":
		var unitList []string
		targetUnitID, ok := c.params["unitid"].(string)
		if ok {
			unitList = append(unitList, targetUnitID)
		} else {
			unitList = c.w.ListKnownUnitIDs()
		}
		cfr := make(map[string]interface{})
		for i := range unitList {
			unitID := unitList[i]
			status, err := c.w.unitStatusForCFR(unitID)
			if err != nil {
				return nil, err
			}
			cfr[unitID] = status
		}

		return cfr, nil
	case "status":
		unitid, err := strFromMap(c.params, "unitid")
		if err != nil {
			return nil, err
		}
		cfr, err := c.w.unitStatusForCFR(unitid)
		if err != nil {
			return nil, err
		}

		return cfr, nil
	case "cancel", "release", "force-release":
		unitid, err := strFromMap(c.params, "unitid")
		if err != nil {
			return nil, err
		}
		signature, err := strFromMap(c.params, "signature")
		if err != nil {
			signature = ""
		}
		cfr := make(map[string]interface{})
		var pendingMsg string
		var completeMsg string
		if c.subcommand == "cancel" {
			pendingMsg = "cancel pending"
			completeMsg = "cancelled"
		} else {
			pendingMsg = "release pending"
			completeMsg = "released"
		}
		unit, err := c.w.findUnit(unitid)
		if err != nil {
			cfr["unit not found"] = unitid

			return cfr, err
		}
		status := unit.Status()
		signWork := getSignWorkFromStatus(status)
		err = c.processSignature(status.WorkType, signature, connIsUnix, signWork)
		if err != nil {
			return nil, err
		}
		if c.subcommand == "cancel" {
			err = unit.Cancel()
		} else {
			err = unit.Release(c.subcommand == "force-release")
		}
		if err != nil && !IsPending(err) {
			return nil, err
		}
		if IsPending(err) {
			cfr[pendingMsg] = unitid
		} else {
			cfr[completeMsg] = unitid
		}

		return cfr, nil
	case "results":
		unitid, err := strFromMap(c.params, "unitid")
		if err != nil {
			return nil, err
		}
		startPos, err := intFromMap(c.params, "startpos")
		if err != nil {
			return nil, err
		}
		signature, err := strFromMap(c.params, "signature")
		if err != nil {
			signature = ""
		}
		unit, err := c.w.findUnit(unitid)
		if err != nil {
			return nil, err
		}
		status := unit.Status()
		signWork := getSignWorkFromStatus(status)
		err = c.processSignature(status.WorkType, signature, connIsUnix, signWork)
		if err != nil {
			return nil, err
		}

		resultChan, err := c.w.GetResults(ctx, unitid, startPos)
		if err != nil {
			return nil, err
		}
		err = cfo.WriteToConn(fmt.Sprintf("Streaming results for work unit %s\n", unitid), resultChan)
		if err != nil {
			return nil, err
		}

		err = cfo.Close()
		if err != nil {
			return nil, err
		}

		return nil, nil
	case "submit_auto":
		return c.executeAutoSubmit(ctx, nc, cfo)
	}

	return nil, fmt.Errorf("bad command")
}

// executeAutoSubmit implements automatic worker selection and job submission
func (c *workceptorCommand) executeAutoSubmit(ctx context.Context, nc controlsvc.NetceptorForControlCommand, cfo controlsvc.ControlFuncOperations) (map[string]interface{}, error) {
	// Extract standard parameters
	workType, err := strFromMap(c.params, "worktype")
	if err != nil {
		return nil, err
	}

	tlsClient, err := strFromMap(c.params, "tlsclient")
	if err != nil {
		tlsClient = ""
	}

	ttl, err := strFromMap(c.params, "ttl")
	if err != nil {
		ttl = ""
	}

	signWork, err := boolFromMap(c.params, "signwork")
	if err != nil {
		signWork = false
	}

	workUnitID, err := strFromMap(c.params, "workUnitID")
	if err != nil {
		// Leave empty - AllocateRemoteUnit will generate one
		workUnitID = ""
	}

	// Extract optional pool parameter for execution node pool selection
	poolName, err := strFromMap(c.params, "pool")
	if err != nil {
		poolName = ""
	}

	// Build work parameters (same logic as existing submit)
	workParams := make(map[string]string)
	nonParams := []string{"command", "subcommand", "worktype", "tlsclient", "ttl", "signwork", "signature", "workUnitID", "pool", "_auto_submit"}

	inNonParams := func(p string) bool {
		for _, nonparam := range nonParams {
			if p == nonparam {
				return true
			}
		}
		return false
	}

	for k, v := range c.params {
		if ok := inNonParams(k); ok {
			continue
		}
		vStr, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%s must be a string", k)
		}
		workParams[k] = vStr
	}

	// Discover workers with lease services (optionally filtered by pool)
	workers := controlsvc.DiscoverWorkerNodes(nc, poolName)
	if len(workers) == 0 {
		if poolName != "" {
			return nil, fmt.Errorf("no worker nodes with lease services found in pool '%s'", poolName)
		}
		return nil, fmt.Errorf("no worker nodes with lease services found in the mesh")
	}

	// Use a temporary job ID for worker ordering if workUnitID is empty
	orderingID := workUnitID
	if orderingID == "" {
		orderingID = nc.NodeID() + "-" + generateRandomID()
	}

	// Try workers in deterministic order based on job ID
	orderedWorkers := controlsvc.OrderWorkersByJobHash(workers, orderingID)

	var worker WorkUnit
	var lastError error
	var selectedWorker string

	// Try up to 5 workers
	maxTries := 5
	if len(orderedWorkers) < maxTries {
		maxTries = len(orderedWorkers)
	}

	for i := 0; i < maxTries; i++ {
		selectedWorker = orderedWorkers[i]

		// Use the existing lease-based allocation method
		worker, err = c.allocateRemoteUnitWithLease(nc, selectedWorker, workType, workUnitID, tlsClient, ttl, signWork, workParams)
		if err != nil {
			lastError = fmt.Errorf("worker %s: %w", selectedWorker, err)
			continue
		}

		// Success - break out of loop
		break
	}

	if worker == nil {
		if lastError != nil {
			return nil, fmt.Errorf("failed to allocate work on any worker: %w", lastError)
		}
		return nil, fmt.Errorf("no workers available for work allocation")
	}

	// Prepare response with worker selection metadata
	cfr := map[string]interface{}{
		"unitid":          worker.ID(),
		"selected_worker": selectedWorker,
	}
	if poolName != "" {
		cfr["pool"] = poolName
	}

	// Handle stdin input (same as existing submit)
	stdin, err := os.OpenFile(path.Join(worker.UnitDir(), "stdin"), os.O_CREATE+os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}

	worker.UpdateBasicStatus(WorkStatePending, "Waiting for Input Data", 0)
	err = cfo.ReadFromConn(fmt.Sprintf("Work unit created with ID %s. Send stdin data and EOF.\n", worker.ID()), stdin, &controlsvc.SocketConnIO{})
	if err != nil {
		worker.UpdateBasicStatus(WorkStateFailed, fmt.Sprintf("Error reading input data: %s", err), 0)
		return nil, err
	}

	err = stdin.Close()
	if err != nil {
		worker.UpdateBasicStatus(WorkStateFailed, fmt.Sprintf("Error reading input data: %s", err), 0)
		return nil, err
	}

	worker.UpdateBasicStatus(WorkStatePending, "Starting Worker", 0)

	// Increment running jobs count when starting work
	c.w.IncrementRunningJobs()

	err = worker.Start()
	if err != nil && !IsPending(err) {
		// Decrement if start failed immediately
		c.w.DecrementRunningJobs()
		worker.UpdateBasicStatus(WorkStateFailed, fmt.Sprintf("Error starting worker: %s", err), 0)
		return cfr, err
	}

	// Set result field after successful worker start (matches original submit pattern)
	if IsPending(err) {
		cfr["result"] = "Job Submitted"
	} else {
		cfr["result"] = "Job Started"
	}

	return cfr, nil
}

// allocateRemoteUnitWithToken submits work to a specific worker using provided lease token
func (c *workceptorCommand) allocateRemoteUnitWithToken(nc controlsvc.NetceptorForControlCommand, workNode, workType, workUnitID, tlsClient, ttl string, signWork bool, leaseToken string, workParams map[string]string) (WorkUnit, error) {
	// Add lease token to work parameters
	workParams["lease_token"] = leaseToken

	// Use existing remote unit allocation
	return c.w.AllocateRemoteUnit(workNode, workType, workUnitID, tlsClient, ttl, signWork, workParams)
}

// generateRandomID creates a random ID for temporary job validation
func generateRandomID() string {
	bytes := make([]byte, 4)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// hasLeaseService checks if the target node advertises a lease service
func hasLeaseService(nc controlsvc.NetceptorForControlCommand, targetNode string) bool {
	status := nc.Status()

	MainInstance.nc.GetLogger().Debug("hasLeaseService checking for target node: %s", targetNode)
	for _, ad := range status.Advertisements {
		MainInstance.nc.GetLogger().Debug("hasLeaseService found advertisement: NodeID=%s, Service=%s, Tags=%v", ad.NodeID, ad.Service, ad.Tags)
		if ad.NodeID == targetNode && ad.Service == "lease" {
			MainInstance.nc.GetLogger().Debug("hasLeaseService found lease service for %s", targetNode)
			if tags, ok := ad.Tags["type"]; ok && tags == "Worker Node" {
				MainInstance.nc.GetLogger().Debug("hasLeaseService confirmed Worker Node type for %s", targetNode)
				return true
			} else {
				MainInstance.nc.GetLogger().Debug("hasLeaseService wrong tags for %s: type=%s, ok=%v", targetNode, tags, ok)
			}
		}
	}
	MainInstance.nc.GetLogger().Debug("hasLeaseService returning false for %s", targetNode)
	return false
}

// allocateRemoteUnitWithLease handles remote work submission with lease-based scheduling
func (c *workceptorCommand) allocateRemoteUnitWithLease(nc controlsvc.NetceptorForControlCommand, workNode, workType, workUnitID, tlsClient, ttl string, signWork bool, workParams map[string]string) (WorkUnit, error) {
	// Generate job ID for lease request - this will become the work unit ID
	jobID := workUnitID
	if jobID == "" {
		// Pre-generate the work unit ID so lease request and work submission use the same ID
		jobID = nc.NodeID() + generateRandomID()
	}

	// Connect to worker's lease service and request lease
	conn, err := nc.Dial(workNode, "lease", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lease service on %s: %v", workNode, err)
	}
	defer conn.Close()

	// Send lease request
	request := map[string]interface{}{
		"type":    "lease_request",
		"job_id":  jobID,
		"ttl_ms":  10000, // 10 seconds
	}
	requestData, _ := json.Marshal(request)
	_, err = conn.Write(append(requestData, '\n'))
	if err != nil {
		return nil, fmt.Errorf("failed to send lease request: %v", err)
	}

	// Read lease response
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read lease response: %v", err)
	}

	var response map[string]interface{}
	err = json.Unmarshal(line[:len(line)-1], &response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse lease response: %v", err)
	}

	// Check if lease was granted
	if responseType, ok := response["type"].(string); !ok || responseType != "lease_grant" {
		if reason, ok := response["reason"].(string); ok {
			return nil, fmt.Errorf("lease denied: %s", reason)
		}
		return nil, fmt.Errorf("lease denied")
	}

	leaseToken, ok := response["lease_token"].(string)
	if !ok || leaseToken == "" {
		return nil, fmt.Errorf("invalid lease response: missing token")
	}

	// Include lease token in work parameters so it gets sent to the remote node
	workParamsWithLease := make(map[string]string)
	for k, v := range workParams {
		workParamsWithLease[k] = v
	}
	workParamsWithLease["lease_token"] = leaseToken

	return c.w.AllocateRemoteUnit(workNode, workType, jobID, tlsClient, ttl, signWork, workParamsWithLease)
}
