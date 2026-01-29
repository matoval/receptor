# Receptor Leasing Service & Auto-Selecting Work Submission

## Overview

The Receptor Leasing Service introduces **auto-selecting work submission**, transforming Receptor from "lease-gated targeted submission" to true "pull scheduling." Instead of requiring explicit worker targeting (`"node":"worker1"`), the system can now automatically discover available workers and intelligently distribute jobs.

## Key Benefits

- **Automatic Worker Discovery**: No need to know worker topology
- **Intelligent Load Balancing**: Jobs distributed evenly across available workers
- **Capacity Awareness**: Respects worker limits and handles backpressure
- **Fault Tolerance**: Automatic retry across multiple workers
- **Execution Node Pools**: Group workers into pools for targeted auto-selection
- **Backward Compatibility**: Existing explicit targeting continues to work

## Architecture Overview

```
Control Node                    Worker Nodes
┌─────────────┐                ┌──────────────┐
│             │                │   Worker1    │
│ submit_auto │──── discover ──▶│ lease service│
│   command   │                │ max: 1 job   │
│             │                └──────────────┘
│             │                ┌──────────────┐
│             │──── schedule ──▶│   Worker2    │
│             │                │ lease service│
│             │                │ max: 1 job   │
└─────────────┘                └──────────────┘
```

## How Auto-Selection Works

### 1. Worker Discovery

The control node discovers available workers by scanning service advertisements for nodes advertising the `lease` service with `type: "Worker Node"` tag.

```go
// Workers discovered via service advertisements
workers := DiscoverWorkerNodes(nc)
// Returns: ["worker1", "worker2", "worker3", "worker4", "worker5"]
```

### 2. Deterministic Worker Ordering

Workers are ordered using a stable hash based on `job_id + worker_id` to ensure consistent, predictable selection:

```go
hash := SHA256(job_id + worker_id)
// This ensures the same job always tries workers in the same order
```

**Example ordering for job `control1-abc123`:**
```
Worker Selection Order:
1. worker3 (hash: 1a2b3c4d)
2. worker1 (hash: 2b3c4d5e)
3. worker5 (hash: 3c4d5e6f)
4. worker2 (hash: 4d5e6f7a)
5. worker4 (hash: 5e6f7a8b)
```

### 3. Lease-Based Scheduling

The control node tries workers sequentially until one grants a lease:

```
1. Request lease from worker3 → "BUSY" (deny)
2. Request lease from worker1 → "GRANTED" ✓
3. Submit job to worker1 with lease token
```

## Execution Node Pools

Execution node pools allow you to group workers by environment, region, or capability, enabling targeted auto-selection within specific worker subsets.

### Why Use Pools?

- **Environment Isolation**: Separate production, staging, and development execution nodes
- **Geographic Distribution**: Route jobs to workers in specific data centers or regions
- **Capacity Tiers**: Group high-capacity vs. low-capacity workers
- **Workload Specialization**: Dedicated pools for long-running jobs vs. quick tasks

### How Pools Work

Workers advertise their pool membership via service tags. When submitting with `submit_auto`, you can optionally specify a pool name to limit worker discovery to only workers in that pool.

**Pool Configuration Example:**

```yaml
# Production Worker Configuration
- node:
    id: prod-worker1

- lease-service:
    service: lease
    pool: production      # Worker advertises membership in "production" pool
    maxrunningjobs: 10

# Staging Worker Configuration
- node:
    id: staging-worker1

- lease-service:
    service: lease
    pool: staging         # Worker advertises membership in "staging" pool
    maxrunningjobs: 5
```

**Submitting to a Specific Pool:**

```bash
# Submit to production pool only
echo '{"command":"work","subcommand":"submit_auto","worktype":"ansible-playbook","pool":"production"}' | nc -U /tmp/control1.sock

# Submit to staging pool only
echo '{"command":"work","subcommand":"submit_auto","worktype":"ansible-playbook","pool":"staging"}' | nc -U /tmp/control1.sock

# Submit to any available worker (no pool specified)
echo '{"command":"work","subcommand":"submit_auto","worktype":"ansible-playbook"}' | nc -U /tmp/control1.sock
```

**Pool Response:**

```json
{
  "result": "Job Started",
  "unitid": "control1-abc123",
  "selected_worker": "prod-worker1",
  "pool": "production"
}
```

### Pool Discovery Process

When a pool is specified:

1. Control node scans service advertisements for lease services with `type: "Worker Node"` **AND** `pool: "specified-pool"`
2. Only workers matching both the worker type and pool name are considered
3. If no workers are found in the specified pool, an error is returned immediately
4. Workers are ordered and tried using the same deterministic algorithm

## Network Protocol

### Lease Request Message

**From Control Node to Worker:**
```json
{
  "type": "lease_request",
  "job_id": "control1-abc123",
  "ttl_ms": 10000
}
```

### Lease Response Messages

**Grant Response:**
```json
{
  "type": "lease_grant",
  "job_id": "control1-abc123",
  "lease_token": "token_xyz789",
  "expires_at_unix_ms": 1643723400000
}
```

**Deny Response:**
```json
{
  "type": "lease_deny",
  "job_id": "control1-abc123",
  "reason": "BUSY"
}
```

### Job Submission with Lease Token

After obtaining a lease, the job is submitted with the lease token included in work parameters:

```json
{
  "worktype": "countdown",
  "workUnitID": "control1-abc123",
  "lease_token": "token_xyz789",
  "params": "additional job parameters"
}
```

## Socket Command Interface

### Auto-Selecting Submission

**JSON Format:**
```bash
echo '{"command":"work","subcommand":"submit_auto","worktype":"countdown"}' | nc -U /tmp/control1.sock
```

**Response:**
```json
{
  "result": "Job Started",
  "unitid": "control1-abc123",
  "selected_worker": "worker3"
}
```

### Traditional Explicit Submission

**JSON Format:**
```bash
echo '{"command":"work","subcommand":"submit","worktype":"countdown","node":"worker1"}' | nc -U /tmp/control1.sock
```

**Response:**
```json
{
  "result": "Job Started",
  "unitid": "control1-def456"
}
```

### Command Parameters

**Required Parameters:**

- `command`: `"work"`
- `subcommand`: `"submit_auto"`
- `worktype`: Job type (e.g., `"countdown"`, `"ansible-playbook"`)

**Optional Parameters:**

- `pool`: Execution node pool name (filters workers to specified pool)
- `workUnitID`: Custom job ID (auto-generated if omitted)
- `tlsclient`: TLS client certificate
- `ttl`: Job timeout
- `params`: Additional job parameters

**Prohibited Parameters:**

- `node`: Explicitly rejected (use `"submit"` for explicit targeting)

## Configuration

### Control Node Configuration

```yaml
- node:
    id: control1

- control-service:
    service: control
    filename: /tmp/control1.sock

- tcp-listener:
    port: 7000
```

### Worker Node Configuration

```yaml
- node:
    id: worker1

- lease-service:
    service: lease
    pool: production              # Optional: pool name for grouping workers
    maxrunningjobs: 1
    maxoutstandingleases: 1

- tcp-peer:
    address: control1:7000

- work-command:
    worktype: countdown
    command: bash
    params: -c "echo STARTING && for i in {1..5}; do echo \"Worker1 second $i/5\"; sleep 1; done && echo COMPLETE"
```

### Key Configuration Settings

**Lease Service Settings:**

- `maxrunningjobs`: Maximum concurrent jobs (typically 1)
- `maxoutstandingleases`: Maximum outstanding lease requests (typically 1)
- `pool`: Optional pool name for grouping workers (omit for no pool)

**Service Advertisement:**

- Workers automatically advertise `lease` service with `type: "Worker Node"` tag
- If `pool` is configured, workers also advertise `pool: "pool-name"` tag
- Control node discovers workers via these advertisements

## Message Flow Example

### Successful Auto-Submission

```
1. Client → Control: {"command":"work","subcommand":"submit_auto","worktype":"countdown"}

2. Control discovers workers: [worker1, worker2, worker3, worker4, worker5]

3. Control orders workers by hash: [worker3, worker1, worker5, worker2, worker4]

4. Control → Worker3: {"type":"lease_request","job_id":"control1-abc123","ttl_ms":10000}
5. Worker3 → Control: {"type":"lease_deny","job_id":"control1-abc123","reason":"BUSY"}

6. Control → Worker1: {"type":"lease_request","job_id":"control1-abc123","ttl_ms":10000}
7. Worker1 → Control: {"type":"lease_grant","job_id":"control1-abc123","lease_token":"token_xyz"}

8. Control submits job to Worker1 with lease token

9. Control → Client: {"result":"Job Started","unitid":"control1-abc123","selected_worker":"worker1"}
```

### Failed Auto-Submission (All Workers Busy)

```
1. Client → Control: {"command":"work","subcommand":"submit_auto","worktype":"countdown"}

2. Control tries worker3: lease_deny "BUSY"
3. Control tries worker1: lease_deny "BUSY"
4. Control tries worker5: lease_deny "BUSY"
5. Control tries worker2: lease_deny "BUSY"
6. Control tries worker4: lease_deny "BUSY"

7. Control → Client: {"error":"all discovered workers are at capacity and denied lease requests"}
```

## Error Handling

### Common Error Responses

**No Workers Available:**
```json
{
  "error": "no worker nodes with lease services found in the mesh"
}
```

**No Workers in Pool:**
```json
{
  "error": "no worker nodes with lease services found in pool 'production'"
}
```

**All Workers Busy:**
```json
{
  "error": "all discovered workers are at capacity and denied lease requests"
}
```

**Invalid Parameters:**
```json
{
  "error": "submit_auto does not accept 'node' parameter - worker is automatically selected"
}
```

## Performance Characteristics

### Worker Selection Algorithm

- **Time Complexity**: O(n log n) for worker ordering + O(k) for lease attempts
- **Space Complexity**: O(n) for worker list storage
- **Retry Limit**: Maximum 5 workers tried per job
- **Deterministic**: Same job ID produces same worker ordering

### Network Efficiency

- **Discovery**: One-time service advertisement scan
- **Lease Requests**: Sequential, fail-fast (10-second timeout)
- **Job Submission**: Single network call to selected worker
- **Total Latency**: Typically 1-3 lease requests per job

## Monitoring and Debugging

### Log Messages

**Worker Discovery:**
```
DEBUG: Discovered worker node: worker1
DEBUG: Discovered 5 worker nodes
```

**Lease Scheduling:**
```
INFO: Attempting to schedule job control1-abc123 on 3/5 workers: [worker1, worker3, worker5]
DEBUG: Trying worker 1/3: worker1 for job control1-abc123
INFO: Lease granted by worker1 for job control1-abc123: token=token_xyz
```

**Capacity Issues:**
```
DEBUG: Lease denied by worker3: BUSY
INFO: Failed to obtain lease from any worker (tried 5/5 workers)
```

### Status Monitoring

Check worker connectivity:
```bash
echo '{"command":"status"}' | nc -U /tmp/control1.sock | jq '.Advertisements[] | select(.Service=="lease")'
```

Monitor active jobs:
```bash
find /tmp/receptor/worker* -name "control1*" -type d | wc -l
```

## Migration Guide

### From Explicit to Auto-Selection

**Before (Explicit):**
```json
{"command":"work","subcommand":"submit","worktype":"countdown","node":"worker1"}
```

**After (Auto-Selection):**
```json
{"command":"work","subcommand":"submit_auto","worktype":"countdown"}
```

### Gradual Migration Strategy

1. **Phase 1**: Deploy lease services on all workers
2. **Phase 2**: Test auto-submission alongside existing explicit submission
3. **Phase 3**: Migrate applications to use `submit_auto`
4. **Phase 4**: Optionally deprecate explicit submission for new workflows

## Best Practices

### When to Use Auto-Selection

✅ **Use `submit_auto` when:**

- You want optimal load balancing
- Worker topology may change
- You need fault tolerance
- You don't care which specific worker executes the job
- You want to target a group of workers via pool name

### When to Use Explicit Targeting

✅ **Use `submit` when:**
- You need jobs on specific workers (e.g., specialized hardware)
- You're integrating with legacy systems
- You need predictable worker placement
- You're debugging worker-specific issues

### Capacity Planning

- Set `maxrunningjobs` based on worker resource capacity
- Monitor lease denial rates to identify capacity bottlenecks
- Use multiple workers to provide redundancy
- Consider job duration when planning capacity
- Use pools to isolate workloads and prevent resource contention between environments

## Troubleshooting

### Common Issues

**Issue**: "no worker nodes with lease services found"
**Solution**: Verify worker lease service configuration and network connectivity

**Issue**: "no worker nodes with lease services found in pool 'poolname'"
**Solution**: Verify workers are configured with correct pool name, check pool spelling, ensure workers have advertised successfully

**Issue**: "all workers denied lease due to capacity"
**Solution**: Increase `maxrunningjobs` on workers or add more worker nodes

**Issue**: Jobs not completing
**Solution**: Check worker logs for job execution errors

### Debugging Commands

```bash
# Check worker discovery
echo '{"command":"status"}' | nc -U /tmp/control1.sock | grep lease

# Check which pools are available
echo '{"command":"status"}' | nc -U /tmp/control1.sock | jq '.Advertisements[] | select(.Service=="lease") | {NodeID, Pool: .Tags.pool}'

# Test explicit submission to specific worker
echo '{"command":"work","subcommand":"submit","worktype":"countdown","node":"worker1"}' | nc -U /tmp/control1.sock

# Test auto submission
echo '{"command":"work","subcommand":"submit_auto","worktype":"countdown"}' | nc -U /tmp/control1.sock

# Test auto submission with pool
echo '{"command":"work","subcommand":"submit_auto","worktype":"countdown","pool":"production"}' | nc -U /tmp/control1.sock

# Monitor job completion
find /tmp/receptor/worker* -name "stdout" -path "*/control1*" -exec head -1 {} \;
```

## Technical Implementation

### Key Files

- **`pkg/workceptor/controlsvc.go`**: Auto-submission command implementation
- **`pkg/controlsvc/lease_scheduling.go`**: Worker discovery and lease scheduling
- **Worker lease services**: Handle capacity management and lease granting

### Integration Points

- **Service Discovery**: Uses existing Receptor service advertisement mechanism
- **Lease Protocol**: JSON-based request/response over Receptor mesh connections
- **Work Submission**: Integrates with existing remote work allocation
- **Capacity Management**: Leverages existing lease service infrastructure

---

**For more information, see the implementation plan at: `/Users/ahetheri/.claude/plans/synchronous-roaming-fiddle.md`**