#!/bin/bash

echo "🔄 === REFINED MIXED SUBMISSION TEST ==="
echo "Testing submit (explicit node) and submit_auto (auto-selection) with capacity awareness"
echo "Pattern: 2 explicit → wait → 3 auto → wait → 3 explicit → wait → 2 auto"
echo

# Function to submit explicit job
submit_explicit_job() {
    local job_num=$1
    local target_worker=$2
    echo "$(date '+%H:%M:%S'): Submitting EXPLICIT job $job_num to $target_worker..."

    result=$(timeout 10 bash -c "(echo \"{\\\"command\\\":\\\"work\\\",\\\"subcommand\\\":\\\"submit\\\",\\\"worktype\\\":\\\"countdown\\\",\\\"node\\\":\\\"$target_worker\\\"}\" && echo) | nc -U /tmp/control1.sock" 2>/dev/null)

    if echo "$result" | grep -q '"unitid"'; then
        json_line=$(echo "$result" | tail -1)
        work_unit_id=$(echo "$json_line" | grep -o '"unitid":"[^"]*"' | cut -d'"' -f4)

        work_unit_dir=$(find /tmp/receptor/worker* -name "$work_unit_id" -type d 2>/dev/null | head -1)
        if [ -n "$work_unit_dir" ]; then
            actual_worker=$(echo "$work_unit_dir" | cut -d'/' -f4)
            echo "✅ Explicit job $job_num: Created work unit $work_unit_id on $actual_worker"
            return 0
        else
            echo "❌ Explicit job $job_num: Work unit $work_unit_id not found"
            return 1
        fi
    else
        echo "❌ Explicit job $job_num failed: $result"
        return 1
    fi
}

# Function to submit auto job
submit_auto_job() {
    local job_num=$1
    echo "$(date '+%H:%M:%S'): Submitting AUTO job $job_num..."

    result=$(timeout 10 bash -c '(echo "{\"command\":\"work\",\"subcommand\":\"submit_auto\",\"worktype\":\"countdown\"}" && echo) | nc -U /tmp/control1.sock' 2>/dev/null)

    if echo "$result" | grep -q '"unitid"'; then
        json_line=$(echo "$result" | tail -1)
        work_unit_id=$(echo "$json_line" | grep -o '"unitid":"[^"]*"' | cut -d'"' -f4)
        selected_worker=$(echo "$json_line" | grep -o '"selected_worker":"[^"]*"' | cut -d'"' -f4)

        work_unit_dir=$(find /tmp/receptor/worker* -name "$work_unit_id" -type d 2>/dev/null | head -1)
        if [ -n "$work_unit_dir" ]; then
            echo "✅ Auto job $job_num: Created work unit $work_unit_id on $selected_worker"
            return 0
        else
            echo "❌ Auto job $job_num: Work unit $work_unit_id not found"
            return 1
        fi
    else
        echo "❌ Auto job $job_num failed: $result"
        return 1
    fi
}

# Get initial status
status_json=$(timeout 10 bash -c 'echo "{\"command\":\"status\"}" | nc -U /tmp/control1.sock' 2>/dev/null | tail -n +2)
lease_count=$(echo "$status_json" | grep -o '"Service":"lease"' | wc -l | tr -d ' ')
echo "✅ Lease services found: $lease_count/5"

echo "✅ Connected workers:"
echo "$status_json" | grep -o '"NodeID":"worker[^"]*"' | sort | uniq

# Record initial state
initial_count=$(find /tmp/receptor/worker* -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
echo "📊 Initial work units: $initial_count"

# Track results
explicit_success=0
auto_success=0
total_jobs=0

echo
echo "🎯 Phase 1: 2 Explicit Submissions"

# 2 explicit submissions to different workers
if submit_explicit_job 1 "worker1"; then
    explicit_success=$((explicit_success + 1))
fi
total_jobs=$((total_jobs + 1))

if submit_explicit_job 2 "worker2"; then
    explicit_success=$((explicit_success + 1))
fi
total_jobs=$((total_jobs + 1))

echo "⏰ Waiting 6 seconds for Phase 1 jobs to complete..."
#sleep 6

echo
echo "🎯 Phase 2: 3 Auto Submissions"

# 3 auto submissions
for i in {3..5}; do
    if submit_auto_job $i; then
        auto_success=$((auto_success + 1))
    fi
    total_jobs=$((total_jobs + 1))
    sleep 1
done

echo "⏰ Waiting 6 seconds for Phase 2 jobs to complete..."
sleep 6

echo
echo "🎯 Phase 3: 3 Explicit Submissions"

# 3 explicit submissions to available workers (after jobs completed)
if submit_explicit_job 6 "worker3"; then
    explicit_success=$((explicit_success + 1))
fi
total_jobs=$((total_jobs + 1))

if submit_explicit_job 7 "worker4"; then
    explicit_success=$((explicit_success + 1))
fi
total_jobs=$((total_jobs + 1))

if submit_explicit_job 8 "worker5"; then
    explicit_success=$((explicit_success + 1))
fi
total_jobs=$((total_jobs + 1))

echo "⏰ Waiting 6 seconds for Phase 3 jobs to complete..."
#sleep 6

echo
echo "🎯 Phase 4: 2 Auto Submissions"

# 2 auto submissions
for i in {9..10}; do
    if submit_auto_job $i; then
        auto_success=$((auto_success + 1))
    fi
    total_jobs=$((total_jobs + 1))
    sleep 1
done

# Check final state
final_count=$(find /tmp/receptor/worker* -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
new_jobs=$((final_count - initial_count))

echo
echo "📊 Mixed Submission Results Summary:"
echo "   Total jobs attempted: $total_jobs"
echo "   Explicit jobs successful: $explicit_success/5"
echo "   Auto jobs successful: $auto_success/5"
echo "   New work units created: $new_jobs"

echo
echo "📂 Work unit distribution by worker:"
for i in {1..5}; do
    worker_jobs=$(find /tmp/receptor/worker$i -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
    if [ "$worker_jobs" -gt 0 ]; then
        echo "   Worker$i: $worker_jobs work units"
    fi
done

echo
echo "🕒 Waiting 8 seconds for final jobs to complete..."
sleep 8

echo
echo "📄 Checking job completion status:"
total_completed=0
for i in {1..5}; do
    completed_jobs=$(find /tmp/receptor/worker$i -name "stdout" -path "*/control1*" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$completed_jobs" -gt 0 ]; then
        echo "   Worker$i: $completed_jobs completed jobs"
        total_completed=$((total_completed + completed_jobs))
    fi
done

echo
echo "🏆 FINAL REFINED MIXED SUBMISSION TEST RESULTS:"
echo "   Explicit submissions: $explicit_success/5"
echo "   Auto submissions: $auto_success/5"
echo "   Work units created: $new_jobs"
echo "   Jobs completed: $total_completed"
echo "   Workers utilized: $(find /tmp/receptor/worker* -name "control1*" -type d 2>/dev/null | cut -d'/' -f4 | sort | uniq | wc -l | tr -d ' ')"

echo
echo "📋 Test Analysis:"
if [ "$auto_success" -eq 5 ]; then
    echo "✅ Auto-submission: Perfect (5/5) - Demonstrates superior capacity handling"
else
    echo "❌ Auto-submission: Issues detected ($auto_success/5)"
fi

if [ "$explicit_success" -eq 5 ]; then
    echo "✅ Explicit submission: Perfect (5/5) - Works when workers available"
elif [ "$explicit_success" -ge 3 ]; then
    echo "⚠️  Explicit submission: Partial success ($explicit_success/5) - Some capacity conflicts"
else
    echo "❌ Explicit submission: Poor performance ($explicit_success/5)"
fi

if [ "$auto_success" -eq 5 ] && [ "$explicit_success" -ge 3 ] && [ "$total_completed" -gt 0 ]; then
    echo
    echo "🎯 SUCCESS: Mixed submission system working correctly!"
    echo "   ✓ Auto-submission handles capacity automatically"
    echo "   ✓ Explicit submission works when capacity available"
    echo "   ✓ Both methods can coexist"
    echo "   ✓ Jobs complete successfully"
else
    echo
    echo "❌ Mixed submission needs attention"
fi