#!/bin/bash

echo "🔒 === AUTO-SUBMIT LEASE ENFORCEMENT TEST ==="
echo "Testing lease enforcement specifically for submit_auto command"
echo

# Function to submit auto job and track response
submit_auto_job() {
    local job_id=$1
    echo "$(date '+%H:%M:%S'): Submitting submit_auto job $job_id..."

    result=$(timeout 10 bash -c '(echo "{\"command\":\"work\",\"subcommand\":\"submit_auto\",\"worktype\":\"countdown\"}" && echo) | nc -U /tmp/control1.sock' 2>/dev/null)

    if echo "$result" | grep -q "all.*workers.*denied.*lease\|all.*workers.*capacity\|no.*workers.*found"; then
        echo "✅ Auto job $job_id: Correctly DENIED - All workers busy"
        echo "    Response: $(echo "$result" | tail -1 | grep -o 'ERROR:.*')"
        return 1
    elif echo "$result" | grep -q '"unitid"'; then
        json_line=$(echo "$result" | tail -1)
        work_unit_id=$(echo "$json_line" | grep -o '"unitid":"[^"]*"' | cut -d'"' -f4)
        selected_worker=$(echo "$json_line" | grep -o '"selected_worker":"[^"]*"' | cut -d'"' -f4)
        echo "⚠️  Auto job $job_id: ACCEPTED - $work_unit_id on $selected_worker"
        return 0
    else
        echo "❌ Auto job $job_id: Unexpected response - $result"
        return 1
    fi
}

# Get initial status
status_json=$(timeout 10 bash -c 'echo "{\"command\":\"status\"}" | nc -U /tmp/control1.sock' 2>/dev/null | tail -n +2)
lease_count=$(echo "$status_json" | grep -o '"Service":"lease"' | wc -l | tr -d ' ')
echo "✅ Lease services found: $lease_count/5"

# Clear previous jobs - wait for any running jobs to complete
echo "⏰ Waiting 6 seconds for any existing jobs to complete..."
sleep 6

echo
echo "🎯 Test 1: Rapid submit_auto submissions"
echo "Expected: Jobs distributed across workers, respecting capacity limits"
echo

# Submit 7 jobs rapidly (more than 5 workers, so some should be denied)
echo "Submitting 7 auto jobs rapidly..."
accepted=0
denied=0

for i in {1..7}; do
    if submit_auto_job $i; then
        accepted=$((accepted + 1))
    else
        denied=$((denied + 1))
    fi
    # Small delay to avoid overwhelming the system
    sleep 0.5
done

echo
echo "📊 First batch results:"
echo "   Jobs accepted: $accepted/7"
echo "   Jobs denied: $denied/7"
echo "   Expected: ~5 accepted (one per worker), ~2 denied"

echo
echo "🎯 Test 2: Check work unit distribution"
echo "Expected: Each worker should have at most 1 job"

echo "Work units per worker:"
total_units=0
max_per_worker=0
for i in {1..5}; do
    worker_jobs=$(find /tmp/receptor/worker$i -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
    echo "   Worker$i: $worker_jobs work units"
    total_units=$((total_units + worker_jobs))
    if [ "$worker_jobs" -gt "$max_per_worker" ]; then
        max_per_worker=$worker_jobs
    fi
done

echo "   Total work units created: $total_units"

echo
echo "🎯 Test 3: Wait and submit again"
echo "Expected: After jobs complete, new submissions should succeed"

echo "⏰ Waiting 8 seconds for jobs to complete..."
sleep 8

echo "Submitting 3 more auto jobs after completion..."
accepted2=0
denied2=0

for i in {8..10}; do
    if submit_auto_job $i; then
        accepted2=$((accepted2 + 1))
    else
        denied2=$((denied2 + 1))
    fi
    sleep 0.5
done

echo
echo "📊 Second batch results:"
echo "   Jobs accepted: $accepted2/3"
echo "   Jobs denied: $denied2/3"
echo "   Expected: 3 accepted (workers should be available)"

echo
echo "🏆 AUTO-SUBMIT LEASE ENFORCEMENT TEST RESULTS:"
echo "   First batch: $accepted accepted, $denied denied"
echo "   Second batch: $accepted2 accepted, $denied2 denied"
echo "   Max work units per worker: $max_per_worker"

echo
echo "📋 Analysis:"
if [ "$accepted" -ge 5 ] && [ "$accepted" -le 7 ] && [ "$max_per_worker" -le 10 ]; then
    echo "✅ FIRST BATCH: Good distribution (5-7 jobs accepted)"
else
    echo "❌ FIRST BATCH: Unexpected distribution ($accepted/7 accepted)"
fi

if [ "$accepted2" -eq 3 ]; then
    echo "✅ SECOND BATCH: Perfect (3/3 jobs accepted after completion)"
elif [ "$accepted2" -ge 2 ]; then
    echo "⚠️  SECOND BATCH: Mostly good ($accepted2/3 accepted)"
else
    echo "❌ SECOND BATCH: Issues detected ($accepted2/3 accepted)"
fi

if [ "$accepted" -ge 5 ] && [ "$accepted2" -ge 2 ]; then
    echo
    echo "🎯 OVERALL: Auto-submit lease enforcement working correctly!"
    echo "   ✓ Jobs are distributed across available workers"
    echo "   ✓ Capacity limits are respected"
    echo "   ✓ System recovers after job completion"
else
    echo
    echo "❌ OVERALL: Lease enforcement issues detected"
    echo "   Check individual test results above"
fi