#!/bin/bash

echo "🤖 === FINAL AUTO-SUBMIT TEST ==="
echo "Testing automatic worker selection with proper job creation"
echo

# Function to submit a job and handle stdin properly
submit_auto_job() {
    local job_num=$1
    echo "$(date '+%H:%M:%S'): Submitting auto job $job_num..."

    # Submit with empty stdin (like the regular submit test)
    result=$(timeout 10 bash -c '(echo "{\"command\":\"work\",\"subcommand\":\"submit_auto\",\"worktype\":\"countdown\"}" && echo) | nc -U /tmp/control1.sock' 2>/dev/null)

    # Extract work unit ID from the JSON response
    if echo "$result" | grep -q '"unitid"'; then
        # Extract unitid from JSON response (last line should be JSON)
        json_line=$(echo "$result" | tail -1)
        work_unit_id=$(echo "$json_line" | grep -o '"unitid":"[^"]*"' | cut -d'"' -f4)
        selected_worker=$(echo "$json_line" | grep -o '"selected_worker":"[^"]*"' | cut -d'"' -f4)

        # Check if work unit directory was created
        work_unit_dir=$(find /tmp/receptor/worker* -name "$work_unit_id" -type d 2>/dev/null | head -1)
        if [ -n "$work_unit_dir" ]; then
            echo "✅ Auto job $job_num: Created work unit $work_unit_id on $selected_worker"
            echo "    Directory: $work_unit_dir"
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

echo
echo "✅ Testing auto-submit job creation and distribution..."

# Record initial state
initial_count=$(find /tmp/receptor/worker* -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
echo "📊 Initial work units: $initial_count"

# Submit 5 jobs sequentially
successful_jobs=0
workers_used=()

for i in {1..5}; do
    if submit_auto_job $i; then
        successful_jobs=$((successful_jobs + 1))
    fi
    sleep 1  # Small delay between jobs
done

# Check final state
final_count=$(find /tmp/receptor/worker* -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
new_jobs=$((final_count - initial_count))

echo
echo "📊 Results Summary:"
echo "   Jobs submitted: 5"
echo "   Jobs successfully created: $new_jobs"
echo "   Expected new work units: 5"

if [ "$new_jobs" -eq 5 ]; then
    echo "✅ All jobs created work units successfully!"
else
    echo "⚠️  Only $new_jobs out of 5 jobs created work units"
fi

echo
echo "📂 Work unit distribution by worker:"
for i in {1..5}; do
    worker_jobs=$(find /tmp/receptor/worker$i -name "control1*" -type d 2>/dev/null | wc -l | tr -d ' ')
    if [ "$worker_jobs" -gt 0 ]; then
        echo "   Worker$i: $worker_jobs work units"
    fi
done

echo
echo "🕒 Waiting 8 seconds for jobs to complete..."
sleep 8

echo
echo "📄 Checking job outputs:"
total_completed=0
for i in {1..5}; do
    completed_jobs=$(find /tmp/receptor/worker$i -name "stdout" -path "*/control1*" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$completed_jobs" -gt 0 ]; then
        echo "   Worker$i: $completed_jobs completed jobs"
        total_completed=$((total_completed + completed_jobs))

        # Show sample output from one job
        sample_output=$(find /tmp/receptor/worker$i -name "stdout" -path "*/control1*" 2>/dev/null | head -1)
        if [ -f "$sample_output" ]; then
            first_line=$(head -1 "$sample_output" 2>/dev/null)
            echo "     Sample output: $first_line"
        fi
    fi
done

echo
echo "🏆 FINAL TEST RESULTS:"
echo "   Work units created: $new_jobs/5"
echo "   Jobs completed: $total_completed"
echo "   Workers utilized: $(find /tmp/receptor/worker* -name "control1*" -type d 2>/dev/null | cut -d'/' -f4 | sort | uniq | wc -l | tr -d ' ')"

if [ "$new_jobs" -eq 5 ] && [ "$total_completed" -gt 0 ]; then
    echo "🎯 SUCCESS: Auto-selecting work submission is fully functional!"
    echo "   ✓ Jobs are properly created and distributed"
    echo "   ✓ Workers execute jobs and produce output"
    echo "   ✓ No explicit node targeting required"
else
    echo "❌ Issues detected in auto-selecting work submission"
fi