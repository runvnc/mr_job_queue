# MindRoot Job Queue

A simple file-based job queue plugin for MindRoot that allows running agent tasks asynchronously.

## Overview

The MindRoot Job Queue (`mr_job_queue`) provides a lightweight, file-based system for:

- Submitting background jobs to be processed by agents
- Tracking job status and results
- Viewing job history and statistics
- Managing the job queue through a web UI

Unlike database-dependent queue systems, this plugin uses the filesystem for storage, making it portable and easy to set up with any MindRoot installation.

## Installation

1. Navigate to the plugin directory:
   ```bash
   cd /xfiles/plugins_ah/mr_job_queue
   ```

2. Install the plugin:
   ```bash
   pip install -e .
   ```

3. Restart MindRoot to activate the plugin.

## Usage

### Submitting Jobs

You can submit jobs either through the API or from other plugins:

```python
from lib.providers.services import service_manager

# Example from another plugin
async def start_background_task(data, context=None):
    # Get the submit_job service
    submit_job = await service_manager.get_service("submit_job")
    
    # Create instructions for the agent
    instructions = f"Process the following data: {json.dumps(data)}"
    
    # Submit the job
    result = await submit_job(
        instructions=instructions,    # What the agent should do
        agent_name="data_processor",   # Which agent should process this
        job_type="my_plugin.process",  # Optional categorization
        metadata={"original_data": data},  # Optional additional data
        context=context
    )
    
    # Return the job ID
    return {"job_id": result["job_id"]}
```

### Checking Job Status

```python
from lib.providers.services import service_manager

async def check_job_status(job_id, context=None):
    get_job_status = await service_manager.get_service("get_job_status")
    job = await get_job_status(job_id, context=context)
    return job
```

### Web Dashboard

The plugin provides a web dashboard at `/mr_job_queue/` that shows:

- Job queue statistics
- Listing of all jobs with filtering options
- Detailed view of individual jobs
- Ability to cancel queued jobs

## API Reference

### Commands

#### `submit_job`

Submit a new job to the queue.

- **Parameters:**
  - `instructions`: Text instructions for the agent
  - `agent_name`: Name of the agent to process this job
  - `job_type`: (Optional) Category/type of job
  - `metadata`: (Optional) Additional data for the job
  
- **Returns:** `{"job_id": "job_uuid"}`

#### `get_job_status`

Get the current status and details of a job.

- **Parameters:**
  - `job_id`: ID of the job to check
  
- **Returns:** Complete job data object

#### `get_jobs`

Get a list of jobs, optionally filtered.

- **Parameters:**
  - `status`: (Optional) Filter by job status (queued, active, completed, failed)
  - `job_type`: (Optional) Filter by job type
  - `username`: (Optional) Filter by username who created the job
  - `limit`: (Optional) Maximum number of jobs to return (default: 50)
  
- **Returns:** Array of job objects

#### `cancel_job`

Cancel a queued job.

- **Parameters:**
  - `job_id`: ID of the job to cancel
  
- **Returns:** `{"success": true, "job_id": "job_uuid"}`

#### `cleanup_jobs`

Clean up old jobs.

- **Parameters:**
  - `status`: Status of jobs to clean up (completed, failed)
  - `older_than_days`: Remove jobs older than this many days
  
- **Returns:** `{"removed_count": 5}`

### Services

#### `start_worker`

Start the job queue worker process.

- **Returns:** `{"status": "Worker started"}`

#### `process_job`

Process a single job (internal service).

- **Parameters:**
  - `job_id`: ID of the job to process
  - `job_data`: Job data object
  
- **Returns:** `true` if successful, `false` otherwise

## Job Object Structure

```json
{
  "id": "job_uuid",
  "agent_name": "agent_name",
  "instructions": "Task instructions for the agent",
  "username": "user_who_created_job",
  "status": "queued|active|completed|failed",
  "created_at": "ISO-timestamp",
  "updated_at": "ISO-timestamp",
  "started_at": null,
  "completed_at": null,
  "plugin": "plugin_that_created_job",
  "job_type": "job_type_name",
  "result": null,
  "error": null,
  "log_id": null,
  "metadata": {}
}
```

## Implementation Details

### Directory Structure

Jobs are stored as JSON files in the following directories:

```
data/jobs/
├── queued/     # Jobs waiting to be processed
├── active/     # Jobs currently being processed
├── completed/  # Successfully completed jobs
├── failed/     # Failed jobs
└── job_index.jsonl  # Simple job index for quick lookups
```

### Worker Process

A background worker process runs continuously to process queued jobs. The worker:

1. Scans the `queued` directory for jobs
2. Moves each job to the `active` directory
3. Processes the job using the `run_task` service
4. Moves the job to either `completed` or `failed` based on result
5. Updates the job file with results and log ID

The worker starts automatically when MindRoot starts and can be restarted if needed.

## Troubleshooting

### Job Stuck in "Active" State

If a job appears stuck in the active state, it may be due to a crash during processing. You can manually move the job file from the `active` directory to the `queued` directory to retry processing.

### Worker Not Processing Jobs

If jobs remain in the queued state and aren't being processed:

1. Check MindRoot logs for errors
2. Restart MindRoot to restart the worker process
3. Ensure the agent specified in the job exists and has necessary commands enabled

## Future Enhancements

Potential future improvements include:

- Job priorities
- Scheduled/recurring jobs
- Multiple worker processes
- Worker load balancing
- Job dependencies
