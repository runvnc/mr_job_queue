from lib.providers.commands import command
from lib.providers.services import service, service_manager
from lib.providers.hooks import hook
import json
import os
import uuid
import time
from datetime import datetime
import asyncio
import shutil
import traceback
import fcntl
import aiofiles
import aiofiles.os
from lib.utils.debug import debug_box


# Job directory structure
JOB_DIR = "data/jobs"
QUEUED_DIR = f"{JOB_DIR}/queued"
ACTIVE_DIR = f"{JOB_DIR}/active"
COMPLETED_DIR = f"{JOB_DIR}/completed"
FAILED_DIR = f"{JOB_DIR}/failed"
JOB_INDEX = f"{JOB_DIR}/job_index.jsonl"

# Ensure directories exist
os.makedirs(QUEUED_DIR, exist_ok=True)
os.makedirs(ACTIVE_DIR, exist_ok=True)
os.makedirs(COMPLETED_DIR, exist_ok=True)
os.makedirs(FAILED_DIR, exist_ok=True)

# Worker process state
worker_task = None
worker_running = asyncio.Event() # Use Event for clearer start/stop signaling
semaphore = None
active_job_tasks = set()

# File locking helper
class FileLock:
    def __init__(self, file_path):
        self.file_path = file_path
        self.lock_file = None
    
    async def __aenter__(self):
        # Create lock file if it doesn't exist
        lock_path = f"{self.file_path}.lock"
        self.lock_file = await aiofiles.open(lock_path, "w")
        await self.lock_file.flush()
        
        # Acquire lock (blocking)
        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file:
            # Release lock
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            await self.lock_file.close()
            # Remove lock file
            try:
                await aiofiles.os.remove(f"{self.file_path}.lock")
            except:
                pass

# Helper functions
async def update_job_index(job_id, new_status):
    """Update a job's status in the index file"""
    if not os.path.exists(JOB_INDEX):
        return
    
    # Use a lock to prevent concurrent modifications
    async with FileLock(JOB_INDEX):
        temp_index = f"{JOB_INDEX}.temp"
        found = False
        
        # Create a new index with the updated status
        async with aiofiles.open(JOB_INDEX, "r") as f_in, \
                  aiofiles.open(temp_index, "w") as f_out:
            async for line in f_in:
                try:
                    job_entry = json.loads(line.strip())
                    if job_entry["id"] == job_id:
                        job_entry["status"] = new_status
                        found = True
                    await f_out.write(json.dumps(job_entry) + "\n")
                except json.JSONDecodeError:
                    await f_out.write(line)
        
        # If job wasn't in index and we're not removing it, add it
        if not found and new_status != "removed":
            # Get job data from current location
            job_data = await get_job_data(job_id)
            if job_data:
                async with aiofiles.open(temp_index, "a") as f_out:
                    index_entry = {
                        "id": job_id,
                        "status": new_status,
                        "agent_name": job_data.get("agent_name", ""),
                        "job_type": job_data.get("job_type", ""),
                        "created_at": job_data.get("created_at", datetime.now().isoformat()),
                        "username": job_data.get("username", "system")
                    }
                    await f_out.write(json.dumps(index_entry) + "\n")
        
        # Replace original with updated index
        await aiofiles.os.rename(temp_index, JOB_INDEX)

async def get_job_data(job_id):
    """Get a job's data from any of the job directories"""
    for status_dir in [QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, FAILED_DIR]:
        job_path = f"{status_dir}/{job_id}.json"
        if os.path.exists(job_path):
            async with aiofiles.open(job_path, "r") as f:
                content = await f.read()
                return json.loads(content)
    return None

@command()
async def get_job_status(job_id, context=None):
    """Get the current status and details of a job.
    
    Args:
        job_id: ID of the job to check
        
    Returns:
        Job data or error message
    """
    job_data = await get_job_data(job_id)
    if job_data:
        return job_data
    else:
        return {"error": "Job not found"}

@command()
async def get_jobs(status=None, job_type=None, username=None, limit=50, context=None):
    """Get a list of jobs, optionally filtered by status or type.
    
    Args:
        status: Filter by job status (queued, active, completed, failed)
        job_type: Filter by job type
        username: Filter by username who created the job
        limit: Maximum number of jobs to return
        
    Returns:
        List of matching jobs
    """
    jobs = []
    count = 0
    
    # Read from index for faster lookup
    if os.path.exists(JOB_INDEX):
        async with aiofiles.open(JOB_INDEX, "r") as f:
            async for line in f:
                if count >= limit:
                    break
                    
                try:
                    job_entry = json.loads(line.strip())
                    
                    # Apply filters
                    if status and job_entry.get("status") != status:
                        continue
                    if job_type and job_entry.get("job_type") != job_type:
                        continue
                    if username and job_entry.get("username") != username:
                        continue
                    
                    jobs.append(job_entry)
                    count += 1
                except json.JSONDecodeError:
                    continue
    
    return jobs

@command()
async def cancel_job(job_id, context=None):
    """Cancel a queued job.
    
    Args:
        job_id: ID of the job to cancel
        
    Returns:
        Success message or error
    """
    job_path = f"{QUEUED_DIR}/{job_id}.json"
    if not os.path.exists(job_path):
        return {"error": "Job not found or not in queued state"}
    
    # Move to failed with cancellation message
    job_data = await get_job_data(job_id)
    if not job_data:
        return {"error": "Could not read job data"}
    
    job_data["status"] = "failed"
    job_data["error"] = "Job cancelled by user"
    job_data["updated_at"] = datetime.now().isoformat()
    
    # Write to failed directory
    failed_path = f"{FAILED_DIR}/{job_id}.json"
    async with FileLock(failed_path):
        async with aiofiles.open(failed_path, "w") as f:
            await f.write(json.dumps(job_data, indent=2))
    
    # Remove from queued
    try:
        await aiofiles.os.remove(job_path)
    except:
        pass
    
    # Update index
    await update_job_index(job_id, "failed")
    
    return {"success": True, "job_id": job_id}

@command()
async def cleanup_jobs(status="completed", older_than_days=30, context=None):
    """Clean up old jobs.
    
    Args:
        status: Status of jobs to clean up (completed, failed)
        older_than_days: Remove jobs older than this many days
        
    Returns:
        Number of jobs removed
    """
    if status not in ["completed", "failed"]:
        return {"error": "Status must be 'completed' or 'failed'"}
    
    status_dir = COMPLETED_DIR if status == "completed" else FAILED_DIR
    cutoff_time = datetime.now().timestamp() - (older_than_days * 24 * 60 * 60)
    removed_count = 0
    
    for job_file in os.listdir(status_dir):
        if not job_file.endswith(".json"):
            continue
            
        job_path = f"{status_dir}/{job_file}"
        job_id = job_file.replace(".json", "")
        
        try:
            # Check file modification time
            file_mtime = os.path.getmtime(job_path)
            if file_mtime < cutoff_time:
                # Also check completed_at inside the file for accuracy
                job_data = await get_job_data(job_id)
                if job_data and "completed_at" in job_data:
                    completed_time = datetime.fromisoformat(job_data["completed_at"]).timestamp()
                    if completed_time < cutoff_time:
                        # Remove the job file
                        os.remove(job_path)
                        # Update the index
                        await update_job_index(job_id, "removed")
                        removed_count += 1
        except Exception as e:
            print(f"Error cleaning up job {job_id}: {e}")
    
    return {"removed_count": removed_count}


