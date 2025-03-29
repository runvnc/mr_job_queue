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
worker_running = False

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

# Commands (for LLM agents)
@command()
async def submit_job(instructions, agent_name, job_type=None, metadata=None, context=None):
    """Submit a job to be processed by an agent.
    
    Args:
        instructions: Text instructions for the agent
        agent_name: Name of the agent to process this job
        job_type: Optional category/type of job
        metadata: Optional additional data for the job
    
    Returns:
        job_id: ID of the submitted job
    """
    # Get the service and delegate
    add_job_service = await service_manager.get_service("add_job")
    return await add_job_service(
        instructions=instructions,
        agent_name=agent_name, 
        job_type=job_type, 
        metadata=metadata, 
        context=context
    )

# Services (for plugin-to-plugin integration)
@service()
async def add_job(instructions, agent_name, job_type=None, metadata=None, context=None):
    """Submit a job to be processed by an agent (service for plugin-to-plugin integration).
    
    Args:
        instructions: Text instructions for the agent
        agent_name: Name of the agent to process this job
        job_type: Optional category/type of job
        metadata: Optional additional data for the job
    
    Returns:
        job_id: ID of the submitted job
    """
    # Generate unique job ID
    job_id = f"job_{uuid.uuid4()}"
    
    debug_box("add_job")
    debug_box(instructions)

    # Create job data
    job_data = {
        "id": job_id,
        "agent_name": agent_name,
        "instructions": instructions,
        "username": context.username if context else "system",
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "started_at": None,
        "completed_at": None,
        "plugin": job_type.split(".")[0] if job_type and "." in job_type else None,
        "job_type": job_type,
        "result": None,
        "error": None,
        "log_id": None,
        "metadata": metadata or {}
    }
    
    # Save job file
    job_path = f"{QUEUED_DIR}/{job_id}.json"
    async with FileLock(job_path):
        async with aiofiles.open(job_path, "w") as f:
            await f.write(json.dumps(job_data, indent=2))
    
    # Append to index
    if not os.path.exists(JOB_INDEX):
        async with aiofiles.open(JOB_INDEX, "w") as f:
            pass  # Create empty file
    
    async with FileLock(JOB_INDEX):
        async with aiofiles.open(JOB_INDEX, "a") as f:
            index_entry = {
                "id": job_id,
                "status": "queued",
                "agent_name": agent_name,
                "job_type": job_type,
                "created_at": job_data["created_at"],
                "username": job_data["username"]
            }
            await f.write(json.dumps(index_entry) + "\n")
    
    # Ensure worker is running
    await ensure_worker_running()
    
    return {"job_id": job_id}

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

@service()
async def process_job(job_id, job_data):
    """Process a single job using the run_task service"""
    try:
        print(f"Processing job {job_id}")
        
        # Update job status to active
        job_data["status"] = "active"
        job_data["started_at"] = datetime.now().isoformat()
        job_data["updated_at"] = datetime.now().isoformat()
        
        # Write updated job data
        active_path = f"{ACTIVE_DIR}/{job_id}.json"
        async with FileLock(active_path):
            async with aiofiles.open(active_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        # Update index
        await update_job_index(job_id, "active")
        
        # Run the task using run_task service
        run_task = await service_manager.get_service("run_task")
        
        print(f"Running task for job {job_id} with agent {job_data['agent_name']}")
        text, full_results, log_id = await run_task(
            instructions=job_data["instructions"],
            agent_name=job_data["agent_name"],
            user=job_data["username"],
            log_id=None,  # Let the service generate a log_id
            retries=3
        )
        
        print(f"Task completed for job {job_id}, log_id: {log_id}")
        
        # Update job with results
        job_data["status"] = "completed"
        job_data["result"] = text
        job_data["log_id"] = log_id
        job_data["completed_at"] = datetime.now().isoformat()
        job_data["updated_at"] = datetime.now().isoformat()
        
        # Write to completed directory
        completed_path = f"{COMPLETED_DIR}/{job_id}.json"
        async with FileLock(completed_path):
            async with aiofiles.open(completed_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        # Remove from active
        try:
            await aiofiles.os.remove(active_path)
        except:
            pass
        
        # Update index
        await update_job_index(job_id, "completed")
        
        return True
        
    except Exception as e:
        print(f"Error processing job {job_id}: {e}")
        print(traceback.format_exc())
        
        # Handle failure
        try:
            error_message = str(e)
            traceback_str = traceback.format_exc()
            
            # Update job with error
            job_data["status"] = "failed"
            job_data["error"] = f"{error_message}\n{traceback_str}"
            job_data["completed_at"] = datetime.now().isoformat()
            job_data["updated_at"] = datetime.now().isoformat()
            
            # Write to failed directory
            failed_path = f"{FAILED_DIR}/{job_id}.json"
            async with FileLock(failed_path):
                async with aiofiles.open(failed_path, "w") as f:
                    await f.write(json.dumps(job_data, indent=2))
            
            # Remove from active
            active_path = f"{ACTIVE_DIR}/{job_id}.json"
            if os.path.exists(active_path):
                try:
                    await aiofiles.os.remove(active_path)
                except:
                    pass
            
            # Update index
            await update_job_index(job_id, "failed")
        except Exception as inner_e:
            print(f"Error handling job failure: {inner_e}")
        
        return False

async def worker_loop():
    """Main worker loop that processes queued jobs"""
    global worker_running
    
    worker_running = True
    
    print("Job queue worker started")
    
    while worker_running:
        try:
            # Get list of queued jobs
            queued_jobs = [f for f in os.listdir(QUEUED_DIR) if f.endswith('.json')]
            
            if not queued_jobs:
                # No jobs to process, sleep briefly
                await asyncio.sleep(5)
                continue
            
            print(f"Found {len(queued_jobs)} jobs to process")
            
            # Process each job
            for job_file in queued_jobs:
                if not worker_running:
                    break
                    
                job_path = f"{QUEUED_DIR}/{job_file}"
                job_id = job_file.replace(".json", "")
                
                try:
                    # Read job data
                    job_data = await get_job_data(job_id)
                    if not job_data:
                        print(f"Could not read job data for {job_id}")
                        continue
                    
                    print(f"Processing job {job_id}")
                    
                    # Move job file to active directory
                    active_path = f"{ACTIVE_DIR}/{job_file}"
                    try:
                        await aiofiles.os.rename(job_path, active_path)
                    except Exception as move_error:
                        print(f"Error moving job file: {move_error}")
                        continue
                    
                    # Process the job
                    await process_job(job_id, job_data)
                    
                except Exception as e:
                    print(f"Error processing job {job_file}: {e}")
                    print(traceback.format_exc())
                
                # Small pause between jobs
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"Worker loop error: {e}")
            print(traceback.format_exc())
            await asyncio.sleep(10)  # Longer sleep on error
    
    print("Job queue worker stopped")

@service()
async def start_worker():
    """Start the job queue worker process"""
    global worker_task, worker_running
    
    if worker_task and not worker_task.done():
        return {"status": "Worker already running"}
    
    # Start the worker in a background task
    worker_task = asyncio.create_task(worker_loop())
    
    return {"status": "Worker started"}

async def ensure_worker_running():
    """Ensure the worker is running, start it if not"""
    global worker_task, worker_running
    
    if worker_task and not worker_task.done():
        return
    
    await start_worker()

@hook()
async def startup(app, context=None):
    """Start the worker when the plugin starts"""
    asyncio.create_task(ensure_worker_running())
    return {"status": "Worker initialized"}

@hook()
async def quit(context=None):
    """Stop the worker when the plugin stops"""
    global worker_running, worker_task
    
    worker_running = False
    
    if worker_task and not worker_task.done():
        try:
            # Give the worker a chance to finish gracefully
            await asyncio.wait_for(worker_task, timeout=10)
        except asyncio.TimeoutError:
            # If it doesn't finish in time, cancel it
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
    
    return {"status": "Worker stopped"}

