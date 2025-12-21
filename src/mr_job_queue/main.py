import json
import os
import uuid
import time
from datetime import datetime
import asyncio
import shutil
import traceback
import aiofiles
import aiofiles.os
import httpx

from lib.providers.services import service, service_manager
from lib.providers.hooks import hook, hook_manager
from lib.providers.hooks import hook
from lib.utils.debug import debug_box
from lib.chatcontext import get_context

# Local imports
from .filelock import FileLock
from .helpers import get_job_data, sanitize_job_type

debug_box("----------------------------------- JOB_QUEUE STARTING ----------------------------------------------------")

# Configuration handling
CONFIG_PATH = "data/jobs/config.json"
def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    return {
        "mode": "standalone",
        "master_url": "",
        "max_concurrent": 5,
        "max_concurrent_per_type": 1
    }

def save_config(config):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=2)

# Job directory structure
JOB_DIR = "data/jobs"  # Base directory for all jobs
QUEUED_DIR = f"{JOB_DIR}/queued"  # Will contain job_type subdirectories
ACTIVE_DIR = f"{JOB_DIR}/active"  # Will contain job_type subdirectories
COMPLETED_DIR = f"{JOB_DIR}/completed"  # Will contain job_type subdirectories
DEFAULT_JOB_TYPE = "default"  # Default job type for backward compatibility
FAILED_DIR = f"{JOB_DIR}/failed"
PAUSED_DIR = f"{JOB_DIR}/paused"
# Note: JOB_INDEX removed - we now scan directories directly

# Concurrency settings - Read from environment variable
try:
    MAX_CONCURRENT_JOBS = int(os.getenv('JOB_QUEUE_MAX_CONCURRENT', '5'))
except ValueError:
    print("Warning: Invalid value for JOB_QUEUE_MAX_CONCURRENT env var, defaulting to 5.")
    MAX_CONCURRENT_JOBS = 5


# Concurrency settings per job type - Read from environment variable
try:
    MAX_CONCURRENT_PER_TYPE = int(os.getenv('JOB_QUEUE_MAX_CONCURRENT_PER_TYPE', '1'))
except ValueError:
    print("Warning: Invalid value for JOB_QUEUE_MAX_CONCURRENT_PER_TYPE env var, defaulting to 1.")
    MAX_CONCURRENT_PER_TYPE = 1

# Ensure directories exist
os.makedirs(QUEUED_DIR, exist_ok=True)
os.makedirs(ACTIVE_DIR, exist_ok=True)
os.makedirs(COMPLETED_DIR, exist_ok=True)
os.makedirs(FAILED_DIR, exist_ok=True)
os.makedirs(PAUSED_DIR, exist_ok=True)
# Worker process state
worker_task = None
worker_running = asyncio.Event() # Use Event for clearer start/stop signaling
semaphore = None
active_job_tasks = set()
# Dictionary to store semaphores for each job type
job_type_semaphores = {}
# Dictionary to store tasks for each job type
job_type_tasks = {}
# Completion notification for jobs being waited on
job_completion_events = {}   # job_id -> asyncio.Event
job_completion_results = {}  # job_id -> result data


# ---------------------------------------------------------------------------
# add_job service - creates a job file in the 'queued' directory
# ---------------------------------------------------------------------------
@service()
async def add_job(instructions, agent_name, job_type=None, username=None, metadata=None, job_id=None, llm=None, context=None):
    if job_id is None:
        job_id = f"job_{uuid.uuid4()}"
  
    if username is None:
        username = getattr(context, 'username', 'system')

    original_job_type = job_type or DEFAULT_JOB_TYPE
    sjt = sanitize_job_type(original_job_type)

    debug_box(f"add_job called by {username} for type {sjt}")
    
    job_data = {
        "id": job_id,
        "agent_name": agent_name,
        "instructions": instructions,
        "username": username,
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "started_at": None,
        "completed_at": None,
        "plugin": sjt.split(".")[0] if "." in sjt else None,
        "job_type": original_job_type,
        "result": None,
        "llm": llm,
        "error": None,
        "log_id": job_id,
        "metadata": metadata or {}
    }
    job_data["metadata"]["sanitized_job_type"] = sjt
    
    queued_job_type_dir = f"{QUEUED_DIR}/{sjt}"
    await aiofiles.os.makedirs(queued_job_type_dir, exist_ok=True)
    
    job_path = f"{queued_job_type_dir}/{job_id}.json"
    try:
        lock = FileLock(job_path)
        async with lock:
            async with aiofiles.open(job_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
    except Exception as e:
        print(f"Error writing job file {job_path}: {e}")
        return {"error": f"Failed to write job file: {e}"}

    # Since we are no longer using an index, the call to update_job_index is removed.

    # Ensure a worker for this job type is running.
    asyncio.create_task(ensure_job_type_worker_running(sjt))
    
    return {"job_id": job_id}

# ---------------------------------------------------------------------------
# execute_job_core - The actual execution logic shared by local and remote workers
# ---------------------------------------------------------------------------
async def execute_job_core(job_id, job_data):
    """Executes the task and returns updated job_data. Does NOT handle filesystem."""
    from .helpers import update_job_index # It's a no-op, but call keeps logic flow
    try:
        job_data["status"] = "active"
        job_data["started_at"] = datetime.now().isoformat()
        job_data["updated_at"] = datetime.now().isoformat()

        print(f"Running task for job {job_id} with agent {job_data['agent_name']}")
        if not hasattr(service_manager, 'run_task'):
             raise RuntimeError("run_task service is not available via service_manager")
        
        instr = job_data["instructions"]
        if job_data.get('metadata'):
             instr += "\n\nMetadata:\n" + json.dumps(job_data['metadata'])
        
        # Get retries and parent_log_id from metadata if available
        metadata = job_data.get('metadata', {})
        retries = metadata.get('retries', 3)
        parent_log_id = metadata.get('parent_log_id', None)

        text, _, log_id = await service_manager.run_task(
            instructions=instr,
            agent_name=job_data["agent_name"],
            user=job_data["username"],
            retries=retries,
            llm=job_data["llm"],
            log_id=job_id,
            parent_log_id=parent_log_id,
            context=None
        )
        
        print(f"Task completed for job {job_id}, log_id: {log_id}")
        
        # Check if the job was paused by the agent
        try:
            task_context = await get_context(job_id, job_data.get('username', 'system'))
            if task_context and task_context.data.get('job_pause_requested'):
                # Job was paused - move to paused directory instead of completed
                pause_reason = task_context.data.get('job_pause_reason', 'Pause requested')
                job_data.update({
                    "status": "paused",
                    "paused_at": datetime.now().isoformat(),
                    "pause_reason": pause_reason,
                    "updated_at": datetime.now().isoformat(),
                    "result": text  # Save any partial result
                })
                return job_data
        except Exception as e:
            print(f"Error checking pause status for job {job_id}: {e}")
        
        job_data.update({
            "status": "completed",
            "result": text,
            "log_id": log_id,
            "completed_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        return job_data
        
    except Exception as e:
        print(f"Error processing job {job_id}: {e}\n{traceback.format_exc()}")
        job_data.update({
            "status": "failed",
            "error": f"{str(e)}\n{traceback.format_exc()}",
            "completed_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        return job_data

# ---------------------------------------------------------------------------
# process_job service - executes the agent task for a given job (Local/Master)
# ---------------------------------------------------------------------------
@service()
async def process_job(job_id, job_data, job_type=None):
    from .helpers import update_job_index
    original_job_type = job_type or job_data.get("job_type", DEFAULT_JOB_TYPE)
    sjt = sanitize_job_type(original_job_type)
    active_path = f"{ACTIVE_DIR}/{sjt}/{job_id}.json"
    
    try:
        # Initial update to active
        async with FileLock(active_path):
            async with aiofiles.open(active_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        # Execute core logic
        job_data = await execute_job_core(job_id, job_data)
        
        # Handle state transition on filesystem
        status = job_data.get("status")
        if status == "paused":
            target_dir = PAUSED_DIR
        elif status == "completed":
            target_dir = COMPLETED_DIR
        else:
            target_dir = FAILED_DIR

        target_path = f"{target_dir}/{sjt}/{job_id}.json"
        await aiofiles.os.makedirs(os.path.dirname(target_path), exist_ok=True)
        
        async with FileLock(target_path):
            async with aiofiles.open(target_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        try:
            await aiofiles.os.remove(active_path)
        except FileNotFoundError: pass

        await update_job_index(job_id, status, sjt)
        
        # Trigger hook
        await hook_manager.job_ended(status, job_data, job_data.get("result"), context=None)
        
        # Signal completion (with failure) to any waiters
        if job_id in job_completion_events:
            job_completion_results[job_id] = job_data
            job_completion_events[job_id].set()
        return False

# ---------------------------------------------------------------------------
# wait_for_job service - waits for a job to complete and returns result
# ---------------------------------------------------------------------------
@service()
async def wait_for_job(job_id, timeout=600, context=None):
    """
    Wait for a job to complete and return its result.
    
    Uses event-based notification for efficiency - no polling required.
    
    Args:
        job_id: The job ID to wait for
        timeout: Maximum seconds to wait (default 600 = 10 minutes)
        context: Optional context object
    
    Returns:
        Job data dict with result on success, or dict with 'error' key on failure/timeout
    """
    # Check if job already completed/failed
    job_data = await get_job_data(job_id)
    if job_data and job_data.get("status") in ("completed", "failed"):
        return job_data
    
    # Create event for this job and wait
    event = asyncio.Event()
    job_completion_events[job_id] = event
    
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        
        # Get result from cache or filesystem
        result = job_completion_results.pop(job_id, None)
        if result:
            return result
        return await get_job_data(job_id)
        
    except asyncio.TimeoutError:
        return {"error": f"Job {job_id} timed out after {timeout} seconds", "job_id": job_id, "status": "timeout"}
    finally:
        # Cleanup
        job_completion_events.pop(job_id, None)
        job_completion_results.pop(job_id, None)

# ---------------------------------------------------------------------------
# get_job_data service - retrieves job data by ID
# ---------------------------------------------------------------------------
@service()
async def get_job_data_service(job_id, context=None):
    """
    Get job data by job ID. Searches queued, active, completed, and failed directories.
    
    Returns job data dict or None if not found.
    """
    return await get_job_data(job_id)

# ---------------------------------------------------------------------------
# Worker implementation
# ---------------------------------------------------------------------------
async def run_job_and_release(job_id, job_data, sem, job_type=None):
    """Wrapper to run process_job and release semaphore."""
    try:
        print(f"Starting processing for job {job_id}")
        await process_job(job_id, job_data, job_type=job_type)
        print(f"Finished processing job {job_id}")
    except Exception as e:
        print(f"Exception in run_job_and_release for {job_id}: {e}")
    finally:
        print(f"Releasing semaphore for job {job_id}")
        sem.release()

async def job_type_worker_loop(job_type):
    """Worker loop for a specific job type."""
    if job_type not in job_type_semaphores:
        job_type_semaphores[job_type] = asyncio.Semaphore(MAX_CONCURRENT_PER_TYPE)
    sem = job_type_semaphores[job_type]
    
    config = load_config()
    if config.get("mode") == "worker":
        await worker_remote_loop(job_type, sem, config)
        return

    await worker_local_loop(job_type, sem)

async def worker_remote_loop(job_type, sem, config):
    """Worker loop that polls a remote master."""
    master_url = config.get("master_url", "").rstrip("/")
    if not master_url:
        print(f"Worker mode active but no master_url configured for {job_type}")
        return

    print(f"Remote worker loop started for {job_type} -> {master_url}")
    
    headers = {}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    async with httpx.AsyncClient(timeout=60.0, headers=headers) as client:
        while worker_running.is_set():
            try:
                await sem.acquire()
                # Lease a job from master
                resp = await client.post(f"{master_url}/api/jobs/lease", json={"job_type": job_type})
                
                if resp.status_code == 204: # Empty
                    sem.release()
                    await asyncio.sleep(10)
                    continue
                
                job_data = resp.json()
                job_id = job_data["id"]
                
                # Process and report
                task = asyncio.create_task(run_remote_job_and_report(job_id, job_data, sem, job_type, client, master_url))
                active_job_tasks.add(task)
                task.add_done_callback(active_job_tasks.discard)

            except Exception as e:
                print(f"Remote worker error: {e}")
                await asyncio.sleep(10)

async def worker_local_loop(job_type, sem):
    """Original local filesystem scanning loop."""
    
    print(f"Job queue worker for type '{job_type}' started (limit: {MAX_CONCURRENT_PER_TYPE})")
    queued_job_type_dir = f"{QUEUED_DIR}/{job_type}"
    active_job_type_dir = f"{ACTIVE_DIR}/{job_type}"
    
    while worker_running.is_set():
        try:
            if not await aiofiles.os.path.isdir(queued_job_type_dir):
                await asyncio.sleep(10) # Dir may not exist yet
                continue

            job_files = sorted(await aiofiles.os.listdir(queued_job_type_dir))
            if not job_files:
                await asyncio.sleep(5)
                continue

            for job_file in job_files:
                if not job_file.endswith('.json'): continue
                if not worker_running.is_set(): break

                job_path = os.path.join(queued_job_type_dir, job_file)
                job_id = job_file.replace(".json", "")
                
                print(f"Attempting to acquire semaphore for job {job_id}...")
                await sem.acquire()
                print(f"Semaphore acquired for job {job_id}")

                if not await aiofiles.os.path.exists(job_path):
                    sem.release()
                    continue

                active_path = os.path.join(active_job_type_dir, job_file)
                await aiofiles.os.makedirs(active_job_type_dir, exist_ok=True)
                await aiofiles.os.rename(job_path, active_path)
                print(f"Moved {job_id} to active directory for type {job_type}")
                
                job_data = await get_job_data(job_id)
                if not job_data:
                    print(f"Could not read job data for {job_id}, moving to failed.")
                    failed_path = f"{FAILED_DIR}/{job_id}.json"
                    error_data = {"id": job_id, "status": "failed", "error": "Failed to read data after move."}
                    async with aiofiles.open(failed_path, "w") as f:
                        await f.write(json.dumps(error_data, indent=2))
                    sem.release()
                    continue
                
                task = asyncio.create_task(run_job_and_release(job_id, job_data, sem, job_type))
                active_job_tasks.add(task)
                task.add_done_callback(active_job_tasks.discard)

        except Exception as e:
            print(f"Worker loop for '{job_type}' encountered an error: {e}")
            await asyncio.sleep(10)

async def run_remote_job_and_report(job_id, job_data, sem, job_type, client, master_url):
    """Run job locally and report result back to master."""
    try:
        print(f"Executing remote job {job_id}...")
        job_data = await execute_job_core(job_id, job_data)
        
        print(f"Reporting remote job {job_id} back to master...")
        resp = await client.post(f"{master_url}/api/jobs/report/{job_id}", json=job_data)
        if not resp.is_success:
            print(f"Failed to report job {job_id}: {resp.text}")
    except Exception as e:
        print(f"Error in run_remote_job_and_report: {e}")
    finally:
        sem.release()
    
    print(f"Job queue worker loop for type '{job_type}' finished.")

# ---------------------------------------------------------------------------
# Worker lifecycle management
# ---------------------------------------------------------------------------
@service()
async def ensure_job_type_worker_running(job_type):
    """Ensure a worker for a specific job type is running."""
    if not worker_running.is_set():
        return
        
    sjt = sanitize_job_type(job_type)
    if sjt in job_type_tasks and not job_type_tasks[sjt].done():
        return
    
    print(f"Starting on-demand worker for job type: {sjt}")
    job_type_tasks[sjt] = asyncio.create_task(job_type_worker_loop(sjt))

async def start_job_type_workers():
    """Start workers for job types with pending jobs at startup."""
    if not await aiofiles.os.path.exists(QUEUED_DIR):
        return

    for job_type_dir in await aiofiles.os.listdir(QUEUED_DIR):
        job_type_path = os.path.join(QUEUED_DIR, job_type_dir)
        if await aiofiles.os.path.isdir(job_type_path):
            try:
                files = await aiofiles.os.listdir(job_type_path)
                if any(f.endswith('.json') for f in files):
                    await ensure_job_type_worker_running(job_type_dir)
            except Exception as e:
                print(f"Error checking job type dir {job_type_dir}: {e}")

@hook()
async def startup(app, context=None):
    """Start the worker system when the plugin loads."""
    print("Plugin startup: Initializing job queue worker system.")
    worker_running.set()
    asyncio.create_task(start_job_type_workers())
    return {"status": "Worker system initialized"}

@hook()
async def quit(context=None):
    """Stop the worker and cleanup active tasks when the plugin stops."""
    print("Plugin quit: Stopping job queue worker.")
    worker_running.clear()
    
    all_tasks = list(job_type_tasks.values()) + list(active_job_tasks)
    if all_tasks:
        print(f"Waiting for {len(all_tasks)} active tasks to complete...")
        try:
            await asyncio.wait_for(asyncio.gather(*all_tasks, return_exceptions=True), timeout=30)
            print("All active tasks finished.")
        except asyncio.TimeoutError:
             print(f"Warning: {len(all_tasks)} tasks did not complete within 30s.")
    
    print("Job queue worker shutdown complete.")
    return {"status": "Worker stopped"}
