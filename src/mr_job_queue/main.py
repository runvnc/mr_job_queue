import json
import os
import sys
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
import nanoid

debug_box("----------------------------------- JOB_QUEUE STARTING ----------------------------------------------------")

# Configuration handling
CONFIG_PATH = "data/jobs/config.json"
def load_config():
    """Load configuration from file, with defaults."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
    else:
        config = {}
    
    # Apply defaults
    defaults = {
        "mode": "standalone",
        "master_url": "",
        "api_key": "",
        "stale_job_timeout_minutes": 60,
        "queue_paused": False,
        "limits": {
            "default": {
                "max_global": 5,
                "max_per_instance": 1
            }
        }
    }
    
    for key, value in defaults.items():
        if key not in config:
            config[key] = value
    
    # Ensure default limits exist
    if "default" not in config.get("limits", {}):
        config.setdefault("limits", {})["default"] = defaults["limits"]["default"]
    
    return config

def is_queue_paused():
    """Check if the job queue is paused."""
    config = load_config()
    return config.get("queue_paused", False)

def set_queue_paused(paused):
    """Set the queue paused state."""
    config = load_config()
    config["queue_paused"] = paused
    save_config(config)

def get_limits_for_type(job_type, config=None):
    """Get the limits for a specific job type.
    
    Supports prefix matching: 'default' matches 'default.mr_gemini__...' etc.
    This allows LLM-specific queues to share the same limits config.
    """
    if config is None:
        config = load_config()
    limits = config.get("limits", {})
    
    # 1. Try exact match first
    if job_type in limits:
        type_limits = limits[job_type]
    else:
        # 2. Try prefix match (e.g., "default" matches "default.mr_gemini__...")
        type_limits = None
        for prefix in limits:
            if prefix != "default" and job_type.startswith(prefix + "."):
                type_limits = limits[prefix]
                break
        
        # 3. Fall back to default
        if type_limits is None:
            type_limits = limits.get("default", {})
    
    return {
        "max_global": type_limits.get("max_global", 5),
        "max_per_instance": type_limits.get("max_per_instance", 1)
    }

def save_config(config):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=2)

# ---------------------------------------------------------------------------
# Worker Identity
# ---------------------------------------------------------------------------
WORKER_ID_PATH = "data/jobs/worker_id"
WORKERS_REGISTRY_PATH = "data/jobs/workers.json"
_worker_id = None

def get_worker_id():
    """Get or generate this instance's unique worker ID."""
    global _worker_id
    if _worker_id:
        return _worker_id
    
    if os.path.exists(WORKER_ID_PATH):
        with open(WORKER_ID_PATH, 'r') as f:
            _worker_id = f.read().strip()
    else:
        _worker_id = f"w_{nanoid.generate(size=12)}"
        os.makedirs(os.path.dirname(WORKER_ID_PATH), exist_ok=True)
        with open(WORKER_ID_PATH, 'w') as f:
            f.write(_worker_id)
    
    return _worker_id

def load_workers_registry():
    """Load the workers registry from file."""
    if os.path.exists(WORKERS_REGISTRY_PATH):
        try:
            with open(WORKERS_REGISTRY_PATH, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_workers_registry(registry):
    """Save the workers registry to file."""
    os.makedirs(os.path.dirname(WORKERS_REGISTRY_PATH), exist_ok=True)
    with open(WORKERS_REGISTRY_PATH, 'w') as f:
        json.dump(registry, f, indent=2)

def update_worker_registry(worker_id, ip=None, job_id=None, remove_job=False):
    """Update worker info in the registry."""
    registry = load_workers_registry()
    
    if worker_id not in registry:
        registry[worker_id] = {
            "ip": ip,
            "first_seen": datetime.now().isoformat(),
            "last_seen": datetime.now().isoformat(),
            "active_jobs": []
        }
    
    registry[worker_id]["last_seen"] = datetime.now().isoformat()
    if ip:
        registry[worker_id]["ip"] = ip
    
    if job_id:
        if remove_job:
            if job_id in registry[worker_id]["active_jobs"]:
                registry[worker_id]["active_jobs"].remove(job_id)
        else:
            if job_id not in registry[worker_id]["active_jobs"]:
                registry[worker_id]["active_jobs"].append(job_id)
    
    save_workers_registry(registry)
    return registry[worker_id]

def count_active_jobs_for_type(job_type):
    """Count how many jobs of a type are currently active."""
    active_type_dir = os.path.join(ACTIVE_DIR, job_type)
    if not os.path.exists(active_type_dir):
        return 0
    try:
        files = os.listdir(active_type_dir)
        return len([f for f in files if f.endswith('.json')])
    except Exception:
        return 0

# Job directory structure
JOB_DIR = "data/jobs"  # Base directory for all jobs
QUEUED_DIR = f"{JOB_DIR}/queued"  # Will contain job_type subdirectories
ACTIVE_DIR = f"{JOB_DIR}/active"  # Will contain job_type subdirectories
COMPLETED_DIR = f"{JOB_DIR}/completed"  # Will contain job_type subdirectories
DEFAULT_JOB_TYPE = "default"  # Default job type for backward compatibility
FAILED_DIR = f"{JOB_DIR}/failed"
PAUSED_DIR = f"{JOB_DIR}/paused"

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
# Stale job cleanup task
stale_cleanup_task = None


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
    except Exception as e:
        print(f"Error in process_job for {job_id}: {e}")
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
    config = load_config()
    limits = get_limits_for_type(job_type, config)
    if job_type not in job_type_semaphores or job_type_semaphores[job_type]._value != limits["max_per_instance"]:
        job_type_semaphores[job_type] = asyncio.Semaphore(limits["max_per_instance"])
    sem = job_type_semaphores[job_type]
    
    config = load_config()
    mode = config.get("mode", "standalone")
    print(f"[DEBUG] job_type_worker_loop for '{job_type}': mode={mode}", flush=True)
    print(f"[DEBUG] Full config: {json.dumps(config, indent=2)}", flush=True)

    if config.get("mode") == "worker":
        print(f"[DEBUG] Entering worker_remote_loop for {job_type}", flush=True)
        await worker_remote_loop(job_type, sem, config)
        return

    print(f"[DEBUG] Entering worker_local_loop for {job_type}", flush=True)
    # Master and standalone modes both process local queue
    # Master mode also respects global limits when leasing to remote workers
    # but processes its own jobs through the same local loop
    await worker_local_loop(job_type, sem, config)

async def worker_remote_loop(job_type, sem, config):
    """Worker loop that polls a remote master."""
    master_url = config.get("master_url", "").rstrip("/")
    if not master_url:
        print(f"Worker mode active but no master_url configured for {job_type}")
        return

    my_worker_id = get_worker_id()
    poll_timeout = 15  # Long poll timeout in seconds
    
    print(f"[WORKER DEBUG] Starting remote worker loop for job_type='{job_type}'", flush=True)
    print(f"[WORKER DEBUG] Master URL: {master_url}", flush=True)
    print(f"[WORKER DEBUG] Worker ID: {my_worker_id}", flush=True)
    print(f"[WORKER DEBUG] Poll timeout: {poll_timeout}s", flush=True)
    
    headers = {}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    # HTTP timeout must be longer than poll timeout
    http_timeout = poll_timeout + 10
    
    async with httpx.AsyncClient(timeout=http_timeout, headers=headers) as client:
        print(f"[WORKER DEBUG] HTTP client created, entering poll loop", flush=True)
        while worker_running.is_set():
            try:
                print(f"[WORKER DEBUG] Acquiring semaphore for {job_type}...", flush=True)
                await sem.acquire()
                print(f"[WORKER DEBUG] Semaphore acquired, sending lease request to {master_url}/api/jobs/lease", flush=True)
                
                # Long poll for a job from master
                resp = await client.post(f"{master_url}/api/jobs/lease", json={
                    "job_type": job_type,
                    "worker_id": my_worker_id,
                    "timeout": poll_timeout
                })
                
                print(f"[WORKER DEBUG] Lease response: status={resp.status_code}", flush=True)
                
                if resp.status_code == 204:  # No jobs available after timeout
                    print(f"[WORKER DEBUG] No jobs available (204), retrying...", flush=True)
                    sem.release()
                    continue  # Immediately retry long poll
                
                if resp.status_code == 400:
                    print(f"[WORKER DEBUG] Lease error (400): {resp.text}", flush=True)
                    sem.release()
                    await asyncio.sleep(5)
                    continue
                
                if resp.status_code != 200:
                    print(f"[WORKER DEBUG] Unexpected status {resp.status_code}: {resp.text}", flush=True)
                    sem.release()
                    await asyncio.sleep(5)
                    continue
                
                job_data = resp.json()
                job_id = job_data["id"]
                print(f"[WORKER DEBUG] Got job {job_id}, starting processing...", flush=True)
                
                # Process and report
                task = asyncio.create_task(run_remote_job_and_report(job_id, job_data, sem, job_type, client, master_url, my_worker_id))
                active_job_tasks.add(task)
                task.add_done_callback(active_job_tasks.discard)

            except httpx.TimeoutException:
                # Server didn't respond in time - just retry
                print(f"[WORKER DEBUG] HTTP timeout for {job_type}, retrying...", flush=True)
                sem.release()
                continue
            except Exception as e:
                print(f"[WORKER DEBUG] Remote worker error: {e}", flush=True)
                import traceback
                traceback.print_exc(file=sys.stderr)
                sem.release()
                await asyncio.sleep(10)

async def worker_local_loop(job_type, sem, config):
    """Local filesystem scanning loop for standalone and master modes."""
    limits = get_limits_for_type(job_type, config)
    mode = config.get("mode", "standalone")
    print(f"Job queue worker for type '{job_type}' started (mode: {mode}, max_per_instance: {limits['max_per_instance']}, max_global: {limits['max_global']})")
    queued_job_type_dir = f"{QUEUED_DIR}/{job_type}"
    active_job_type_dir = f"{ACTIVE_DIR}/{job_type}"
    is_master = mode == "master"
    
    while worker_running.is_set():
        try:
            # Check if queue is paused
            if is_queue_paused():
                await asyncio.sleep(5)
                continue
            
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
                
                # In master mode, also check global limit before processing
                if is_master:
                    active_count = count_active_jobs_for_type(job_type)
                    if active_count >= limits["max_global"]:
                        print(f"Global limit reached for {job_type} ({active_count}/{limits['max_global']}), waiting...")
                        sem.release()
                        await asyncio.sleep(5)
                        break  # Re-check queue from start
                

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
    
    print(f"Job queue worker loop for type '{job_type}' finished.")

async def run_remote_job_and_report(job_id, job_data, sem, job_type, client, master_url, worker_id):
    """Run job locally and report result back to master."""
    try:
        print(f"Executing remote job {job_id}...")
        job_data = await execute_job_core(job_id, job_data)
        
        # Add worker_id to the report
        job_data["worker_id"] = worker_id
        
        print(f"Reporting remote job {job_id} back to master...")
        resp = await client.post(f"{master_url}/api/jobs/report/{job_id}", json=job_data)
        if not resp.is_success:
            print(f"Failed to report job {job_id}: {resp.text}")
    except Exception as e:
        print(f"Error in run_remote_job_and_report: {e}")
    finally:
        sem.release()

# ---------------------------------------------------------------------------
# Stale job cleanup
# ---------------------------------------------------------------------------
async def cleanup_stale_jobs():
    """Periodically check for stale jobs and move them back to queued. Uses worker registry to detect dead workers."""
    config = load_config()
    timeout_minutes = config.get("stale_job_timeout_minutes", 60)
    
    while worker_running.is_set():
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            config = load_config()  # Reload in case it changed
            timeout_minutes = config.get("stale_job_timeout_minutes", 60)
            
            if timeout_minutes <= 0:
                continue  # Disabled
            
            now = datetime.now()
            registry = load_workers_registry()
            # Consider a worker dead if not seen in 10 minutes
            worker_timeout_seconds = 600
            
            # Scan active directory for stale jobs
            if not os.path.exists(ACTIVE_DIR):
                continue
                
            for job_type_dir in os.listdir(ACTIVE_DIR):
                type_path = os.path.join(ACTIVE_DIR, job_type_dir)
                if not os.path.isdir(type_path):
                    continue
                    
                for job_file in os.listdir(type_path):
                    if not job_file.endswith('.json'):
                        continue
                    
                    job_path = os.path.join(type_path, job_file)
                    try:
                        async with aiofiles.open(job_path, 'r') as f:
                            job_data = json.loads(await f.read())
                        
                        # Check if assigned worker is still alive
                        assigned_worker = job_data.get("assigned_worker")
                        worker_is_dead = False
                        
                        if assigned_worker and assigned_worker in registry:
                            worker_info = registry[assigned_worker]
                            last_seen = worker_info.get("last_seen")
                            if last_seen:
                                last_seen_dt = datetime.fromisoformat(last_seen)
                                if (now - last_seen_dt).total_seconds() > worker_timeout_seconds:
                                    worker_is_dead = True
                                    print(f"Worker {assigned_worker} appears dead (last seen: {last_seen})")
                        elif assigned_worker:
                            # Worker not in registry at all - probably dead
                            worker_is_dead = True
                            print(f"Worker {assigned_worker} not in registry")
                        
                        # Check time-based staleness as fallback
                        time_is_stale = False
                        started_at = job_data.get("started_at") or job_data.get("assigned_at")
                        if started_at:
                            started = datetime.fromisoformat(started_at)
                            if (now - started).total_seconds() > timeout_minutes * 60:
                                time_is_stale = True
                        
                        # Requeue if worker is dead OR job is time-stale
                        if worker_is_dead or time_is_stale:
                            reason = "worker dead" if worker_is_dead else "time stale"
                            print(f"Stale job detected: {job_file} (reason: {reason}, started: {started_at})")
                            # Clear worker assignment before requeuing
                            job_data.pop("assigned_worker", None)
                            job_data.pop("assigned_worker_ip", None)
                            job_data.pop("assigned_at", None)
                            job_data["status"] = "queued"
                            job_data["updated_at"] = datetime.now().isoformat()
                            queued_path = os.path.join(QUEUED_DIR, job_type_dir, job_file)
                            os.makedirs(os.path.dirname(queued_path), exist_ok=True)
                            # Write updated job data
                            async with aiofiles.open(queued_path, 'w') as f:
                                await f.write(json.dumps(job_data, indent=2))
                            os.remove(job_path)
                            print(f"Moved stale job {job_file} back to queued")
                    except Exception as e:
                        print(f"Error checking stale job {job_file}: {e}")
        except Exception as e:
            print(f"Error in stale job cleanup: {e}")

# ---------------------------------------------------------------------------
# Worker lifecycle management
# ---------------------------------------------------------------------------
@service()
async def ensure_job_type_worker_running(job_type):
    """Ensure a worker for a specific job type is running."""
    if not worker_running.is_set():
        print(f"[DEBUG] ensure_job_type_worker_running: worker_running not set, skipping {job_type}", flush=True)
        return
        
    sjt = sanitize_job_type(job_type)
    if sjt in job_type_tasks and not job_type_tasks[sjt].done():
        print(f"[DEBUG] ensure_job_type_worker_running: worker already running for {sjt}", flush=True)
        return
    
    print(f"[DEBUG] Starting on-demand worker for job type: {sjt}", flush=True)
    job_type_tasks[sjt] = asyncio.create_task(job_type_worker_loop(sjt))

async def start_job_type_workers():
    """Start workers for job types with pending jobs at startup."""
    print(f"[DEBUG] start_job_type_workers called", flush=True)
    if not await aiofiles.os.path.exists(QUEUED_DIR):
        print(f"[DEBUG] QUEUED_DIR {QUEUED_DIR} does not exist", flush=True)
        return

    dirs = await aiofiles.os.listdir(QUEUED_DIR)
    print(f"[DEBUG] Found job type dirs in queue: {dirs}", flush=True)
    
    for job_type_dir in await aiofiles.os.listdir(QUEUED_DIR):
        job_type_path = os.path.join(QUEUED_DIR, job_type_dir)
        if await aiofiles.os.path.isdir(job_type_path):
            try:
                files = await aiofiles.os.listdir(job_type_path)
                json_files = [f for f in files if f.endswith('.json')]
                print(f"[DEBUG] Job type {job_type_dir}: {len(json_files)} queued jobs", flush=True)
                if any(f.endswith('.json') for f in files):
                    await ensure_job_type_worker_running(job_type_dir)
            except Exception as e:
                print(f"[DEBUG] Error checking job type dir {job_type_dir}: {e}", flush=True)

@hook()
async def startup(app, context=None):
    """Start the worker system when the plugin loads."""
    global stale_cleanup_task
    config = load_config()
    print(f"[DEBUG] Plugin startup: mode={config.get('mode')}", flush=True)
    print(f"[DEBUG] Plugin startup: Initializing job queue worker system.", flush=True)
    worker_running.set()
    asyncio.create_task(start_job_type_workers())
    # Start stale job cleanup task
    stale_cleanup_task = asyncio.create_task(cleanup_stale_jobs())
    return {"status": "Worker system initialized"}

@hook()
async def quit(context=None):
    """Stop the worker and cleanup active tasks when the plugin stops."""
    print("Plugin quit: Stopping job queue worker.")
    worker_running.clear()
    
    # Cancel stale cleanup task
    if stale_cleanup_task:
        stale_cleanup_task.cancel()
        try:
            await stale_cleanup_task
        except asyncio.CancelledError:
            pass
    
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
