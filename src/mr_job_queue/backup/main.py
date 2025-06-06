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

from lib.providers.services import service, service_manager
from lib.providers.hooks import hook
from lib.utils.debug import debug_box

# Local imports
from .filelock import FileLock
from .helpers import get_job_data, update_job_index

debug_box("----------------------------------- JOB_QUEUE STARTING ----------------------------------------------------")

# Job directory structure
JOB_DIR = "data/jobs"  # Base directory for all jobs
QUEUED_DIR = f"{JOB_DIR}/queued"  # Will contain job_type subdirectories
ACTIVE_DIR = f"{JOB_DIR}/active"  # Will contain job_type subdirectories
COMPLETED_DIR = f"{JOB_DIR}/completed"  # Will contain job_type subdirectories
DEFAULT_JOB_TYPE = "default"  # Default job type for backward compatibility
FAILED_DIR = f"{JOB_DIR}/failed"
JOB_INDEX = f"{JOB_DIR}/job_index.jsonl"

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

# Worker process state
worker_task = None
worker_running = asyncio.Event() # Use Event for clearer start/stop signaling
semaphore = None
active_job_tasks = set()
# Dictionary to store semaphores for each job type
job_type_semaphores = {}
job_type_tasks = {}

# Services (for plugin-to-plugin integration)
@service()
async def add_job(instructions, agent_name, job_type=None, username=None, metadata=None, job_id=None, llm=None, context=None):
    """Submit a job to be processed by an agent (service for plugin-to-plugin integration).
    
    Args:
        instructions: Text instructions for the agent
        agent_name: Name of the agent to process this job
        job_type: Optional category/type of job
        metadata: Optional additional data for the job
        job_id: Optional, otherwise will be random ID
        context: The execution context (contains username etc.)
    
    Returns:
        job_id: ID of the submitted job
    """

    if job_id is None:
        job_id = f"job_{uuid.uuid4()}"
  
    if username is None:
        username = getattr(context, 'username', 'system')

    # Ensure job_type is not None for directory structure
    job_type = job_type or DEFAULT_JOB_TYPE

    debug_box(f"add_job called by {username}")
    # debug_box(instructions) # Potentially very long
    
    # Create job data
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
        "plugin": job_type.split(".")[0] if job_type and "." in job_type else None,
        "job_type": job_type,
        "result": None,
        "llm": llm,
        "error": None,
        "log_id": job_id,
        "metadata": metadata or {}
    }
    
    # Create job_type directories if they don't exist
    queued_job_type_dir = f"{QUEUED_DIR}/{job_type}"
    active_job_type_dir = f"{ACTIVE_DIR}/{job_type}"
    completed_job_type_dir = f"{COMPLETED_DIR}/{job_type}"
    
    # Ensure job type directories exist
    os.makedirs(queued_job_type_dir, exist_ok=True)
    os.makedirs(active_job_type_dir, exist_ok=True)
    os.makedirs(completed_job_type_dir, exist_ok=True)
    
    # Save job file in the appropriate job_type directory
    job_path = f"{queued_job_type_dir}/{job_id}.json"
    try:
        # Use a try-finally block to ensure the lock is released even if an exception occurs
        lock = FileLock(job_path)
        try:
            async with lock:
                async with aiofiles.open(job_path, "w") as f:
                    await f.write(json.dumps(job_data, indent=2))
        except Exception as e:
            print(f"Error writing job file {job_path}: {e}")
            return {"error": f"Failed to write job file: {e}"}
    except Exception as e:
        print(f"Error acquiring lock for job file {job_path}: {e}")
        return {"error": f"Failed to acquire lock for job file: {e}"}

    # Append to index
    # Ensure index file exists before attempting to lock/append
    if not await aiofiles.os.path.exists(JOB_INDEX):
        try:
            # Create empty index file
            async with aiofiles.open(JOB_INDEX, "w") as f:
                pass  # Create empty file
        except Exception as e:
            print(f"Error creating job index file {JOB_INDEX}: {e}")
            # Proceed without index update if creation fails
            pass

    # Append to index file
    try:  # Append to index
        async with FileLock(JOB_INDEX):

            # first make sure if there is text in the file that it already ends in a newline
            add_newline = False
            async with aiofiles.open(JOB_INDEX, "rb") as f:
                last_char = await f.read(1)
                if last_char != b'\n':
                    add_newline = True

            async with aiofiles.open(JOB_INDEX, "a") as f:
                index_entry = {
                    "id": job_id,
                    "status": "queued",
                    "agent_name": agent_name,
                    "job_type": job_type,
                    "created_at": job_data["created_at"],
                    "username": job_data["username"]
                }
                if add_newline:
                    await f.write("\n")
                await f.write(json.dumps(index_entry) + "\n")
    except Exception as e:
        print(f"Error appending to job index {JOB_INDEX}: {e}")
        # Job file created, but index update failed. Log and continue.

    # Ensure worker is running (non-blocking)
    asyncio.create_task(ensure_worker_running())
    
    # Ensure job type worker is running (non-blocking)
    asyncio.create_task(ensure_job_type_worker_running(job_type))
    
    return {"job_id": job_id}

@service()
async def process_job(job_id, job_data, job_type=None):
    """Process a single job using the run_task service"""
    job_type = job_type or job_data.get("job_type", DEFAULT_JOB_TYPE)
    active_path = f"{ACTIVE_DIR}/{job_type}/{job_id}.json"
    try:
        print(f"Processing job {job_id}")
        
        # Update job status to active (already moved to ACTIVE_DIR)
        job_data["status"] = "active"
        job_data["started_at"] = datetime.now().isoformat()
        job_data["updated_at"] = datetime.now().isoformat()
        
        # Write updated job data to active file
        async with FileLock(active_path):
            async with aiofiles.open(active_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        # Update index with job_type (redundant if move was successful, but safe)
        await update_job_index(job_id, "active", job_type)
        
        print(f"Running task for job {job_id} with agent {job_data['agent_name']}")
        # Ensure run_task service exists
        if not hasattr(service_manager, 'run_task'):
             raise RuntimeError("run_task service is not available via service_manager")

        text, full_results, log_id = await service_manager.run_task(
            instructions=job_data["instructions"],
            agent_name=job_data["agent_name"],
            user=job_data["username"],
            retries=3,
            llm=job_data["llm"],
            log_id=job_id, # Use job_id as log_id for correlation
            context=None # Pass context if needed by run_task
        )
        
        print(f"Task completed for job {job_id}, log_id: {log_id}")
        
        # Update job with results
        job_data["status"] = "completed"
        job_data["result"] = text # Store main text result
        # Consider storing full_results if needed, maybe in metadata?
        # job_data["metadata"]["full_results"] = full_results
        job_data["log_id"] = log_id
        job_data["completed_at"] = datetime.now().isoformat()
        job_data["updated_at"] = datetime.now().isoformat()
        
        # Write to completed directory
        completed_path = f"{COMPLETED_DIR}/{job_type}/{job_id}.json"
        async with FileLock(completed_path):
            async with aiofiles.open(completed_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        # Remove from active *after* writing to completed
        try:
            await aiofiles.os.remove(active_path)
        except FileNotFoundError:
            print(f"Warning: Active job file {active_path} already removed.")
        except Exception as e:  # Handle errors gracefully
            print(f"Warning: Failed to remove active job file {active_path}: {e}")
        
        # Update index with job_type
        await update_job_index(job_id, "completed", job_type)
        
        return True
        
    except Exception as e:
        print(f"Error processing job {job_id}: {e}")
        print(traceback.format_exc())
        
        # Handle failure
        try:
            error_message = str(e)
            traceback_str = traceback.format_exc()
            
            # Update job data with error
            job_data["status"] = "failed"
            job_data["error"] = f"{error_message}\
{traceback_str}"
            job_data["completed_at"] = datetime.now().isoformat() # Mark failure time
            job_data["updated_at"] = datetime.now().isoformat()
            
            # Write to failed directory
            failed_path = f"{FAILED_DIR}/{job_id}.json"  # Keep failed jobs in a single directory
            async with FileLock(failed_path):
                async with aiofiles.open(failed_path, "w") as f:
                    await f.write(json.dumps(job_data, indent=2))
            
            # Remove from active if it still exists
            if await aiofiles.os.path.exists(active_path):
                try:
                    await aiofiles.os.remove(active_path)
                except Exception as remove_err:
                    print(f"Warning: Failed to remove active job file {active_path} after failure: {remove_err}")
            
            # Update index
            await update_job_index(job_id, "failed", job_type)
        except Exception as inner_e:
            # Log error during failure handling
            print(f"Critical Error: Failed during job failure handling for {job_id}: {inner_e}")
            print(traceback.format_exc())
        
        return False

async def run_job_and_release(job_id, job_data, sem, job_type=None):
    """Wrapper to run process_job and release semaphore."""
    global active_job_tasks
    try:
        print(f"Starting processing for job {job_id}")
        await process_job(job_id, job_data)
        print(f"Finished processing job {job_id}")
    except Exception as e:
        print(f"Exception caught in run_job_and_release for {job_id} of type {job_type}: {e}")
        # process_job should have already handled moving to failed state
    finally:
        print(f"Releasing semaphore for job {job_id}")
        sem.release()

# --- End of first half ---

async def job_type_worker_loop(job_type):
    """Worker loop for a specific job type."""
    global worker_running
    global job_type_semaphores
    
    # Get or create semaphore for this job type
    if job_type not in job_type_semaphores:
        print(f"Initializing semaphore for job type {job_type} with limit {MAX_CONCURRENT_PER_TYPE}")
        job_type_semaphores[job_type] = asyncio.Semaphore(MAX_CONCURRENT_PER_TYPE)
    
    sem = job_type_semaphores[job_type]
    
    print(f"Job queue worker for type {job_type} started (Max Concurrent: {MAX_CONCURRENT_PER_TYPE})")
    
    # Create job type directories if they don't exist
    queued_job_type_dir = f"{QUEUED_DIR}/{job_type}"
    active_job_type_dir = f"{ACTIVE_DIR}/{job_type}"
    completed_job_type_dir = f"{COMPLETED_DIR}/{job_type}"
    
    # Ensure job type directories exist
    os.makedirs(queued_job_type_dir, exist_ok=True)
    os.makedirs(active_job_type_dir, exist_ok=True)
    os.makedirs(completed_job_type_dir, exist_ok=True)
    
    while worker_running.is_set():
        queued_jobs_files = []
        job_files_with_mtime = []
        try:
            # Get list of queued jobs for this job type
            if not await aiofiles.os.path.exists(queued_job_type_dir):
                await aiofiles.os.makedirs(queued_job_type_dir, exist_ok=True)
                
            raw_files = await aiofiles.os.listdir(queued_job_type_dir)
            for job_file in raw_files:
                if job_file.endswith('.json'):
                    full_path = os.path.join(queued_job_type_dir, job_file)
                    try:
                        stat_result = await aiofiles.os.stat(full_path)
                        job_files_with_mtime.append((stat_result.st_mtime, job_file))
                    except FileNotFoundError:
                        # File might have been processed/moved since listdir
                        print(f"Warning: File {full_path} not found during mtime check.")
                        continue
                    except Exception as e_stat:
                        print(f"Warning: Could not stat file {full_path}: {e_stat}")
                        continue
            
            # Sort jobs by modification time (oldest first)
            job_files_with_mtime.sort(key=lambda x: x[0])
            queued_jobs_files = [job_file for _, job_file in job_files_with_mtime]

            if not queued_jobs_files:
                # No jobs to process, wait before checking again
                await asyncio.sleep(5) 
                continue
            
            # Try to acquire semaphore and launch jobs
            for job_file in queued_jobs_files:
                if not worker_running.is_set():
                    print(f"Worker loop for job type {job_type} stopping signal received.")
                    break # Exit inner loop if stop signal received
                    
                job_path = f"{queued_job_type_dir}/{job_file}"
                job_id = job_file.replace(".json", "")
                
                # Try to acquire semaphore without blocking the entire loop indefinitely
                print(f"Attempting to acquire semaphore for job {job_id} of type {job_type}... ({sem._value} available)")
                try:
                    # Use a timeout to prevent indefinite blocking
                    acquired = False
                    for _ in range(3):  # Try a few times with short timeouts
                        try:
                            # Use wait_for with a timeout to prevent indefinite blocking
                            await asyncio.wait_for(sem.acquire(), timeout=2.0)
                            acquired = True
                            break
                        except asyncio.TimeoutError:
                            print(f"Timeout acquiring semaphore for {job_id}, retrying...")
                            # Continue the retry loop
                            await asyncio.sleep(0.5)  # Short sleep before retry
                            
                    if not acquired:
                        print(f"Could not acquire semaphore for {job_id} after retries, will try again later")
                        continue  # Skip to next job
                        
                    print(f"Semaphore acquired for job {job_id} of type {job_type}")
                except Exception as acq_e:
                    print(f"Error acquiring semaphore for {job_id}: {acq_e}")
                    continue  # Skip to next job

                # Double-check worker status after acquiring semaphore
                if not worker_running.is_set():
                    print(f"Worker stopped after acquiring semaphore for {job_id}, releasing.")
                    sem.release()
                    break # Exit inner loop

                # Process the job (similar to the main worker loop but with job_type paths)
                try:
                    # Check if file still exists (might have been processed/cancelled)
                    if not await aiofiles.os.path.exists(job_path):
                        print(f"Job file {job_path} disappeared before moving, releasing semaphore.")
                        sem.release()
                        continue # Try next file

                    # Move job file to active directory *before* launching task
                    active_path = f"{active_job_type_dir}/{job_file}"
                    try:
                        await aiofiles.os.rename(job_path, active_path)
                        print(f"Moved {job_id} to active directory for type {job_type}")
                    except Exception as move_error:
                        print(f"Error moving job file {job_path} to {active_path}: {move_error}, releasing semaphore.")
                        sem.release()
                        continue # Try next file
                    
                    # Read job data *after* moving
                    job_data = await get_job_data(job_id) # Should read from ACTIVE_DIR now
                    if not job_data:
                        print(f"Could not read job data for {job_id} from active dir, moving to failed.")
                        # Attempt to move to failed state manually
                        failed_path = f"{FAILED_DIR}/{job_id}.json"
                        error_data = {"id": job_id, "status": "failed", "error": "Failed to read data after moving to active."}
                        try:
                            async with FileLock(failed_path):
                                async with aiofiles.open(failed_path, "w") as f:
                                    await f.write(json.dumps(error_data, indent=2))
                            await update_job_index(job_id, "failed", job_type)
                            print(f"Manually moved {job_id} of type {job_type} to failed state due to read error.")
                        except Exception as fail_err:  # Handle errors gracefully
                            print(f"Error moving {job_id} to failed state after read error: {fail_err}")
                        # Release semaphore even if manual move fails
                        sem.release()
                        continue # Try next file
                    
                    # Launch the job processing in the background
                    print(f"Creating task for job {job_id} of type {job_type}")
                    task = asyncio.create_task(run_job_and_release(job_id, job_data, sem, job_type))
                    active_job_tasks.add(task)
                    # Remove task from set when done to prevent memory leak
                    task.add_done_callback(active_job_tasks.discard)
                    print(f"Task for {job_id} of type {job_type} created and added to active set ({len(active_job_tasks)} total).")

                except Exception as e:
                    # Catch errors during the acquire/move/launch phase for a single job
                    print(f"Error in job type worker loop processing job {job_file}: {e}")
                    print(traceback.format_exc())
                    # Ensure semaphore is released if an error occurred after acquiring it
                    try:
                        sem.release()
                        print(f"Semaphore released due to error during processing setup for {job_id}")
                    except (ValueError, RuntimeError):
                        # Already released or semaphore state issue, log it
                        print(f"Note: Semaphore already released or state error during error handling for {job_id}")
                        pass 
                    # Continue to the next job file
                    continue
            await asyncio.sleep(1) # Shorter sleep when jobs were processed

        # Handle errors at the worker loop level
        except FileNotFoundError:
            print(f"Queued directory {queued_job_type_dir} not found. Worker sleeping.")
            await asyncio.sleep(15)
        except Exception as e:
            # Catch errors in the main worker loop (e.g., listing directory)
            print(f"Worker loop for job type {job_type} encountered an error: {e}")
            print(traceback.format_exc())
            await asyncio.sleep(10)  # Longer sleep on major loop error
    
    print(f"Job queue worker loop for type {job_type} finished.")

async def ensure_job_type_worker_running(job_type):
    """Ensure a worker for a specific job type is running."""
    global job_type_tasks, worker_running
    
    # If worker is not running, don't start type-specific workers
    if not worker_running.is_set():
        print(f"Main worker not running, not starting job type worker for {job_type}")
        return
    
    # Check if a task for this job type already exists and is running
    if job_type in job_type_tasks and not job_type_tasks[job_type].done():
        # Task exists and is not finished, assume it's running
        return
    
    print(f"Starting worker for job type {job_type}")
    # Create a new task for this job type
    job_type_tasks[job_type] = asyncio.create_task(job_type_worker_loop(job_type))

async def start_job_type_workers():
    """Start workers for all existing job type directories."""
    # Check for existing job type directories in QUEUED_DIR
    try:
        if await aiofiles.os.path.exists(QUEUED_DIR):
            dirs = await aiofiles.os.listdir(QUEUED_DIR)
            for job_type_dir in dirs:
                job_type_path = os.path.join(QUEUED_DIR, job_type_dir)
                if await aiofiles.os.path.isdir(job_type_path):
                    # This is a job type directory, start a worker for it
                    await ensure_job_type_worker_running(job_type_dir)
    except Exception as e:
        print(f"Error starting job type workers: {e}")
        print(traceback.format_exc())

    # Also check for the default job type
    await ensure_job_type_worker_running(DEFAULT_JOB_TYPE)

async def worker_loop():
    """Main worker loop that processes queued jobs concurrently up to a limit."""
    global worker_running
    global semaphore
    global active_job_tasks
    global job_type_tasks
    
    if not semaphore:
        print("Error: Worker loop started before semaphore was initialized!")
        return
    
    print(f"Job queue worker started (Max Concurrent: {MAX_CONCURRENT_JOBS})")
    
    # Start job type workers for existing directories
    await start_job_type_workers()
    
    while worker_running.is_set():
        queued_jobs_files = []
        job_files_with_mtime = []
        try:
            # Get list of queued jobs
            raw_files = await aiofiles.os.listdir(QUEUED_DIR)
            
            # Check for job type directories and ensure workers are running for them
            for item in raw_files:
                item_path = os.path.join(QUEUED_DIR, item)
                try:
                    if await aiofiles.os.path.isdir(item_path):
                        # This is a job type directory, ensure worker is running
                        await ensure_job_type_worker_running(item)
                except Exception as dir_e:
                    print(f"Error checking directory {item_path}: {dir_e}")
                    continue
            
            # Process legacy jobs (directly in QUEUED_DIR, not in a job_type subdir)
            for job_file in raw_files:
                if job_file.endswith('.json'):
                    full_path = os.path.join(QUEUED_DIR, job_file)
                    try:
                        stat_result = await aiofiles.os.stat(full_path)
                        job_files_with_mtime.append((stat_result.st_mtime, job_file))
                    except FileNotFoundError:
                        # File might have been processed/moved since listdir
                        print(f"Warning: File {full_path} not found during mtime check.")
                        continue
                    except Exception as e_stat:
                        print(f"Warning: Could not stat file {full_path}: {e_stat}")
                        continue
            
            # Sort jobs by modification time (oldest first)
            job_files_with_mtime.sort(key=lambda x: x[0])
            queued_jobs_files = [job_file for _, job_file in job_files_with_mtime]

            if not queued_jobs_files:
                # No jobs to process, wait before checking again
                await asyncio.sleep(5) 
                continue
            
            # Try to acquire semaphore and launch jobs
            for job_file in queued_jobs_files:
                if not worker_running.is_set():
                    print("Worker loop stopping signal received.")
                    break # Exit inner loop if stop signal received
                    
                job_path = f"{QUEUED_DIR}/{job_file}"
                job_id = job_file.replace(".json", "")
                
                # Try to acquire semaphore without blocking the entire loop indefinitely
                print(f"Attempting to acquire semaphore for job {job_id}... ({semaphore._value} available)")
                await semaphore.acquire()
                print(f"Semaphore acquired for job {job_id}")

                # Double-check worker status after acquiring semaphore
                if not worker_running.is_set():
                    print("Worker stopped after acquiring semaphore, releasing.")
                    semaphore.release()
                    break # Exit inner loop

                try:
                    # Check if file still exists in QUEUED (might have been processed/cancelled)
                    if not await aiofiles.os.path.exists(job_path):
                        print(f"Job file {job_path} disappeared before moving, releasing semaphore.")
                        semaphore.release()
                        continue # Try next file

                    # Move job file to active directory *before* launching task
                    active_path = f"{ACTIVE_DIR}/{job_file}"
                    try:
                        await aiofiles.os.rename(job_path, active_path)
                        print(f"Moved {job_id} to active directory")
                    except Exception as move_error:
                        print(f"Error moving job file {job_path} to {active_path}: {move_error}, releasing semaphore.")
                        semaphore.release()
                        continue # Try next file
                    
                    # Read job data *after* moving
                    job_data = await get_job_data(job_id) # Should read from ACTIVE_DIR now
                    if not job_data:
                        print(f"Could not read job data for {job_id} from active dir, moving to failed.")
                        # Attempt to move to failed state manually
                        failed_path = f"{FAILED_DIR}/{job_id}.json"
                        error_data = {"id": job_id, "status": "failed", "error": "Failed to read data after moving to active."}
                        try:
                            async with FileLock(failed_path):
                                async with aiofiles.open(failed_path, "w") as f:
                                    await f.write(json.dumps(error_data, indent=2))
                            await update_job_index(job_id, "failed", DEFAULT_JOB_TYPE)  # Use default job type for legacy jobs
                            print(f"Manually moved {job_id} of type {job_type} to failed state due to read error.")
                        except Exception as fail_err:
                            print(f"Error moving {job_id} to failed state after read error: {fail_err}")
                        # Release semaphore even if manual move fails
                        semaphore.release()
                        continue # Try next file
                    
                    # Launch the job processing in the background
                    print(f"Creating task for job {job_id}")
                    task = asyncio.create_task(run_job_and_release(job_id, job_data, semaphore))
                    active_job_tasks.add(task)
                    # Remove task from set when done to prevent memory leak
                    task.add_done_callback(active_job_tasks.discard)
                    print(f"Task for {job_id} created and added to active set ({len(active_job_tasks)} total).")

                except Exception as e:
                    # Catch errors during the acquire/move/launch phase for a single job
                    print(f"Error in worker loop processing job {job_file}: {e}")
                    print(traceback.format_exc())
                    # Ensure semaphore is released if an error occurred after acquiring it
                    # Check if the semaphore is locked by the current task context (tricky)
                    # A simple release might work if the error happened after acquire
                    try:
                        # This might raise ValueError if already released, hence the try/except
                        semaphore.release()
                        print(f"Semaphore released due to error during processing setup for {job_id}")
                    except (ValueError, RuntimeError):
                        # Already released or semaphore state issue, log it
                        print(f"Note: Semaphore already released or state error during error handling for {job_id}")
                        pass 
                    # Continue to the next job file
                    continue

            # If loop finished, wait a bit before scanning again
            await asyncio.sleep(1) # Shorter sleep when jobs were processed

        except FileNotFoundError:
            print(f"Queued directory {QUEUED_DIR} not found. Worker sleeping.")
            await asyncio.sleep(15)
        except Exception as e:
            # Catch errors in the main worker loop (e.g., listing directory)
            print(f"Worker loop encountered an error: {e}")
            print(traceback.format_exc())
            await asyncio.sleep(10)  # Longer sleep on major loop error
    
    print("Job queue worker loop finished.")

@service()
async def start_worker():
    """Start the job queue worker process if not already running."""
    global worker_task, worker_running, semaphore, MAX_CONCURRENT_JOBS
    
    if worker_task and not worker_task.done():
        print("Worker already running.")
        return {"status": "Worker already running"}
    
    # Initialize semaphore if not already done (should be done at startup ideally)
    if not semaphore:
        print(f"Initializing semaphore with limit {MAX_CONCURRENT_JOBS}")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
        
    # Set the running event before creating the task
    worker_running.set()
    
    # Start the worker in a background task
    print("Creating worker loop task.")
    worker_task = asyncio.create_task(worker_loop())
    
    return {"status": "Worker started"}

async def ensure_worker_running():
    """Ensure the worker is running, start it if not."""
    global worker_task
    
    if worker_task and not worker_task.done():
        # Task exists and is not finished, assume it's running or will run
        return
    
    print("Worker task not found or finished, ensuring worker starts.")
    await start_worker()

@hook()
async def startup(app, context=None):
    """Start the worker when the plugin starts."""
    global worker_running, semaphore, MAX_CONCURRENT_JOBS
    print("Plugin startup: Initializing job queue worker.")
    # Initialize semaphore here definitively
    if not semaphore:
         print(f"Initializing semaphore at startup with limit {MAX_CONCURRENT_JOBS}")
         semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    # Signal worker loop that it's okay to run
    worker_running.set() 
    # Create the task to ensure it starts
    asyncio.create_task(ensure_worker_running())
    
    # Also start job type workers for existing directories
    asyncio.create_task(start_job_type_workers())
    return {"status": "Worker initialized"}

@hook()
async def quit(context=None):
    """Stop the worker and cleanup active tasks when the plugin stops."""
    global job_type_tasks
    global worker_running, worker_task, active_job_tasks
    
    print("Plugin quit: Stopping job queue worker.")
    # Signal worker loop to stop checking for new jobs and exit
    worker_running.clear() 
    
    # Stop the main worker loop task
    if worker_task and not worker_task.done():
        try:
            # Give the worker loop a chance to finish its current iteration
            print("Waiting for worker loop task to finish...")
            await asyncio.wait_for(worker_task, timeout=5)
            print("Worker loop task finished gracefully.")
        except asyncio.TimeoutError:
            # If it doesn't finish in time, cancel it
            print("Worker loop task timed out, cancelling...")
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                print("Worker loop task cancelled.")
            except Exception as e:
                 print(f"Error awaiting cancelled worker task: {e}")
        except Exception as e:
            print(f"Error waiting for worker loop task: {e}")

    # Stop job type worker tasks
    if job_type_tasks:
        print(f"Stopping {len(job_type_tasks)} job type worker tasks...")
        for job_type, task in list(job_type_tasks.items()):
            if not task.done():
                try:
                    # Give the worker loop a chance to finish its current iteration
                    print(f"Waiting for job type worker {job_type} to finish...")
                    await asyncio.wait_for(task, timeout=5)
                    print(f"Job type worker {job_type} finished gracefully.")
                except asyncio.TimeoutError:
                    # If it doesn't finish in time, cancel it
                    print(f"Job type worker {job_type} timed out, cancelling...")
                    task.cancel()
                except Exception as e:
                    print(f"Error waiting for job type worker {job_type}: {e}")

    # Wait for any active job tasks (spawned by worker loop) to complete
    if active_job_tasks:
        print(f"Waiting for {len(active_job_tasks)} active job tasks to complete...")
        try:
            # Wait for all tasks with a timeout
            # Use gather to wait for them
            await asyncio.wait_for(asyncio.gather(*active_job_tasks, return_exceptions=True), timeout=30)
            print("All active job tasks finished within timeout.")
        except asyncio.TimeoutError:
             print(f"Warning: {len(active_job_tasks)} job tasks did not complete within 30s timeout. They may be left running or cancelled abruptly.")
             # Optionally, iterate and cancel remaining tasks
             # for task in active_job_tasks:
             #     if not task.done(): task.cancel()
             # await asyncio.gather(*active_job_tasks, return_exceptions=True) # Wait for cancellations
        except Exception as e:
            print(f"Error waiting for active job tasks during shutdown: {e}")
    else:
        print("No active job tasks to wait for.")

    print("Job queue worker shutdown complete.")
    return {"status": "Worker stopped and active jobs handled."}
