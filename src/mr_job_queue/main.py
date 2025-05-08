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

# Job directory structure
JOB_DIR = "data/jobs"
QUEUED_DIR = f"{JOB_DIR}/queued"
ACTIVE_DIR = f"{JOB_DIR}/active"
COMPLETED_DIR = f"{JOB_DIR}/completed"
FAILED_DIR = f"{JOB_DIR}/failed"
JOB_INDEX = f"{JOB_DIR}/job_index.jsonl"

# Concurrency settings - Read from environment variable
try:
    MAX_CONCURRENT_JOBS = int(os.getenv('JOB_QUEUE_MAX_CONCURRENT', '1'))
except ValueError:
    print("Warning: Invalid value for JOB_QUEUE_MAX_CONCURRENT env var, defaulting to 1.")
    MAX_CONCURRENT_JOBS = 5

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

# Services (for plugin-to-plugin integration)
@service()
async def add_job(instructions, agent_name, job_type=None, username=None, metadata=None, job_id=None, context=None):
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
        "error": None,
        "log_id": job_id,
        "metadata": metadata or {}
    }
    
    # Save job file
    job_path = f"{QUEUED_DIR}/{job_id}.json"
    try:
        async with FileLock(job_path):
            async with aiofiles.open(job_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
    except Exception as e:
        print(f"Error writing job file {job_path}: {e}")
        return {"error": f"Failed to write job file: {e}"}

    # Append to index
    # Ensure index file exists before attempting to lock/append
    if not await aiofiles.os.path.exists(JOB_INDEX):
        try:
            async with aiofiles.open(JOB_INDEX, "w") as f:
                pass  # Create empty file
        except Exception as e:
             print(f"Error creating job index file {JOB_INDEX}: {e}")
             # Proceed without index update if creation fails?

    try:
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
    
    return {"job_id": job_id}

@service()
async def process_job(job_id, job_data):
    """Process a single job using the run_task service"""
    active_path = f"{ACTIVE_DIR}/{job_id}.json"
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
        
        # Update index (redundant if move was successful, but safe)
        await update_job_index(job_id, "active")
        
        print(f"Running task for job {job_id} with agent {job_data['agent_name']}")
        # Ensure run_task service exists
        if not hasattr(service_manager, 'run_task'):
             raise RuntimeError("run_task service is not available via service_manager")

        text, full_results, log_id = await service_manager.run_task(
            instructions=job_data["instructions"],
            agent_name=job_data["agent_name"],
            user=job_data["username"],
            retries=3,
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
        completed_path = f"{COMPLETED_DIR}/{job_id}.json"
        async with FileLock(completed_path):
            async with aiofiles.open(completed_path, "w") as f:
                await f.write(json.dumps(job_data, indent=2))
        
        # Remove from active *after* writing to completed
        try:
            await aiofiles.os.remove(active_path)
        except FileNotFoundError:
            print(f"Warning: Active job file {active_path} already removed.")
        except Exception as e:
            print(f"Warning: Failed to remove active job file {active_path}: {e}")
        
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
            
            # Update job data with error
            job_data["status"] = "failed"
            job_data["error"] = f"{error_message}\
{traceback_str}"
            job_data["completed_at"] = datetime.now().isoformat() # Mark failure time
            job_data["updated_at"] = datetime.now().isoformat()
            
            # Write to failed directory
            failed_path = f"{FAILED_DIR}/{job_id}.json"
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
            await update_job_index(job_id, "failed")
        except Exception as inner_e:
            # Log error during failure handling
            print(f"Critical Error: Failed during job failure handling for {job_id}: {inner_e}")
            print(traceback.format_exc())
        
        return False

async def run_job_and_release(job_id, job_data, sem):
    """Wrapper to run process_job and release semaphore."""
    global active_job_tasks
    try:
        print(f"Starting processing for job {job_id}")
        await process_job(job_id, job_data)
        print(f"Finished processing job {job_id}")
    except Exception as e:
        print(f"Exception caught in run_job_and_release for {job_id}: {e}")
        # process_job should have already handled moving to failed state
    finally:
        print(f"Releasing semaphore for job {job_id}")
        sem.release()

# --- End of first half ---
async def worker_loop():
    """Main worker loop that processes queued jobs concurrently up to a limit."""
    global worker_running
    global semaphore
    global active_job_tasks
    
    if not semaphore:
        print("Error: Worker loop started before semaphore was initialized!")
        return
    
    print(f"Job queue worker started (Max Concurrent: {MAX_CONCURRENT_JOBS})")
    
    while worker_running.is_set():
        queued_jobs_files = []
        job_files_with_mtime = []
        try:
            # Get list of queued jobs
            raw_files = await aiofiles.os.listdir(QUEUED_DIR)
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
                            await update_job_index(job_id, "failed")
                            print(f"Manually moved {job_id} to failed state due to read error.")
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
    return {"status": "Worker initialized"}

@hook()
async def quit(context=None):
    """Stop the worker and cleanup active tasks when the plugin stops."""
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
