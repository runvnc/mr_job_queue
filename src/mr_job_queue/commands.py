import os
import json
from datetime import datetime
import aiofiles
import aiofiles.os

from lib.providers.commands import command
from lib.providers.services import service_manager

# Assuming these are defined elsewhere, e.g., in main.py or config.py
# We need them for path construction and potentially for get_job_data/update_job_index
JOB_DIR = "data/jobs"
QUEUED_DIR = f"{JOB_DIR}/queued"
ACTIVE_DIR = f"{JOB_DIR}/active"
COMPLETED_DIR = f"{JOB_DIR}/completed"
DEFAULT_JOB_TYPE = "default"  # Default job type for backward compatibility
FAILED_DIR = f"{JOB_DIR}/failed"
JOB_INDEX = f"{JOB_DIR}/job_index.jsonl"

# Import necessary helpers (assuming they are moved to a helpers.py or main.py)
# If they remain in mod.py (renamed to main.py), adjust the import
from .helpers import get_job_data, update_job_index, sanitize_job_type

# Commands (for LLM agents)
@command()
async def submit_job(instructions, agent_name, job_type=None, job_id=None, metadata=None, context=None):
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
    # Assuming add_job service is available via service_manager
    return await service_manager.add_job(
        instructions=instructions,
        agent_name=agent_name, 
        job_type=job_type,
        job_id=job_id,
        metadata=metadata, 
        context=context
    )

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
        # Note: FileLock is not used here in the original code for read-only access
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
                    # Ignore malformed lines in the index
                    print(f"Warning: Skipping malformed line in {JOB_INDEX}: {line.strip()}")
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
    # Get job data first to ensure it exists and we can update it
    job_data = await get_job_data(job_id) # This should read from QUEUED_DIR
    if not job_data or job_data.get("status") != "queued":
        # If get_job_data found it elsewhere or status isn't queued, something is wrong
        return {"error": "Job not found or not in queued state (race condition?)"}
    
    # Get job type from job data
    original_job_type = job_data.get("job_type", DEFAULT_JOB_TYPE)
    job_type = sanitize_job_type(original_job_type)
    
    # Construct path to job file in the appropriate job_type directory
    job_path = f"{QUEUED_DIR}/{job_type}/{job_id}.json"
    
    # Check if the job file exists in the job_type directory
    if not await aiofiles.os.path.exists(job_path):
        # If not found in job_type directory, check if it's in the root QUEUED_DIR (legacy)
        legacy_job_path = f"{QUEUED_DIR}/{job_id}.json"
        if await aiofiles.os.path.exists(legacy_job_path):
            job_path = legacy_job_path
        else:
            # Check if it's active maybe?
            active_path = f"{ACTIVE_DIR}/{job_type}/{job_id}.json"
            legacy_active_path = f"{ACTIVE_DIR}/{job_id}.json"
            
            if await aiofiles.os.path.exists(active_path) or await aiofiles.os.path.exists(legacy_active_path):
                return {"error": "Job is already active, cannot cancel via this command."}
            return {"error": "Job not found in queued state."}
    
    # Update status and add error message
    job_data["status"] = "failed"
    job_data["error"] = "Job cancelled by user"
    job_data["updated_at"] = datetime.now().isoformat()
    job_data["completed_at"] = job_data["updated_at"] # Mark completion time
    
    # Write to failed directory
    failed_path = f"{FAILED_DIR}/{job_id}.json"
    # Use FileLock here for safety, although moving from QUEUED should be atomic enough
    # from .filelock import FileLock # Assuming FileLock is in filelock.py
    # async with FileLock(failed_path):
    try:
        async with aiofiles.open(failed_path, "w") as f:
            await f.write(json.dumps(job_data, indent=2))
    except Exception as e:
        return {"error": f"Failed to write cancellation status to failed directory: {e}"}

    # Remove from queued *after* successfully writing to failed
    try:
        await aiofiles.os.remove(job_path)
    except FileNotFoundError:
        # Already gone, maybe processed between check and remove? Log warning.
        print(f"Warning: Queued job file {job_path} was already removed during cancellation.")
    except Exception as e:
        # Failed to remove, log but proceed with index update
        print(f"Warning: Failed to remove queued job file {job_path} during cancellation: {e}")

    # Update index
    await update_job_index(job_id, "failed", original_job_type)
    
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
    
    base_status_dir = COMPLETED_DIR if status == "completed" else FAILED_DIR
    cutoff_time = datetime.now().timestamp() - (older_than_days * 24 * 60 * 60)
    removed_count = 0
    jobs_to_remove_from_index = []

    try:
        job_files = await aiofiles.os.listdir(base_status_dir)
    except FileNotFoundError:
        print(f"Directory not found for cleanup: {base_status_dir}")
        return {"removed_count": 0}
    except Exception as e:
        print(f"Error listing directory {base_status_dir}: {e}")
        return {"error": f"Failed to list directory {base_status_dir}"}

    for job_file in job_files:
        if not job_file.endswith(".json"):
            job_type_name = sanitize_job_type(job_file)
            # Check if it's a job type directory
            if status == "completed":  # Only completed uses job type dirs
                job_type_dir = os.path.join(base_status_dir, job_file)
                if await aiofiles.os.path.isdir(job_type_dir):
                    # Process files in this job type directory
                    try:
                        type_job_files = await aiofiles.os.listdir(job_type_dir)
                        for type_job_file in type_job_files:
                            if not type_job_file.endswith(".json"):
                                continue
                                
                            job_path = f"{job_type_dir}/{type_job_file}"
                            job_id = type_job_file.replace(".json", "")
                            
                            try:
                                # Check file modification time
                                stat_result = await aiofiles.os.stat(job_path)
                                file_mtime = stat_result.st_mtime
                                
                                if file_mtime < cutoff_time:
                                    # If old enough, remove it
                                    print(f"Cleaning up job {job_id} from {job_type_dir}")
                                    await aiofiles.os.remove(job_path)
                                    jobs_to_remove_from_index.append((job_id, job_file))  # Use original job_type for index
                                    removed_count += 1
                            except FileNotFoundError:
                                print(f"Job file {job_path} disappeared during cleanup scan.")
                                continue
                            except Exception as e:
                                print(f"Error processing job {job_id} during cleanup: {e}")
                                continue
                    except Exception as e:
                        print(f"Error listing job type directory {job_type_dir}: {e}")
                        continue
            continue  # Skip non-json files in the base directory
            
        # Process files directly in the base directory (legacy or failed)
        job_path = f"{base_status_dir}/{job_file}"
        job_id = job_file.replace(".json", "")
        
        try:
            # Check file modification time first as a quick filter
            stat_result = await aiofiles.os.stat(job_path)
            file_mtime = stat_result.st_mtime
            
            should_remove = False
            if file_mtime < cutoff_time:
                # If mtime is old enough, check internal timestamp for accuracy
                job_data = await get_job_data(job_id) # Reads from the specific status_dir
                if job_data:
                    timestamp_key = "completed_at" # Both completed and failed should have this
                    if timestamp_key in job_data and job_data[timestamp_key]:
                        try:
                            internal_time = datetime.fromisoformat(job_data[timestamp_key]).timestamp()
                            if internal_time < cutoff_time:
                                should_remove = True
                        except (ValueError, TypeError):
                            # Invalid timestamp format, rely on mtime
                            print(f"Warning: Invalid timestamp format in job {job_id}, using file mtime for cleanup.")
                            should_remove = True # Fallback to mtime check
                    else:
                         # No internal timestamp, rely on mtime
                         should_remove = True # Fallback to mtime check
                else:
                    # Couldn't read job data, maybe corrupted? Remove based on mtime.
                    print(f"Warning: Could not read job data for {job_id} during cleanup, using file mtime.")
                    should_remove = True
            
            if should_remove:
                print(f"Cleaning up job {job_id} from {base_status_dir}")
                await aiofiles.os.remove(job_path)
                jobs_to_remove_from_index.append((job_id, DEFAULT_JOB_TYPE))
                removed_count += 1

        except FileNotFoundError:
             print(f"Job file {job_path} disappeared during cleanup scan.")
             continue # File was removed by another process?
        except Exception as e:
            print(f"Error processing job {job_id} during cleanup: {e}")
            # Decide whether to skip or try to remove from index anyway
            # For now, just log and continue
            continue
    
    # Update the index - remove all cleaned jobs in one go potentially
    # (Original code updated index inside the loop, which is less efficient)
    # This requires modifying update_job_index or creating a bulk version.
    # For simplicity, sticking to original logic for now:
    for job_id_to_remove, job_type in jobs_to_remove_from_index:
         await update_job_index(job_id_to_remove, "removed", job_type)

    return {"removed_count": removed_count}
 
