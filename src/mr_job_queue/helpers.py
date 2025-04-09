import os
import json
import aiofiles
import aiofiles.os
from datetime import datetime

from .filelock import FileLock

# Assuming these are defined elsewhere or passed in
JOB_DIR = "data/jobs"
QUEUED_DIR = f"{JOB_DIR}/queued"
ACTIVE_DIR = f"{JOB_DIR}/active"
COMPLETED_DIR = f"{JOB_DIR}/completed"
FAILED_DIR = f"{JOB_DIR}/failed"
JOB_INDEX = f"{JOB_DIR}/job_index.jsonl"

# Helper functions
async def update_job_index(job_id, new_status):
    """Update a job's status in the index file"""
    if not os.path.exists(JOB_INDEX):
        # If index doesn't exist, we can't update it.
        # If status is 'removed', this is fine.
        # If adding a new job, the add_job function handles initial creation.
        print(f"Warning: Job index {JOB_INDEX} not found during update for job {job_id}.")
        return

    # Use a lock to prevent concurrent modifications
    async with FileLock(JOB_INDEX):
        temp_index = f"{JOB_INDEX}.temp"
        found = False
        entries_to_keep = []

        try:
            # Read existing entries
            async with aiofiles.open(JOB_INDEX, "r") as f_in:
                async for line in f_in:
                    try:
                        job_entry = json.loads(line.strip())
                        if job_entry["id"] == job_id:
                            if new_status != "removed":
                                job_entry["status"] = new_status
                                entries_to_keep.append(job_entry)
                            # If new_status is 'removed', we simply don't append it
                            found = True
                        else:
                            entries_to_keep.append(job_entry)
                    except json.JSONDecodeError:
                        # Keep malformed lines as they are? Or discard?
                        # Discarding seems safer to prevent index corruption.
                        print(f"Warning: Discarding malformed line in {JOB_INDEX}: {line.strip()}")
                        pass # Discard malformed line

            # If job wasn't found and we are not removing, add it as a new entry
            if not found and new_status != "removed":
                # Get job data from its current location to populate the index entry
                job_data = await get_job_data(job_id)
                if job_data:
                    index_entry = {
                        "id": job_id,
                        "status": new_status,
                        "agent_name": job_data.get("agent_name", ""),
                        "job_type": job_data.get("job_type", ""),
                        "created_at": job_data.get("created_at", datetime.now().isoformat()),
                        "username": job_data.get("username", "system")
                    }
                    entries_to_keep.append(index_entry)
                else:
                    # Cannot find job data to create index entry
                    print(f"Warning: Could not find job data for {job_id} to add to index.")

            # Write the updated entries to the temporary file
            async with aiofiles.open(temp_index, "w") as f_out:
                for entry in entries_to_keep:
                    await f_out.write(json.dumps(entry) + "\
")

            # Replace original with updated index atomically
            await aiofiles.os.rename(temp_index, JOB_INDEX)

        except Exception as e:
            print(f"Error updating job index {JOB_INDEX}: {e}")
            # Attempt to remove temp file if it exists
            try:
                if await aiofiles.os.path.exists(temp_index):
                    await aiofiles.os.remove(temp_index)
            except Exception as cleanup_e:
                print(f"Error cleaning up temp index file {temp_index}: {cleanup_e}")

async def get_job_data(job_id):
    """Get a job's data from any of the job directories"""
    for status_dir in [QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, FAILED_DIR]:
        job_path = f"{status_dir}/{job_id}.json"
        try:
            if await aiofiles.os.path.exists(job_path):
                async with aiofiles.open(job_path, "r") as f:
                    content = await f.read()
                    return json.loads(content)
        except FileNotFoundError:
            # Might have been moved between exists check and open
            continue
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for job {job_id} in {status_dir}: {e}")
            # Return None or raise? Returning None seems safer.
            return None
        except Exception as e:
            print(f"Error reading job data for {job_id} from {status_dir}: {e}")
            return None # Indicate error reading data
    return None # Not found in any directory
