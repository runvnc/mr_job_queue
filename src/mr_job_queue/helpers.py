import os
import json
import aiofiles
import aiofiles.os
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------
JOB_DIR = "data/jobs"
QUEUED_DIR = f"{JOB_DIR}/queued"
ACTIVE_DIR = f"{JOB_DIR}/active"
COMPLETED_DIR = f"{JOB_DIR}/completed"
FAILED_DIR = f"{JOB_DIR}/failed"
PAUSED_DIR = f"{JOB_DIR}/paused"

# ---------------------------------------------------------------------------
# sanitize_job_type
# ---------------------------------------------------------------------------
def sanitize_job_type(job_type):
    """Sanitize job_type to ensure it's safe for use in file paths."""
    if not job_type:
        return "default"
    return re.sub(r'[/\\?%*:|"<>]', '_', job_type)

# ---------------------------------------------------------------------------
# update_job_index (no-op)
# ---------------------------------------------------------------------------
async def update_job_index(job_id, new_status, job_type=None):
    """No-op function; job index is deprecated."""
    pass

# ---------------------------------------------------------------------------
# get_job_data
# ---------------------------------------------------------------------------
async def get_job_data(job_id):
    """Get a job's data from any of the job directories."""
    for base_dir in (QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, PAUSED_DIR):
        try:
            if await aiofiles.os.path.exists(base_dir):
                for job_type_dir in await aiofiles.os.listdir(base_dir):
                    job_type_path = os.path.join(base_dir, job_type_dir)
                    if await aiofiles.os.path.isdir(job_type_path):
                        job_path = f"{job_type_path}/{job_id}.json"
                        if await aiofiles.os.path.exists(job_path):
                            async with aiofiles.open(job_path, "r") as f:
                                return json.loads(await f.read())
        except Exception as e:
            print(f"Error checking {base_dir} for job {job_id}: {e}")

    # Check FAILED_DIR (flat structure)
    failed_path = f"{FAILED_DIR}/{job_id}.json"
    if await aiofiles.os.path.exists(failed_path):
        async with aiofiles.open(failed_path, "r") as f:
            return json.loads(await f.read())
            
    return None # Not found
