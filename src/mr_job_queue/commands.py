import os, json, heapq, asyncio
from datetime import datetime, timedelta
import aiofiles
import aiofiles.os
import aiofiles.os
import aiofiles.os
from typing import Dict, Any, Optional, List
import time

from lib.providers.commands import command
from lib.providers.hooks import hook_manager

from lib.providers.services import service_manager

# ---------------------------------------------------------------------------
# Job directory locations
# ---------------------------------------------------------------------------
JOB_DIR         = "data/jobs"
QUEUED_DIR      = f"{JOB_DIR}/queued"
ACTIVE_DIR      = f"{JOB_DIR}/active"
COMPLETED_DIR   = f"{JOB_DIR}/completed"
FAILED_DIR      = f"{JOB_DIR}/failed"
PAUSED_DIR      = f"{JOB_DIR}/paused"
DEFAULT_JOB_TYPE = "default"

# Helpers from mr_job_queue.helpers
from .helpers import get_job_data, sanitize_job_type

# ---------------------------------------------------------------------------
# IN-MEMORY CACHE FOR JOB LISTINGS
# ---------------------------------------------------------------------------
class JobCache:
    """In-memory cache for job listings to avoid filesystem scanning during active calls."""
    
    def __init__(self, ttl_seconds: int = 5):
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, tuple[float, List[Dict]]] = {}  # key -> (timestamp, data)
        self._lock = asyncio.Lock()
        self._refresh_task: Optional[asyncio.Task] = None
        self._last_fs_scan_mtime: float = 0
        
    def _make_key(self, status, job_type, username, limit) -> str:
        """Generate cache key from query parameters."""
        return f"{status}:{job_type}:{username}:{limit}"
    
    def _is_expired(self, timestamp: float) -> bool:
        """Check if cache entry is expired."""
        return (time.time() - timestamp) > self.ttl_seconds
    
    async def get(self, status, job_type, username, limit) -> Optional[List[Dict]]:
        """Get cached jobs if available and not expired."""
        key = self._make_key(status, job_type, username, limit)
        print(f"[JobCache] Checking cache for key: {key}")
        async with self._lock:
            if key in self._cache:
                timestamp, data = self._cache[key]
                if not self._is_expired(timestamp):
                    return data
        return None
    
    async def set(self, status, job_type, username, limit, data: List[Dict]):
        """Cache job listing."""
        key = self._make_key(status, job_type, username, limit)
        print(f"[JobCache] Setting cache for key: {key} with {len(data)} jobs")
        async with self._lock:
            self._cache[key] = (time.time(), data)
    
    async def invalidate(self, status: Optional[str] = None):
        """Invalidate cache entries for a specific status or all."""
        async with self._lock:
            if status is None:
                self._cache.clear()
            else:
                # Remove all entries matching this status
                keys_to_remove = [k for k in self._cache.keys() if k.startswith(f"{status}:")]
                # ALSO remove entries for "None" (ALL) status, as they include this status
                keys_to_remove.extend([k for k in self._cache.keys() if k.startswith("None:")])
                for key in keys_to_remove:
                    if key in self._cache:
                        del self._cache[key]
    
    async def extend_ttl(self):
        """Extend the TTL of all current cache entries."""
        async with self._lock:
            now = time.time()
            for key in self._cache:
                _, data = self._cache[key]
                self._cache[key] = (now, data)

    async def _check_if_fs_changed(self) -> bool:
        """Check if any job directories have been modified since last scan."""
        def _scan_sync():
            """Synchronous helper to scan all mtimes without blocking event loop."""
            max_mtime = 0.0
            dirs_to_check = [QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, FAILED_DIR]
            
            for base_dir in dirs_to_check:
                try:
                    if os.path.isdir(base_dir):
                        for entry in os.scandir(base_dir):
                            # Check mtime of the entry (file or dir)
                            max_mtime = max(max_mtime, entry.stat().st_mtime)
                            
                            # If it's a directory (job type subdir), scan its children too
                            if entry.is_dir():
                                for sub_entry in os.scandir(entry.path):
                                    max_mtime = max(max_mtime, sub_entry.stat().st_mtime)
                except FileNotFoundError:
                    continue
                except Exception as e:
                    print(f"[JobCache] Error checking mtime for {base_dir}: {e}")
            return max_mtime

        max_mtime = await asyncio.to_thread(_scan_sync)
        if max_mtime > self._last_fs_scan_mtime:
            self._last_fs_scan_mtime = max_mtime
            return True
        return False

    async def start_background_refresh(self):
        """Start background task to refresh cache periodically."""
        if self._refresh_task is None or self._refresh_task.done():
            print("[JobCache] Starting background refresh task")
            self._refresh_task = asyncio.create_task(self._refresh_loop())
    
    async def _refresh_loop(self):
        """Background task to refresh commonly-used cache entries."""
        while True:
            try:
                await asyncio.sleep(5)  # Refresh every 5 seconds
                
                # Optimization: Check directory mtimes first
                if not await self._check_if_fs_changed():
                    # No changes on disk, skip heavy JSON parsing
                    # IMPORTANT: Extend TTL so valid data doesn't expire
                    await self.extend_ttl()
                    continue

                # Files changed - invalidate all existing cache to prevent stale reads
                # for queries not in our common list
                await self.invalidate()

                # Refresh common queries
                common_queries = [
                    (None, None, None, 100),  # All jobs
                    ("queued", None, None, 100),  # Queued jobs
                    ("active", None, None, 100),  # Active jobs
                    (None, None, None, 10000),  # Search jobs (large limit)
                ]
                
                for status, job_type, username, limit in common_queries:
                    try:
                        # Scan filesystem and update cache
                        jobs = await _scan_jobs_from_filesystem(status, job_type, username, limit)
                        await self.set(status, job_type, username, limit, jobs)
                    except Exception as e:
                        print(f"[JobCache] Error refreshing cache for {status}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[JobCache] Error in refresh loop: {e}")
                await asyncio.sleep(5)  # Back off on error

# Global cache instance
_job_cache = JobCache(ttl_seconds=15)

# Start background refresh on module load
asyncio.create_task(_job_cache.start_background_refresh())

# ---------------------------------------------------------------------------
# OPTIMIZED FILESYSTEM SCANNING (used by cache)
# ---------------------------------------------------------------------------
async def _scan_jobs_from_filesystem(status=None, job_type=None, username=None, limit:int=50) -> List[Dict]:
    """Scan filesystem for jobs - optimized version with batching.
    
    This is the core scanning logic, now separated for caching.
    """
    # Async helpers with batching
    async def listdir(path):
        return await asyncio.to_thread(lambda p=path: os.listdir(p) if os.path.isdir(p) else [])

    async def scandir(path):
        return await asyncio.to_thread(lambda p=path: list(os.scandir(p)) if os.path.isdir(p) else [])

    async def load_json_batch(paths: List[str], force_status=None) -> List[Dict]:
        """Load multiple JSON files in parallel."""
        async def load_one(path):
            try:
                async with aiofiles.open(path, "r") as f:
                    data = json.loads(await f.read())
                if force_status:
                    data["status"] = force_status
                return data
            except Exception as e:
                print(f"[_scan_jobs] Error loading {path}: {e}")
                return None
        
        results = await asyncio.gather(*[load_one(p) for p in paths], return_exceptions=True)
        return [r for r in results if r is not None and not isinstance(r, Exception)]

    wants = lambda s: status is None or status == s
    out = []

    # Collect all paths first, then batch load
    paths_to_load = []
    
    # queued / active / failed ----------------------------------------------
    for st, base in (("queued", QUEUED_DIR), ("active", ACTIVE_DIR), ("failed", FAILED_DIR)):
        if not wants(st):
            continue

        # legacy root-level files
        for de in await scandir(base):
            if de.name.endswith(".json") and not de.is_dir():
                paths_to_load.append((de.path, st if base==FAILED_DIR else None))

        # job-type subdirs
        for sub in await listdir(base):
            sp = os.path.join(base, sub)
            if not os.path.isdir(sp):
                continue
            if job_type and sanitize_job_type(job_type) != sanitize_job_type(sub):
                continue
            for de in await scandir(sp):
                if de.name.endswith(".json"):
                    paths_to_load.append((de.path, st if base==FAILED_DIR else None))
    
    # Batch load all collected paths
    if paths_to_load:
        # Group by force_status for batch loading
        by_status = {}
        for path, force_status in paths_to_load:
            if force_status not in by_status:
                by_status[force_status] = []
            by_status[force_status].append(path)
        
        for force_status, paths in by_status.items():
            batch_results = await load_json_batch(paths, force_status)
            out.extend(batch_results)

    # completed – newest limited --------------------------------------------
    if wants("completed") and limit > 0:
        heap = []  # (mtime,path)
        
        # Collect all completed job paths with mtimes
        completed_paths = []
        for de in await scandir(COMPLETED_DIR):
            if de.name.endswith(".json") and not de.is_dir():
                completed_paths.append(de.path)
        
        for sub in await listdir(COMPLETED_DIR):
            sp = os.path.join(COMPLETED_DIR, sub)
            if not os.path.isdir(sp):
                continue
            if job_type and sanitize_job_type(job_type) != sanitize_job_type(sub):
                continue
            for de in await scandir(sp):
                if de.name.endswith(".json"):
                    completed_paths.append(de.path)
        
        # Get mtimes in batch
        async def get_mtime(p):
            try:
                stat = await aiofiles.os.stat(p)
                return (stat.st_mtime, p)
            except:
                return None
        
        mtime_results = await asyncio.gather(*[get_mtime(p) for p in completed_paths], return_exceptions=True)
        mtime_results = [r for r in mtime_results if r is not None and not isinstance(r, Exception)]
        
        # Build heap
        for mtime, path in mtime_results:
            if len(heap) < limit * 2:
                heapq.heappush(heap, (mtime, path))
            else:
                heapq.heappushpop(heap, (mtime, path))
        
        # Load top N completed jobs
        top_paths = [p for _, p in sorted(heap, reverse=True)[:limit]]
        if top_paths:
            completed_jobs = await load_json_batch(top_paths, force_status="completed")
            out.extend(completed_jobs)

    # username filter --------------------------------------------------------
    if username:
        out = [j for j in out if j.get("username") == username]

    # Sort all collected jobs by creation date descending
    out.sort(key=lambda j: j.get("created_at", ""), reverse=True)

    # Apply limit
    if status is None:
        return out[:limit]
    
    return out

# ---------------------------------------------------------------------------
# submit_job – thin wrapper around service_manager.add_job
# ---------------------------------------------------------------------------
@command()
async def submit_job(instructions, agent_name, job_type=None, job_id=None, metadata=None, context=None):
    result = await service_manager.add_job(
        instructions=instructions,
        agent_name=agent_name,
        job_type=job_type,
        job_id=job_id,
        metadata=metadata,
        context=context,
    )
    # Invalidate cache for queued jobs
    await _job_cache.invalidate("queued")
    return result

# ---------------------------------------------------------------------------
# get_job_status
# ---------------------------------------------------------------------------
@command()
async def get_job_status(job_id, context=None):
    jd = await get_job_data(job_id)
    return jd if jd else {"error": "Job not found"}

# ---------------------------------------------------------------------------
# get_jobs – OPTIMIZED with caching
# ---------------------------------------------------------------------------
@command()
async def get_jobs(status=None, job_type=None, username=None, limit:int=50, context=None):
    """Return jobs with in-memory caching to avoid filesystem scanning during active calls.

    status      – queued|active|completed|failed|None
    job_type    – original job_type string or None
    username    – filter by creator, None = everyone
    limit       – max completed jobs to return (others unlimited)
    """
    # Ensure background refresh is running
    await _job_cache.start_background_refresh()

    # Try cache first
    print(f"[get_jobs] Checking cache: status={status or 'ALL'} jt={job_type or 'ALL'} user={username or 'ANY'}")
    cached = await _job_cache.get(status, job_type, username, limit)
    print(f"[get_jobs] Cache check complete.")
    if cached is not None:
        print(f"[get_jobs] CACHE HIT: status={status or 'ALL'} jt={job_type or 'ALL'} user={username or 'ANY'} -> {len(cached)} jobs")
        return cached
    
    # Cache miss - scan filesystem
    print(f"[get_jobs] CACHE MISS: scanning filesystem...")
    jobs = await _scan_jobs_from_filesystem(status, job_type, username, limit)
    
    # Update cache
    await _job_cache.set(status, job_type, username, limit, jobs)
    
    print(f"[get_jobs] status={status or 'ALL'} jt={job_type or 'ALL'} user={username or 'ANY'} -> {len(jobs)} jobs")
    return jobs

# ---------------------------------------------------------------------------
# cancel_job (invalidate cache)
# ---------------------------------------------------------------------------
@command()
async def cancel_job(job_id, context=None):
    jd = await get_job_data(job_id)
    if not jd or jd.get("status") != "queued":
        return {"error": "Job not found or not queued"}
    ojt = jd.get("job_type", DEFAULT_JOB_TYPE)
    sjt = sanitize_job_type(ojt)
    qpath = f"{QUEUED_DIR}/{sjt}/{job_id}.json"
    if not await aiofiles.os.path.exists(qpath):
        return {"error": "Job file missing"}

    jd.update({
        "status": "failed",
        "error": "Job cancelled by user",
        "updated_at": datetime.now().isoformat(),
        "completed_at": datetime.now().isoformat()
    })
    fpath = f"{FAILED_DIR}/{job_id}.json"
    async with aiofiles.open(fpath, "w") as f:
        await f.write(json.dumps(jd, indent=2))
    await aiofiles.os.remove(qpath)
    
    # Invalidate cache
    await _job_cache.invalidate("queued")
    await _job_cache.invalidate("failed")
    
    # Trigger hook
    await hook_manager.job_ended("cancelled", jd, None, context=None)
    
    return {"success": True}

# ---------------------------------------------------------------------------
# cleanup_jobs (simple)
# ---------------------------------------------------------------------------
@command()
async def cleanup_jobs(status="completed", older_than_days:int=30, context=None):
    if status not in ("completed", "failed"):
        return {"error": "status must be completed|failed"}
    base = COMPLETED_DIR if status == "completed" else FAILED_DIR
    cutoff = datetime.now().timestamp() - older_than_days * 24 * 3600
    removed = 0
    for root, _, files in await asyncio.to_thread(lambda: list(os.walk(base))):
        for fn in files:
            if not fn.endswith(".json"): 
                continue
            p = os.path.join(root, fn)
            try:
                st = await aiofiles.os.stat(p)
                if st.st_mtime < cutoff:
                    await aiofiles.os.remove(p)
                    removed += 1
            except FileNotFoundError:
                continue
    
    # Invalidate cache
    await _job_cache.invalidate(status)
    
    return {"removed_count": removed}

# ---------------------------------------------------------------------------
# search_jobs – search jobs with metadata filtering and date range
# ---------------------------------------------------------------------------
@command()
async def search_jobs(
    metadata_query: Optional[Dict[str, Any]] = None,
    before_date: Optional[str] = None,
    after_date: Optional[str] = None,
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    username: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    context=None
):
    """Search jobs with flexible filtering on metadata, dates, and other fields.
    
    Uses cached get_jobs for better performance.
    """
    from datetime import datetime
    
    # Parse date strings to datetime objects for comparison
    before_dt = datetime.fromisoformat(before_date) if before_date else None
    after_dt = datetime.fromisoformat(after_date) if after_date else None
    
    # Get all jobs matching status/job_type/username filters (use cache)
    all_jobs = await get_jobs(
        status=status, 
        job_type=job_type, 
        username=username, 
        limit=10000,  # Large limit to get all potential matches
        context=context
    )
    
    # Apply additional filters
    filtered_jobs = []
    for job in all_jobs:
        # Skip if no created_at timestamp
        created_at_str = job.get("created_at")
        if not created_at_str:
            continue
            
        try:
            job_created_dt = datetime.fromisoformat(created_at_str)
        except ValueError:
            continue
        
        # Date range filtering
        if before_dt and job_created_dt >= before_dt:
            continue
        if after_dt and job_created_dt <= after_dt:
            continue
        
        # Metadata field matching
        if metadata_query:
            job_metadata = job.get("metadata", {}) or {}
            match = True
            for key, value in metadata_query.items():
                if job_metadata.get(key) != value:
                    match = False
                    break
            if not match:
                continue
        
        filtered_jobs.append(job)
    
    # Sort by created_at descending (newest first)
    filtered_jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    
    # Apply pagination
    total_count = len(filtered_jobs)
    paginated_jobs = filtered_jobs[offset:offset + limit]
    
    return {
        "jobs": paginated_jobs,
        "total_count": total_count,
        "offset": offset,
        "limit": limit
    }

# ---------------------------------------------------------------------------
# delegate_job - queue a job and wait for completion
# ---------------------------------------------------------------------------
@command()
async def delegate_job(instructions: str, agent_name: str, job_type: str = None, retries: int = 3,
                       timeout: int = 600, job_id: str = None, metadata: dict = None, 
                       context=None):
    """
    Delegate a task to another agent via the job queue, waiting for completion.
    
    Unlike delegate_task which runs immediately, this queues the job and waits
    for it to be processed by the job queue worker. This respects rate limits
    and concurrency settings of the job queue.
    
    Parameters:
        instructions: The task instructions for the agent
        agent_name: Name of the agent to run the task
        job_type: Optional job type for queue organization (default: "delegated.{agent_name}")
        retries: Number of retries if task fails (default: 3) - passed to underlying run_task
        timeout: Maximum seconds to wait for completion (default: 600 = 10 minutes)
        job_id: Optional custom job ID (auto-generated if not provided)
        metadata: Optional dict of metadata to attach to the job
    
    Returns:
        Result from the completed job, or error message if failed/timed out
    
    Example:
    
    { "delegate_job": { 
        "instructions": "Analyze this document and summarize key points",
        "agent_name": "analyst",
        "timeout": 300
    }}
    """
    # Use agent_name as default job_type if not specified
    if job_type is None:
        job_type = f"delegated.{agent_name}"
    
    # Get LLM from context if available
    llm = None
    if context is not None:
        if hasattr(context, 'current_model'):
            llm = context.current_model
        elif hasattr(context, 'data') and 'llm' in context.data:
            llm = context.data['llm']
    
    # Get username from context
    username = getattr(context, 'username', None) if context else None
    
    # Build metadata with retries info
    job_metadata = metadata.copy() if metadata else {}
    job_metadata['retries'] = retries
    if context and hasattr(context, 'log_id'):
        job_metadata['parent_log_id'] = context.log_id
    
    # Queue the job
    result = await service_manager.add_job(
        instructions=instructions,
        agent_name=agent_name,
        job_type=job_type,
        username=username,
        metadata=job_metadata,
        job_id=job_id,
        llm=llm,
        context=context
    )
    
    if "error" in result:
        return f"Failed to queue job: {result['error']}"
    
    queued_job_id = result["job_id"]
    
    # Note: job uses job_id as log_id, so they're the same
    # Invalidate cache for queued jobs
    await _job_cache.invalidate("queued")
    
    # Wait for job completion
    job_result = await service_manager.wait_for_job(
        job_id=queued_job_id,
        timeout=timeout,
        context=context
    )
    
    # Format result
    status = job_result.get("status", "unknown")
    if status == "completed":
        text = job_result.get("result")
        # Fallback for empty results - match delegate_task behavior
        if text is None or text == '' or text == [] or text == '[]':
            from lib.chatlog import ChatLog
            chatlog = ChatLog(log_id=queued_job_id, user=username, agent=agent_name)
            text = json.dumps(chatlog.messages)
        return f'<a href="/session/{agent_name}/{queued_job_id}" target="_blank">Task completed with log ID: {queued_job_id}</a>\nResults:\n\n{text}'
    elif status == "failed":
        error = job_result.get("error", "Unknown error")
        return f'Job {queued_job_id} failed: {error}'
    elif status == "timeout":
        return f'Job {queued_job_id} timed out after {timeout} seconds'
    else:
        return f'Job {queued_job_id} ended with unexpected status: {status}'

# ---------------------------------------------------------------------------
# pause_job - pause a queued job (prevents it from being processed)
# ---------------------------------------------------------------------------
@command()
async def pause_job(job_id: str, context=None):
    """Pause a queued job, preventing it from being processed.
    
    The job can be resumed later with resume_job.
    Only queued jobs can be paused (not active or completed jobs).
    
    Parameters:
    job_id - String. The ID of the job to pause.
    
    Returns:
    Status of the pause operation.
    
    Example:
    { "pause_job": { "job_id": "job_abc123" } }
    """
    jd = await get_job_data(job_id)
    if not jd:
        return {"error": "Job not found"}
    
    if jd.get("status") != "queued":
        return {"error": f"Cannot pause job with status '{jd.get('status')}'. Only queued jobs can be paused."}
    
    ojt = jd.get("job_type", DEFAULT_JOB_TYPE)
    sjt = sanitize_job_type(ojt)
    qpath = f"{QUEUED_DIR}/{sjt}/{job_id}.json"
    
    if not await aiofiles.os.path.exists(qpath):
        return {"error": "Job file missing from queue"}
    
    # Create paused directory if needed
    paused_type_dir = f"{PAUSED_DIR}/{sjt}"
    await aiofiles.os.makedirs(paused_type_dir, exist_ok=True)
    
    # Update job status and move to paused directory
    jd.update({
        "status": "paused",
        "paused_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    })
    
    ppath = f"{paused_type_dir}/{job_id}.json"
    async with aiofiles.open(ppath, "w") as f:
        await f.write(json.dumps(jd, indent=2))
    
    await aiofiles.os.remove(qpath)
    
    # Invalidate cache
    await _job_cache.invalidate("queued")
    
    return {"success": True, "status": "paused", "job_id": job_id}

# ---------------------------------------------------------------------------
# resume_job - resume a paused job
# ---------------------------------------------------------------------------
@command()
async def resume_job(job_id: str, context=None):
    """Resume a paused job, putting it back in the queue.
    
    Parameters:
    job_id - String. The ID of the job to resume.
    
    Returns:
    Status of the resume operation.
    
    Example:
    { "resume_job": { "job_id": "job_abc123" } }
    """
    jd = await get_job_data(job_id)
    if not jd:
        return {"error": "Job not found"}
    
    if jd.get("status") != "paused":
        return {"error": f"Cannot resume job with status '{jd.get('status')}'. Only paused jobs can be resumed."}
    
    ojt = jd.get("job_type", DEFAULT_JOB_TYPE)
    sjt = sanitize_job_type(ojt)
    ppath = f"{PAUSED_DIR}/{sjt}/{job_id}.json"
    
    if not await aiofiles.os.path.exists(ppath):
        return {"error": "Job file missing from paused directory"}
    
    # Update job status and move back to queued directory
    jd.update({
        "status": "queued",
        "resumed_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    })
    # Remove paused_at if present
    jd.pop("paused_at", None)
    
    queued_type_dir = f"{QUEUED_DIR}/{sjt}"
    await aiofiles.os.makedirs(queued_type_dir, exist_ok=True)
    
    qpath = f"{queued_type_dir}/{job_id}.json"
    async with aiofiles.open(qpath, "w") as f:
        await f.write(json.dumps(jd, indent=2))
    
    await aiofiles.os.remove(ppath)
    
    # Invalidate cache
    await _job_cache.invalidate("queued")
    
    return {"success": True, "status": "queued", "job_id": job_id}

# ---------------------------------------------------------------------------
# continue_job - create a new job to continue from a previous job
# ---------------------------------------------------------------------------
@command()
async def continue_job(job_id: str, additional_instructions: str = None, context=None):
    """Continue a job by creating a new job with the same parameters.
    
    This is useful for resuming work on a completed or failed job,
    or for scheduling a job to continue at a later time.
    
    Parameters:
    job_id - String. The ID of the job to continue from.
    additional_instructions - String. Optional additional instructions to append.
    
    Returns:
    The new job ID and status.
    
    Example:
    { "continue_job": { "job_id": "job_abc123" } }
    
    Example with additional instructions:
    { "continue_job": { 
        "job_id": "job_abc123",
        "additional_instructions": "Focus on the remaining items from the previous run."
    }}
    """
    # Get the original job data
    jd = await get_job_data(job_id)
    if not jd:
        return {"error": "Original job not found"}
    
    # Build new instructions
    original_instructions = jd.get("instructions", "")
    if additional_instructions:
        new_instructions = f"{original_instructions}\n\n[CONTINUATION]\n{additional_instructions}"
    else:
        new_instructions = f"{original_instructions}\n\n[CONTINUATION]\nPlease continue from where the previous job left off."
    
    # Build metadata linking to original job
    new_metadata = jd.get("metadata", {}).copy() if jd.get("metadata") else {}
    new_metadata["continued_from"] = job_id
    new_metadata["original_job_id"] = jd.get("metadata", {}).get("original_job_id", job_id)
    
    # Get username from context or original job
    username = getattr(context, 'username', None) if context else None
    if not username:
        username = jd.get("username", "system")
    
    # Create the new job
    result = await service_manager.add_job(
        instructions=new_instructions,
        agent_name=jd.get("agent_name"),
        job_type=jd.get("job_type"),
        username=username,
        metadata=new_metadata,
        llm=jd.get("llm"),
        context=context
    )
    
    if "error" in result:
        return result
    
    # Invalidate cache
    await _job_cache.invalidate("queued")
    
    return {
        "success": True,
        "new_job_id": result["job_id"],
        "continued_from": job_id,
        "status": "queued"
    }

# ---------------------------------------------------------------------------
# request_job_pause - agent command to pause the current running job
# ---------------------------------------------------------------------------
@command()
async def request_job_pause(reason: str = None, context=None):
    """Request to pause the current job (for use by agents during job execution).
    
    This command allows an agent to pause its own job during execution.
    The job will be moved to the paused state and will NOT be retried.
    Use continue_job() later to resume the job.
    
    Parameters:
    reason - String. Optional reason for pausing the job.
    
    Returns:
    A pause confirmation that signals the job system to stop processing.
    
    Example:
    { "request_job_pause": { "reason": "Waiting for external data" } }
    
    Example in a task flow:
    { "request_job_pause": { "reason": "Rate limited, will continue later" } }
    """
    if context is None:
        return {"error": "No context available"}
    
    # Get the job_id from context (log_id is used as job_id)
    job_id = getattr(context, 'log_id', None)
    if not job_id:
        return {"error": "No job_id found in context"}
    
    # Check if this is actually a job (job IDs start with 'job_')
    if not job_id.startswith('job_'):
        return {"error": "This command can only be used within a job context"}
    
    # Set flags in context to signal pause
    context.data['job_pause_requested'] = True
    context.data['job_pause_reason'] = reason or "Pause requested by agent"
    context.data['finished_conversation'] = True  # Stop the agent loop
    
    # Build the pause result
    pause_result = {
        "status": "paused",
        "job_id": job_id,
        "reason": reason or "Pause requested by agent",
        "message": "Job paused. Use continue_job() to resume."
    }
    context.data['job_paused_result'] = pause_result  # Extra flag for process_job to detect
    # Set task_result so the fallback in send_message_to_agent adds it to full_results
    # with the 'output' key that results_output() looks for (prevents retries)
    context.data['task_result'] = pause_result
    
    await context.save_context()
    
    # Return 'stop' to halt processing - the fallback will handle adding to full_results
    return 'stop'
