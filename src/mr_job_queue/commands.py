import os, json, heapq, asyncio
from datetime import datetime, timedelta
import aiofiles
import aiofiles.os
from typing import Dict, Any, Optional, List
import time

from lib.providers.commands import command
from lib.providers.services import service_manager

# ---------------------------------------------------------------------------
# Job directory locations
# ---------------------------------------------------------------------------
JOB_DIR         = "data/jobs"
QUEUED_DIR      = f"{JOB_DIR}/queued"
ACTIVE_DIR      = f"{JOB_DIR}/active"
COMPLETED_DIR   = f"{JOB_DIR}/completed"
FAILED_DIR      = f"{JOB_DIR}/failed"
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
        
    def _make_key(self, status, job_type, username, limit) -> str:
        """Generate cache key from query parameters."""
        return f"{status}:{job_type}:{username}:{limit}"
    
    def _is_expired(self, timestamp: float) -> bool:
        """Check if cache entry is expired."""
        return (time.time() - timestamp) > self.ttl_seconds
    
    async def get(self, status, job_type, username, limit) -> Optional[List[Dict]]:
        """Get cached jobs if available and not expired."""
        key = self._make_key(status, job_type, username, limit)
        async with self._lock:
            if key in self._cache:
                timestamp, data = self._cache[key]
                if not self._is_expired(timestamp):
                    return data
        return None
    
    async def set(self, status, job_type, username, limit, data: List[Dict]):
        """Cache job listing."""
        key = self._make_key(status, job_type, username, limit)
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
                for key in keys_to_remove:
                    del self._cache[key]
    
    async def start_background_refresh(self):
        """Start background task to refresh cache periodically."""
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._refresh_loop())
    
    async def _refresh_loop(self):
        """Background task to refresh commonly-used cache entries."""
        while True:
            try:
                await asyncio.sleep(self.ttl_seconds / 2)  # Refresh at half TTL
                
                # Refresh common queries
                common_queries = [
                    (None, None, None, 50),  # All jobs
                    ("queued", None, None, 50),  # Queued jobs
                    ("active", None, None, 50),  # Active jobs
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
_job_cache = JobCache(ttl_seconds=5)

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
    # Try cache first
    cached = await _job_cache.get(status, job_type, username, limit)
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
