import os, json, heapq, asyncio
from datetime import datetime
import aiofiles
import aiofiles.os

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
# submit_job – thin wrapper around service_manager.add_job
# ---------------------------------------------------------------------------
@command()
async def submit_job(instructions, agent_name, job_type=None, job_id=None, metadata=None, context=None):
    return await service_manager.add_job(
        instructions=instructions,
        agent_name=agent_name,
        job_type=job_type,
        job_id=job_id,
        metadata=metadata,
        context=context,
    )

# ---------------------------------------------------------------------------
# get_job_status
# ---------------------------------------------------------------------------
@command()
async def get_job_status(job_id, context=None):
    jd = await get_job_data(job_id)
    return jd if jd else {"error": "Job not found"}

# ---------------------------------------------------------------------------
# get_jobs – walk filesystem (no job_index.jsonl)
# ---------------------------------------------------------------------------
@command()
async def get_jobs(status=None, job_type=None, username=None, limit:int=50, context=None):
    """Return jobs by scanning data/jobs/* directories.

    status      – queued|active|completed|failed|None
    job_type    – original job_type string or None
    username    – filter by creator, None = everyone
    limit       – max completed jobs to return (others unlimited)
    """
    # Async helpers -----------------------------------------------------------
    async def listdir(path):
        return await asyncio.to_thread(lambda p=path: os.listdir(p) if os.path.isdir(p) else [])

    async def scandir(path):
        return await asyncio.to_thread(lambda p=path: list(os.scandir(p)) if os.path.isdir(p) else [])

    async def load_json(path, force_status=None):
        async with aiofiles.open(path, "r") as f:
            data = json.loads(await f.read())
        if force_status:
            data["status"] = force_status
        return data

    wants = lambda s: status is None or status == s
    out = []

    # queued / active / failed ----------------------------------------------
    for st, base in (("queued", QUEUED_DIR), ("active", ACTIVE_DIR), ("failed", FAILED_DIR)):
        if not wants(st):
            continue

        # legacy root-level files
        for de in await scandir(base):
            if de.name.endswith(".json") and not de.is_dir():
                out.append(await load_json(de.path, force_status=(st if base==FAILED_DIR else None)))

        # job-type subdirs
        for sub in await listdir(base):
            sp = os.path.join(base, sub)
            if not os.path.isdir(sp):
                continue
            if job_type and sanitize_job_type(job_type) != sanitize_job_type(sub):
                continue
            for de in await scandir(sp):
                if de.name.endswith(".json"):
                    out.append(await load_json(de.path, force_status=(st if base==FAILED_DIR else None)))

    # completed – newest limited --------------------------------------------
    if wants("completed") and limit > 0:
        heap = []  # (mtime,path)
        async def consider(p):
            m = (await aiofiles.os.stat(p)).st_mtime
            if len(heap) < limit*2:
                heapq.heappush(heap,(m,p))
            else:
                heapq.heappushpop(heap,(m,p))

        for de in await scandir(COMPLETED_DIR):
            if de.name.endswith(".json") and not de.is_dir():
                await consider(de.path)
        for sub in await listdir(COMPLETED_DIR):
            sp = os.path.join(COMPLETED_DIR,sub)
            if not os.path.isdir(sp):
                continue
            if job_type and sanitize_job_type(job_type)!=sanitize_job_type(sub):
                continue
            for de in await scandir(sp):
                if de.name.endswith(".json"):
                    await consider(de.path)
        for _,p in sorted(heap,reverse=True)[:limit]:
            out.append(await load_json(p, force_status="completed"))

    # username filter --------------------------------------------------------
    if username:
        out = [j for j in out if j.get("username")==username]

    # Debug line
    try:
        print(f"[get_jobs] status={status or 'ALL'} jt={job_type or 'ALL'} user={username or 'ANY'} -> {len(out)} jobs")
    except Exception:
        pass

    # Sort all collected jobs by creation date descending to get the most recent first
    out.sort(key=lambda j: j.get("created_at", ""), reverse=True)

    # If no specific status was requested, apply the overall limit to the sorted list.
    # Otherwise, return all jobs for the requested status (completed is already limited).
    if status is None:
        return out[:limit]
    
    return out

# ---------------------------------------------------------------------------
# cancel_job (no job index update needed since we scan directories)
# ---------------------------------------------------------------------------
@command()
async def cancel_job(job_id, context=None):
    jd = await get_job_data(job_id)
    if not jd or jd.get("status")!="queued":
        return {"error":"Job not found or not queued"}
    ojt = jd.get("job_type", DEFAULT_JOB_TYPE)
    sjt = sanitize_job_type(ojt)
    qpath = f"{QUEUED_DIR}/{sjt}/{job_id}.json"
    if not await aiofiles.os.path.exists(qpath):
        return {"error":"Job file missing"}

    jd.update({
        "status":"failed",
        "error":"Job cancelled by user",
        "updated_at":datetime.now().isoformat(),
        "completed_at":datetime.now().isoformat()
    })
    fpath = f"{FAILED_DIR}/{job_id}.json"
    async with aiofiles.open(fpath,"w") as f:
        await f.write(json.dumps(jd,indent=2))
    await aiofiles.os.remove(qpath)
    # No need to update job index since we scan directories
    return {"success":True}

# ---------------------------------------------------------------------------
# cleanup_jobs (simple)
# ---------------------------------------------------------------------------
@command()
async def cleanup_jobs(status="completed", older_than_days:int=30, context=None):
    if status not in ("completed","failed"):
        return {"error":"status must be completed|failed"}
    base = COMPLETED_DIR if status=="completed" else FAILED_DIR
    cutoff = datetime.now().timestamp() - older_than_days*24*3600
    removed=0
    for root,_,files in await asyncio.to_thread(lambda: list(os.walk(base))):
        for fn in files:
            if not fn.endswith(".json"): continue
            p=os.path.join(root,fn)
            try:
                st=await aiofiles.os.stat(p)
                if st.st_mtime<cutoff:
                    await aiofiles.os.remove(p); removed+=1
            except FileNotFoundError:
                continue
    return {"removed_count":removed}