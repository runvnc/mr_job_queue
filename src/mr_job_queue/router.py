from fastapi import APIRouter, Request, Depends, HTTPException, File, UploadFile, Form, Header
from datetime import datetime
import asyncio
import time
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import io
from lib.templates import render
from lib.auth.auth import require_user
import os
import aiofiles
import aiofiles.os
from lib.providers.hooks import hook_manager
import nanoid
import json
import traceback
from typing import List, Optional
from .commands import (
    get_job_status, get_jobs, cancel_job, cleanup_jobs,
    QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, FAILED_DIR
, DEFAULT_JOB_TYPE, JOB_DIR)
from .helpers import sanitize_job_type
from .main import load_config, get_limits_for_type, count_active_jobs_for_type
from .main import add_job

# Import worker tracking and queue control functions
from .main import update_worker_registry, load_workers_registry
from .main import is_queue_paused, set_queue_paused

# Import ChatLog for sync endpoint
from lib.chatlog import ChatLog

router = APIRouter()

def get_client_ip(request: Request) -> str:
    """Extract client IP, handling reverse proxies."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip
    return request.client.host if request.client else "unknown"

async def require_admin(user=Depends(require_user)):
    if "admin" not in getattr(user, 'roles', []):
        raise HTTPException(status_code=403, detail="Admin access required")

@router.get("/api/config")
async def get_job_config(_=Depends(require_admin)):
    from .main import load_config
    return load_config()

@router.post("/api/config")
async def update_job_config(request: Request, _=Depends(require_admin)):
    from .main import save_config
    config = await request.json()
    save_config(config)
    return {"status": "ok"}

@router.get("/api/workers")
async def get_workers(_=Depends(require_admin)):
    """Get the current workers registry"""
    registry = load_workers_registry()
    return JSONResponse(registry)

@router.get("/api/queue/status")
async def get_queue_status(_=Depends(require_user)):
    """Get the current queue paused status"""
    return JSONResponse({"paused": is_queue_paused()})

@router.post("/api/queue/pause")
async def toggle_queue_pause(request: Request, _=Depends(require_admin)):
    """Toggle or set the queue paused state"""
    data = await request.json()
    paused = data.get("paused", not is_queue_paused())  # Toggle if not specified
    set_queue_paused(paused)
    return JSONResponse({"paused": paused, "status": "ok"})

@router.post("/api/jobs/lease")
async def lease_job(request: Request, user=Depends(require_user)):
    """Allow a worker to lease a job from the master"""
    config = load_config()
    data = await request.json()
    requested_type = data.get("job_type")
    worker_id = data.get("worker_id")
    client_ip = get_client_ip(request)
    requested_timeout = data.get("timeout", 15)
    timeout = min(max(requested_timeout, 1), 55)
    
    print(f"[MASTER DEBUG] Lease request from worker_id={worker_id}, ip={client_ip}, job_type={requested_type}, timeout={timeout}")
    
    if not worker_id:
        return JSONResponse({"error": "worker_id required"}, status_code=400)
    
    if is_queue_paused():
        print(f"[MASTER DEBUG] Queue is paused, returning 204")
        return JSONResponse({"status": "paused"}, status_code=204)
    
    update_worker_registry(worker_id, ip=client_ip)
    
    if requested_type:
        target_types = []
        if os.path.exists(QUEUED_DIR):
            for d in os.listdir(QUEUED_DIR):
                if os.path.isdir(os.path.join(QUEUED_DIR, d)):
                    if d == requested_type or d.startswith(requested_type + "."):
                        target_types.append(d)
        print(f"[MASTER DEBUG] Requested type '{requested_type}' matched dirs: {target_types}", flush=True)
    else:
        target_types = [d for d in os.listdir(QUEUED_DIR) if os.path.isdir(os.path.join(QUEUED_DIR, d))] if os.path.exists(QUEUED_DIR) else []
    
    print(f"[MASTER DEBUG] Checking job types: {target_types}")

    if not target_types:
        print(f"[MASTER DEBUG] No matching job type directories found, waiting {timeout}s...", flush=True)
        await asyncio.sleep(timeout)
        print(f"[MASTER DEBUG] No jobs found after {timeout}s, returning 204")
        return JSONResponse({"status": "empty"}, status_code=204)

    start_time = time.time()
    poll_interval = 2
    
    while time.time() - start_time < timeout:
        for sjt in target_types:
            qdir = os.path.join(QUEUED_DIR, sjt)
            if not os.path.exists(qdir): continue
            
            limits = get_limits_for_type(sjt, config)
            active_count = count_active_jobs_for_type(sjt)
            if active_count >= limits["max_global"]:
                print(f"[MASTER DEBUG] Global limit reached for {sjt}: {active_count}/{limits['max_global']}")
                continue
            
            try:
                files = sorted(os.listdir(qdir))
            except OSError:
                continue
                
            for f in files:
                if not f.endswith(".json"): continue
                
                job_id = f.replace(".json", "")
                old_path = os.path.join(qdir, f)
                new_dir = os.path.join(ACTIVE_DIR, sjt)
                os.makedirs(new_dir, exist_ok=True)
                new_path = os.path.join(new_dir, f)
                
                try:
                    os.rename(old_path, new_path)
                    
                    async with aiofiles.open(new_path, "r") as jf:
                        job_data = json.loads(await jf.read())
                    
                    job_data["assigned_worker"] = worker_id
                    job_data["assigned_worker_ip"] = client_ip
                    job_data["assigned_at"] = datetime.now().isoformat()
                    
                    print(f"[MASTER DEBUG] Leasing job {job_id} to worker {worker_id}")
                    async with aiofiles.open(new_path, "w") as jf:
                        await jf.write(json.dumps(job_data, indent=2))
                    
                    update_worker_registry(worker_id, ip=client_ip, job_id=job_id)
                    
                    return JSONResponse(job_data)
                except FileNotFoundError:
                    continue
                except Exception as e:
                    print(f"Lease error: {e}")
                    continue
        
        await asyncio.sleep(poll_interval)
                
    print(f"[MASTER DEBUG] No jobs found after {timeout}s, returning 204")
    return JSONResponse({"status": "empty"}, status_code=204)

@router.post("/api/jobs/report/{job_id}")
async def report_job(job_id: str, request: Request, user=Depends(require_user)):
    """Worker reporting job result back to master"""
    report = await request.json()
    status = report.get("status")
    sjt = sanitize_job_type(report.get("job_type", DEFAULT_JOB_TYPE))
    worker_id = report.get("reporting_worker_id")
    client_ip = get_client_ip(request)
    
    active_path = os.path.join(ACTIVE_DIR, sjt, f"{job_id}.json")
    if not os.path.exists(active_path):
        return JSONResponse({"error": "Job not found in active queue"}, status_code=404)
        
    async with aiofiles.open(active_path, "r") as f:
        job_data = json.loads(await f.read())
        
    assigned_worker = job_data.get("assigned_worker")
    if assigned_worker and worker_id and assigned_worker != worker_id:
        print(f"Warning: Job {job_id} assigned to {assigned_worker} but reported by {worker_id}")
    
    if worker_id:
        update_worker_registry(worker_id, ip=client_ip, job_id=job_id, remove_job=True)
    
    report.pop("reporting_worker_id", None)
    
    job_data.update(report)
    job_data["updated_at"] = datetime.now().isoformat()
    
    if status == "completed":
        target_dir = COMPLETED_DIR
    elif status == "paused":
        target_dir = os.path.join(JOB_DIR, "paused")
    else:
        target_dir = FAILED_DIR
    os.makedirs(os.path.join(target_dir, sjt), exist_ok=True)
    final_path = os.path.join(target_dir, sjt, f"{job_id}.json")
    
    async with aiofiles.open(final_path, "w") as f:
        await f.write(json.dumps(job_data, indent=2))
    
    os.remove(active_path)
    
    await hook_manager.job_ended(status, job_data, job_data.get("result"), context=None)
    return {"status": "ok"}

@router.post("/api/chatlog/sync")
async def sync_chatlog(request: Request, user=Depends(require_user)):
    """Receive chat log updates from workers."""
    try:
        print("[CHATLOG SYNC] Received sync request")
        data = await request.json()
        log_id = data.get("log_id")
        username = data.get("user")
        agent = data.get("agent")
        message = data.get("message")
        parent_log_id = data.get("parent_log_id")
        
        if not all([log_id, username, agent, message]):
            return JSONResponse(
                {"error": "Missing required fields: log_id, user, agent, message"},
                status_code=400
            )
        
        try:
            chatlog = ChatLog(
                log_id=log_id,
                user=username,
                agent=agent,
                parent_log_id=parent_log_id
            )
            
            chatlog._add_message_impl(message)
            chatlog._save_log_sync()
            
            return JSONResponse({"status": "ok"})
            
        except Exception as e:
            print(f"[CHATLOG SYNC] Error updating chatlog {log_id}: {e}")
            return JSONResponse({"error": str(e)}, status_code=500)
            
    except Exception as e:
        print(f"[CHATLOG SYNC] Error processing sync request: {e}")
        print(traceback.format_exc())
        return JSONResponse({"error": str(e)}, status_code=500)

from lib.chatlog import count_tokens_for_log_id

@router.get("/jobs", include_in_schema=False)
async def index(request: Request, user=Depends(require_user)):
    """Job queue dashboard page"""
    try:
        html = await render("dashboard", {"request": request, "user": user})
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<h1>Error</h1><p>{str(e)}</p>")

@router.get("/api/jobs")
async def list_jobs(request: Request, status: str = None, job_type: str = None, limit: int = 50, user=Depends(require_user)):
    """Get a list of jobs with optional filtering"""
    try:
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
        
        uname_filter = None if 'admin' in getattr(user, 'roles', []) else user.username

        jobs = await get_jobs(status=status, job_type=job_type, username=uname_filter, limit=limit, context=context)
        return JSONResponse(jobs)
    except Exception as e:
        print(e)
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/jobs/search")
async def search_jobs_endpoint(
    request: Request,
    metadata_query: Optional[str] = None,
    before_date: Optional[str] = None,
    after_date: Optional[str] = None,
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    user=Depends(require_user)
):
    """Search jobs with metadata filtering and date range"""
    try:
        query_params = dict(request.query_params)
        known_params = {'api_key', 'metadata_query', 'before_date', 'after_date', 'username', 'output',
                       'status', 'job_type', 'limit', 'offset'}
        
        metadata_dict = {}
        for key, value in query_params.items():
            if key not in known_params:
                metadata_dict[key] = value
        
        if metadata_query:
            try:
                parsed_metadata = json.loads(metadata_query)
                for key, value in parsed_metadata.items():
                    if key not in known_params:
                        metadata_dict[key] = value
            except json.JSONDecodeError:
                return JSONResponse({"error": "Invalid metadata_query format"}, status_code=400)
        
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
        username_filter = None if 'admin' in getattr(user, 'roles', []) else user.username
        
        from .commands import search_jobs
        
        result = await search_jobs(
            metadata_query=metadata_dict,
            before_date=before_date,
            after_date=after_date,
            status=status,
            job_type=job_type,
            username=username_filter,
            limit=limit,
            offset=offset,
            context=context
        )
        
        if query_params.get('output') == 'results':
            simplified = []
            for job in result['jobs']:
                simplified.append([job.get('created_at', ''), job.get('id', ''), job.get('instructions', ''), job.get('status', ''), job.get('result', '')])
            return JSONResponse(simplified)
        
        if query_params.get('output') == 'csv':
            import csv
            output = io.StringIO()
            writer = csv.writer(output)
            
            writer.writerow(['Created At', 'Job ID', 'Instructions', 'Status', 'Result'])
            
            for job in result['jobs']:
                writer.writerow([
                    job.get('created_at', ''),
                    job.get('id', ''),
                    job.get('instructions', ''),
                    job.get('status', ''),
                    job.get('result', '')
                ])
            
            output.seek(0)
            return StreamingResponse(
                io.BytesIO(output.getvalue().encode('utf-8')),
                media_type='text/csv',
                headers={'Content-Disposition': 'attachment; filename="job_results.csv"'}
            )
        
        return JSONResponse(result)
    except Exception as e:
        print(f"Error in job search: {e}")
        print(traceback.format_exc())
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/jobs/{job_id}")
async def get_job(request: Request, job_id: str, user=Depends(require_user)):
    """Get details for a specific job"""
    try:
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
            
        job = await get_job_status(job_id, context=context)
        print("job result", job)
        if "error" in job and job['error'] is not None:
            return JSONResponse({"error": job["error"]}, status_code=404)
        return JSONResponse(job)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/jobs")
async def create_job_form(
    request: Request,
    instructions: str = Form(...),
    agent: str = Form(...),
    metadata: Optional[str] = Form(None),
    job_type: Optional[str] = Form(None),
    files: List[UploadFile] = File([]),
    user=Depends(require_user)
):
    """Submit a new job via form data (for web UI with file uploads)."""
    try:
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
        
        metadata_dict = None
        if metadata:
            try:
                metadata_dict = json.loads(metadata)
            except json.JSONDecodeError:
                return JSONResponse({"error": "Invalid metadata format"}, status_code=400)
        
        uploaded_files = []
        if files:
            upload_dir = os.path.join("data", "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            
            for file in files:
                file_path = os.path.join(upload_dir, file.filename)
                content = await file.read()
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(content)
                uploaded_files.append(file_path)
        
        if uploaded_files:
            if not metadata_dict:
                metadata_dict = {}
            metadata_dict["uploaded_files"] = uploaded_files
        
        result = await add_job(
            instructions=instructions,
            agent_name=agent,
            job_type=job_type,
            metadata=metadata_dict,
            username=user.username,
            context=context
        )
        
        if "error" in result:
            return JSONResponse({"error": result["error"]}, status_code=500)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/jobs/json")
async def create_job_json(
    request: Request,
    user=Depends(require_user)
):
    """Submit a new job via JSON payload (for workers and API clients)."""
    try:
        data = await request.json()
        
        instructions = data.get('instructions')
        agent_name = data.get('agent_name')
        job_type = data.get('job_type')
        metadata = data.get('metadata')
        job_id = data.get('job_id')
        llm = data.get('llm')
        username = data.get('username') or getattr(user, 'username', 'worker')
        
        if not instructions or not agent_name:
            return JSONResponse({"error": "instructions and agent_name are required"}, status_code=400)
        
        result = await add_job(
            instructions=instructions,
            agent_name=agent_name,
            job_type=job_type,
            metadata=metadata,
            job_id=job_id,
            llm=llm,
            username=username,
            context=None
        )
        
        if "error" in result:
            return JSONResponse({"error": result["error"]}, status_code=500)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/jobs/{job_id}")
async def delete_job(request: Request, job_id: str, user=Depends(require_user)):
    """Cancel a job"""
    try:
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
            
        result = await cancel_job(job_id, context=context)
        if "error" in result:
            return JSONResponse({"error": result["error"]}, status_code=404)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/cleanup")
async def cleanup(request: Request, user=Depends(require_user)):
    """Clean up old jobs"""
    if "admin" not in user.roles:
        return JSONResponse({"error": "Admin access required"}, status_code=403)
    
    try:
        data = await request.json()
        status = data.get("status", "completed")
        older_than_days = data.get("older_than_days", 30)
        
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
            
        result = await cleanup_jobs(
            status=status,
            older_than_days=older_than_days,
            context=context
        )
        
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/stats")
async def get_stats(request: Request, user=Depends(require_user)):
    """Get job queue statistics"""
    try:
        counts = {}
        for status in ["queued", "active", "completed", "failed"]:
            counts[status] = 0
            
        for status_name, status_dir in [
            ("queued", QUEUED_DIR),
            ("active", ACTIVE_DIR),
            ("completed", COMPLETED_DIR),
            ("failed", FAILED_DIR)
        ]:
            try:
                if not await aiofiles.os.path.isdir(status_dir):
                    await aiofiles.os.makedirs(status_dir, exist_ok=True)
                    
                files = await aiofiles.os.listdir(status_dir)
                base_count = len([f for f in files if f.endswith('.json')])
                counts[status_name] += base_count
                
                for item in files:
                    item_path = os.path.join(status_dir, item)
                    if await aiofiles.os.path.isdir(item_path):
                        try:
                            type_files = await aiofiles.os.listdir(item_path)
                            type_count = len([f for f in type_files if f.endswith('.json')])
                            counts[status_name] += type_count
                            
                            original_job_type = item
                            job_type = sanitize_job_type(original_job_type)
                            type_key = f"{status_name}_{job_type}"
                            counts[type_key] = type_count
                        except Exception as e:
                            print(f"Error counting files in {item_path}: {e}")
                            continue
                
            except FileNotFoundError:
                counts[status_name] = 0
            except Exception as e:
                print(f"Error getting stats for {status_name}: {e}")
        
        stats = {
            "queued": counts.get("queued", 0),
            "active": counts.get("active", 0),
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "total": sum(counts.values())
        }
        
        for key, value in counts.items():
            if key not in ["queued", "active", "completed", "failed"]:
                stats[key] = value
                
        return JSONResponse(stats)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/jobs/{job_id}/tokens")
async def get_job_token_counts(request: Request, job_id: str, user=Depends(require_user)):
    """Get token counts for a specific job including delegated tasks"""
    try:
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
            
        job = await get_job_status(job_id, context=context)
        if "error" in job and job['error'] is not None:
            return JSONResponse({"error": job["error"]}, status_code=404)
        
        token_counts = await count_tokens_for_log_id(job_id, user=user.username, hierarchical=True)
        
        if token_counts is None:
            return JSONResponse({"error": f"No token data found for job {job_id}"}, status_code=404)
        
        return JSONResponse({"status": "ok", "hierarchy": token_counts.get("hierarchy")})
    except Exception as e:
        print(f"Error getting token counts for job {job_id}: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/jobs/bulk")
async def create_bulk_jobs(
    request: Request,
    instructions: List[str] = Form(None),
    instructions_file: Optional[UploadFile] = File(None),
    instructions_csv: Optional[str] = Form(None),
    agent: str = Form(...),
    metadata: Optional[str] = Form(None),
    search_key: str = Form(...),
    job_type: Optional[str] = Form(None),
    files: List[UploadFile] = File([]),
    user=Depends(require_user)
):
    """Submit multiple jobs with the same parameters but different instructions"""
    try:
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
        
        instructions_data = None
        
        if instructions:
            instructions_data = instructions
        elif instructions_file:
            content = await instructions_file.read()
            text = content.decode('utf-8')
            instructions_data = [line.strip() for line in text.split('\n') if line.strip()]
        elif instructions_csv:
            instructions_data = [instr.strip() for instr in instructions_csv.split(',') if instr.strip()]
        else:
            return JSONResponse({"error": "No instructions provided. Use instructions, instructions_file, or instructions_csv"}, status_code=400)
        
        if not isinstance(instructions_data, list):
            return JSONResponse({"error": "Instructions must be a list"}, status_code=400)
        
        if len(instructions_data) == 0:
            return JSONResponse({"error": "Instructions list cannot be empty"}, status_code=400)
        
        for i, instr in enumerate(instructions_data):
            if not isinstance(instr, str):
                return JSONResponse({"error": f"Instruction at index {i} must be a string"}, status_code=400)
            if not instr.strip():
                return JSONResponse({"error": f"Instruction at index {i} cannot be empty"}, status_code=400)
        
        metadata_dict = None
        if metadata:
            try:
                metadata_dict = json.loads(metadata)
            except json.JSONDecodeError:
                return JSONResponse({"error": "Invalid metadata format"}, status_code=400)
        
        uploaded_files = []
        if files:
            upload_dir = os.path.join("data", "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            
            for file in files:
                file_path = os.path.join(upload_dir, file.filename)
                content = await file.read()
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(content)
                uploaded_files.append(file_path)
        
        if uploaded_files:
            if not metadata_dict:
                metadata_dict = {}
            metadata_dict["uploaded_files"] = uploaded_files
        
        results = []
        for i, instructions in enumerate(instructions_data):
            if not isinstance(instructions, str):
                results.append({"index": i, "error": "Instructions must be a string"})
                continue
            
            job_id = nanoid.generate()
            job_metadata = dict(metadata_dict) if metadata_dict else {}
            job_metadata['bulk_job_id'] = job_id
            job_metadata['bulk_index'] = i
            job_metadata['search_key'] = search_key

            result = await add_job(
                instructions=instructions,
                agent_name=agent,
                job_id=job_id,
                job_type=job_type,
                metadata=job_metadata,
                username=user.username,
                context=context
            )
            
            if "error" in result:
                results.append({"index": i, "error": result["error"]})
            else:
                results.append({"index": i, "job_id": result.get("job_id"), "status": "queued"})
        
        return JSONResponse({
            "success": True,
            "submitted_count": len([r for r in results if "job_id" in r]),
            "failed_count": len([r for r in results if "error" in r]),
            "results": results
        })
    except Exception as e:
        print(f"Error in bulk job submission: {e}")
        print(traceback.format_exc())
        return JSONResponse({"error": str(e)}, status_code=500)
