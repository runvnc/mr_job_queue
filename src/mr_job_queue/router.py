from fastapi import APIRouter, Request, Depends, HTTPException, File, UploadFile, Form
from fastapi.responses import HTMLResponse, JSONResponse
from lib.templates import render
from lib.auth.auth import require_user
import os
import aiofiles
import aiofiles.os
import json
import traceback
from typing import List, Optional
from .commands import (
    get_job_status, get_jobs, cancel_job, cleanup_jobs,
    QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, FAILED_DIR
, DEFAULT_JOB_TYPE)
from .helpers import sanitize_job_type
# Assuming main still provides add_job service
from .main import add_job

from lib.chatlog import count_tokens_for_log_id
router = APIRouter()

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
        # Get context if available, otherwise pass None
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
        
        # Call get_jobs with appropriate parameters
        # Admins see all jobs, others see only their own
        uname_filter = None if 'admin' in getattr(user, 'roles', []) else user.username

        jobs = await get_jobs(status=status, job_type=job_type, username=uname_filter, limit=limit, context=context)
        return JSONResponse(jobs)
    except Exception as e:
        print(e)
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/jobs/{job_id}")
async def get_job(request: Request, job_id: str, user=Depends(require_user)):
    """Get details for a specific job"""
    try:
        # Get context if available, otherwise pass None
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
async def create_job(
    request: Request,
    instructions: str = Form(...),
    agent_name: str = Form(...),
    metadata: Optional[str] = Form(None),
    job_type: Optional[str] = Form(None),
    files: List[UploadFile] = File([]),
    user=Depends(require_user)
):
    """Submit a new job with optional file uploads"""
    try:
        # Get context if available, otherwise pass None
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
        
        # Parse metadata if provided
        metadata_dict = None
        if metadata:
            try:
                metadata_dict = json.loads(metadata)
            except json.JSONDecodeError:
                return JSONResponse({"error": "Invalid metadata format"}, status_code=400)
        
        # Handle file uploads if any
        uploaded_files = []
        if files:
            # Create a directory for uploaded files if it doesn't exist
            upload_dir = os.path.join("data", "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            
            for file in files:
                # Save the file
                file_path = os.path.join(upload_dir, file.filename)
                content = await file.read()
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(content)
                uploaded_files.append(file_path)
        
        # Add uploaded files to metadata
        if uploaded_files:
            if not metadata_dict:
                metadata_dict = {}
            metadata_dict["uploaded_files"] = uploaded_files
        
        # Submit the job
        result = await add_job(
            instructions=instructions,
            agent_name=agent_name,
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

@router.delete("/api/jobs/{job_id}")
async def delete_job(request: Request, job_id: str, user=Depends(require_user)):
    """Cancel a job"""
    try:
        # Get context if available, otherwise pass None
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
    # Require admin role
    if "admin" not in user.roles:
        return JSONResponse({"error": "Admin access required"}, status_code=403)
    
    try:
        data = await request.json()
        status = data.get("status", "completed")
        older_than_days = data.get("older_than_days", 30)
        
        # Get context if available, otherwise pass None
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
        # Initialize counts for each status
        for status in ["queued", "active", "completed", "failed"]:
            counts[status] = 0
            
        for status_name, status_dir in [
            ("queued", QUEUED_DIR),
            ("active", ACTIVE_DIR),
            ("completed", COMPLETED_DIR),
            ("failed", FAILED_DIR)
        ]:
            try:
                # Ensure directory exists
                if not await aiofiles.os.path.isdir(status_dir):
                    await aiofiles.os.makedirs(status_dir, exist_ok=True)
                    
                # Count files in the base directory (legacy jobs)
                files = await aiofiles.os.listdir(status_dir)
                base_count = len([f for f in files if f.endswith('.json')])
                counts[status_name] += base_count
                
                # Count files in job type subdirectories
                for item in files:
                    item_path = os.path.join(status_dir, item)
                    if await aiofiles.os.path.isdir(item_path):
                        try:
                            # This is a job type directory
                            type_files = await aiofiles.os.listdir(item_path)
                            type_count = len([f for f in type_files if f.endswith('.json')])
                            counts[status_name] += type_count
                            
                            # Optionally, track counts per job type
                            original_job_type = item
                            job_type = sanitize_job_type(original_job_type)
                            type_key = f"{status_name}_{job_type}"
                            counts[type_key] = type_count
                        except Exception as e:
                            print(f"Error counting files in {item_path}: {e}")
                            # Continue with other directories
                            continue
                
            except FileNotFoundError:
                counts[status_name] = 0 # Should not happen if makedirs worked
            except Exception as e:
                print(f"Error getting stats for {status_name}: {e}")
                # Keep the count at 0 and continue
        
        stats = {
            "queued": counts.get("queued", 0),
            "active": counts.get("active", 0),
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "total": sum(counts.values())
            
            # Include job type specific counts if available
            # This will add entries like queued_default, active_default, etc.
        }
        
        # Add job type specific counts to stats
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
        # Get context if available, otherwise pass None
        context = None
        if hasattr(request.state, 'context'):
            context = request.state.context
            
        # Get the job to verify it exists and get the log_id
        job = await get_job_status(job_id, context=context)
        if "error" in job and job['error'] is not None:
            return JSONResponse({"error": job["error"]}, status_code=404)
        
        # Use the job_id as the log_id for token counting
        token_counts = await count_tokens_for_log_id(job_id, user=user.username, hierarchical=True)
        
        if token_counts is None:
            return JSONResponse({"error": f"No token data found for job {job_id}"}, status_code=404)
        
        return JSONResponse({"status": "ok", "hierarchy": token_counts.get("hierarchy")})
    except Exception as e:
        print(f"Error getting token counts for job {job_id}: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)