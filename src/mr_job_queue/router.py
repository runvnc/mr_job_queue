from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from lib.templates import render
from lib.auth.auth import require_user
import os
import json
from .mod import (
    submit_job, get_job_status, get_jobs, cancel_job, cleanup_jobs,
    QUEUED_DIR, ACTIVE_DIR, COMPLETED_DIR, FAILED_DIR
)

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
        jobs = await get_jobs(status=status, job_type=job_type, limit=limit, context=request.state.context)
        return JSONResponse(jobs)
    except Exception as e:
        print(e)
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/jobs/{job_id}")
async def get_job(request: Request, job_id: str, user=Depends(require_user)):
    """Get details for a specific job"""
    try:
        job = await get_job_status(job_id, context=request.state.context)
        if "error" in job:
            return JSONResponse({"error": job["error"]}, status_code=404)
        return JSONResponse(job)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/jobs")
async def create_job(request: Request, user=Depends(require_user)):
    """Submit a new job"""
    try:
        data = await request.json()
        if not all(k in data for k in ["instructions", "agent_name"]):
            return JSONResponse({"error": "Missing required fields"}, status_code=400)
        
        result = await submit_job(
            instructions=data["instructions"],
            agent_name=data["agent_name"],
            job_type=data.get("job_type"),
            metadata=data.get("metadata"),
            context=request.state.context
        )
        
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/jobs/{job_id}")
async def delete_job(request: Request, job_id: str, user=Depends(require_user)):
    """Cancel a job"""
    try:
        result = await cancel_job(job_id, context=request.state.context)
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
        
        result = await cleanup_jobs(
            status=status,
            older_than_days=older_than_days,
            context=request.state.context
        )
        
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/stats")
async def get_stats(request: Request, user=Depends(require_user)):
    """Get job queue statistics"""
    try:
        # Count jobs in each directory
        queued_count = len([f for f in os.listdir(QUEUED_DIR) if f.endswith('.json')])
        active_count = len([f for f in os.listdir(ACTIVE_DIR) if f.endswith('.json')])
        completed_count = len([f for f in os.listdir(COMPLETED_DIR) if f.endswith('.json')])
        failed_count = len([f for f in os.listdir(FAILED_DIR) if f.endswith('.json')])
        
        stats = {
            "queued": queued_count,
            "active": active_count,
            "completed": completed_count,
            "failed": failed_count,
            "total": queued_count + active_count + completed_count + failed_count
        }
        
        return JSONResponse(stats)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
