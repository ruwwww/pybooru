from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uuid
import threading
import queue
import time
from typing import Dict, List, Optional
from downloader import BooruDownloader
import os

app = FastAPI()

class JobRequest(BaseModel):
    tags: str
    force: bool = False

class JobManager:
    def __init__(self):
        self.queue = queue.Queue()
        self.jobs: Dict[str, dict] = {}
        self.current_job_id = None
        self.stop_event = threading.Event()
        self.downloader = BooruDownloader()
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def _worker(self):
        while True:
            job_id = self.queue.get()
            self.current_job_id = job_id
            self.stop_event.clear()
            
            job = self.jobs[job_id]
            job['status'] = 'running'
            job['started_at'] = time.time()
            
            def progress_callback(status_data):
                self.jobs[job_id].update(status_data)

            try:
                self.downloader.process_job(
                    tags=job['tags'],
                    force=job['force'],
                    progress_callback=progress_callback,
                    stop_event=self.stop_event
                )
                # If status wasn't set to cancelled/completed by callback (or if callback wasn't called enough), ensure final state
                if self.jobs[job_id]['status'] not in ['cancelled', 'completed', 'failed']:
                     self.jobs[job_id]['status'] = 'completed'
            except Exception as e:
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['error'] = str(e)
            finally:
                self.jobs[job_id]['completed_at'] = time.time()
                self.current_job_id = None
                self.queue.task_done()

    def add_job(self, tags, force=False):
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            'id': job_id,
            'tags': tags,
            'force': force,
            'status': 'pending',
            'created_at': time.time(),
            'message': 'Waiting in queue...'
        }
        self.queue.put(job_id)
        return job_id

    def get_jobs(self):
        # Return jobs sorted by creation time (newest first)
        return sorted(self.jobs.values(), key=lambda x: x['created_at'], reverse=True)

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def cancel_current_job(self):
        if self.current_job_id:
            self.stop_event.set()
            return True
        return False

job_manager = JobManager()

@app.get("/api/jobs")
def list_jobs():
    return job_manager.get_jobs()

@app.post("/api/jobs")
def create_job(job: JobRequest):
    job_id = job_manager.add_job(job.tags, job.force)
    return {"job_id": job_id, "message": "Job submitted"}

@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/api/jobs/cancel")
def cancel_job():
    if job_manager.cancel_current_job():
        return {"message": "Cancellation requested"}
    return {"message": "No running job to cancel"}

# Serve static files
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def read_root():
    return FileResponse("static/index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
