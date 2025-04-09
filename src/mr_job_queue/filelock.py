START_RAW
import aiofiles
import aiofiles.os
import fcntl

# File locking helper
class FileLock:
    def __init__(self, file_path):
        self.file_path = file_path
        self.lock_file = None
    
    async def __aenter__(self):
        # Create lock file if it doesn't exist
        lock_path = f"{self.file_path}.lock"
        self.lock_file = await aiofiles.open(lock_path, "w")
        await self.lock_file.flush()
        
        # Acquire lock (blocking)
        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file:
            # Release lock
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            await self.lock_file.close()
            # Remove lock file
            try:
                await aiofiles.os.remove(f"{self.file_path}.lock")
            except:
                pass
END_RAW