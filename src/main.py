import redis
import os
from rq import Worker, Queue, Connection
from video import processImage, videoToFrames, framesToVideo
from storage import initWorkspace, countItemsPath
# Env
REDIS_SERVER = os.getenv('REDIS_SERVER', "redis://localhost:6379")
SERVER_TYPE = os.getenv('SERVER_TYPE', "client")  # Client, master, slave

conn_redis = redis.from_url(REDIS_SERVER)


async def background_work():
    print(2)
    return True


q = Queue(connection=conn_redis, default_timeout=7200)

if __name__ == '__main__':
    initWorkspace()
    if SERVER_TYPE == 'slave':
        print("SERVER MODE: slave")
        with Connection(conn_redis):
            listen = ['default']
            worker = Worker(list(map(Queue, listen)))
            worker.work()
    elif SERVER_TYPE == 'master':
        print("SERVER MODE: master")
        path = '/desarrollo/development/git/uaq/proyecto-final/media/video_example.mp4'
        job_id, total = videoToFrames(path)
        for i in range(total):
            job = q.enqueue_call(func=processImage, args=(job_id, i))
        success = False
        while success:
            progress = countItemsPath('output', f"/{job_id}/")
            if progress == total:
                success = True
                break
            print(progress)
        result = framesToVideo(job_id)
        if result:
            print(f"Video Processed with job_id {job_id}")

    elif SERVER_TYPE == 'client':
        print('SERVER MODE: client')
