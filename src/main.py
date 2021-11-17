import redis
from rq import Queue
conn_redis = redis.from_url('redis://localhost:6379')


async def background_work():
    print(2)
    return True


q = Queue(connection=conn_redis, default_timeout=7200)

if __name__ == '__main__':
    job = q.enqueue_call(func=background_work, )
    job = q.enqueue_call(func=background_work, )
