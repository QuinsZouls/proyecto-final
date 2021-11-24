import redis
import os
import socket
import sys
from rq import Worker, Queue, Connection
from video import processImage, videoToFrames, framesToVideo
from storage import initWorkspace, countItemsPath
# Env
REDIS_SERVER = os.getenv('REDIS_SERVER', "redis://localhost:6379")
SERVER_TYPE = os.getenv('SERVER_TYPE', "client")  # Client, master, slave
SERVER_SK_PORT = int(os.getenv('SERVER_SK_PORT', 6791))
CLIENT_SK_PORT = int(os.getenv('SERVER_SK_PORT', 6793))
SERVER_URL = os.getenv('SERVER_URL', "127.0.0.1")

BUFFER_SIZE = 4096
SEPARATOR = ";"

if __name__ == '__main__':
    initWorkspace()
    if SERVER_TYPE == 'slave':
        conn_redis = redis.from_url(REDIS_SERVER)
        q = Queue(connection=conn_redis, default_timeout=7200)
        print("SERVER MODE: slave")
        with Connection(conn_redis):
            listen = ['default']
            worker = Worker(list(map(Queue, listen)))
            worker.work()
    elif SERVER_TYPE == 'master':
        print("SERVER MODE: master")
        # Init redis
        conn_redis = redis.from_url(REDIS_SERVER)
        q = Queue(connection=conn_redis, default_timeout=7200)
        # Init socket
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind((SERVER_URL, SERVER_SK_PORT))
        serversocket.listen(5)
        while True:
            try:
                # accept connections from outside
                (clientsocket, address) = serversocket.accept()
                received = clientsocket.recv(BUFFER_SIZE).decode()
                filename, filesize = received.split(SEPARATOR)
                # remove absolute path if there is
                path = os.path.basename(filename)
                # convert to integer
                filesize = int(filesize)
                with open(path, "wb") as f:
                    while True:
                        bytes_read = clientsocket.recv(BUFFER_SIZE)
                        if not bytes_read:
                            break
                        f.write(bytes_read)
                    f.close()
                print('Archivo recibido')
                job_id, total = videoToFrames(path)
                print(job_id)
                for i in range(total):
                    job = q.enqueue_call(func=processImage, args=(job_id, i))
                success = False
                while success:
                    progress = countItemsPath('output', f"/{job_id}/")
                    if progress == total:
                        success = True
                        break
                result, tmp_path = framesToVideo(job_id)
                if result:
                    print(f"Video Processed with job_id {job_id}")
                    s = socket.socket()
                    s.connect((SERVER_URL, CLIENT_SK_PORT))
                    filename = 'result.mp4'
                    file_path = f"{tmp_path}/{filename}"
                    filesize = os.path.getsize(file_path)
                    s.send(f"{filename}{SEPARATOR}{filesize}".encode())
                    with open(file_path, "rb") as f:
                        while True:
                            bytes_read = f.read(BUFFER_SIZE)
                            if not bytes_read:
                                break
                            s.send(bytes_read)
                        f.close()
                    s.close()
                    # Removemos todos los archivos del video
                    os.system(f"rm -rf {tmp_path}")
                    print("Done")
            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                print(e)

    elif SERVER_TYPE == 'client':
        print('SERVER MODE: client')
        s = socket.socket()
        s.connect((SERVER_URL, SERVER_SK_PORT))
        filename = 'video_example.mp4'
        file_path = f"/desarrollo/development/git/uaq/proyecto-final/media/{filename}"
        filesize = os.path.getsize(file_path)
        s.send(f"{filename}{SEPARATOR}{filesize}".encode())
        with open(file_path, "rb") as f:
            while True:
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    break
                s.send(bytes_read)
            f.close()
        s.close()
        # Iniciamos el socket para escuchar
        print("Escuchando mensajes...")
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind((SERVER_URL, CLIENT_SK_PORT))
        serversocket.listen(5)
        while True:
            (clientsocket, address) = serversocket.accept()
            received = clientsocket.recv(BUFFER_SIZE).decode()
            filename, filesize = received.split(SEPARATOR)
            # remove absolute path if there is
            path = os.path.basename(filename)
            # convert to integer
            filesize = int(filesize)
            with open(path, "wb") as f:
                while True:
                    bytes_read = clientsocket.recv(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                f.close()
            print('Archivo procesado recibido')
