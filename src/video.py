import cv2
import time
import numpy as np
import io
import urllib.request
from os import path, mkdir, system
from datetime import timedelta
from storage import client


def urlToImage(url):
    resp = urllib.request.urlopen(url)
    image = np.asarray(bytearray(resp.read()), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)

    return image


def processImage(job_id, frame_number):
    path = f"/{job_id}/{frame_number}.jpg"
    url_temp = client.get_presigned_url(
        "GET",
        "input",
        path,
        expires=timedelta(hours=2),
    )
    img = urlToImage(url_temp)
    grayImage = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    image_bytes = cv2.imencode('.jpg', grayImage)[1].tobytes()
    image_buffer = io.BytesIO(image_bytes)
    client.put_object("output", path, image_buffer, len(image_bytes))

    return True


def videoToFrames(video_path):
    vidcap = cv2.VideoCapture(video_path)
    success, image = vidcap.read()
    count = 0
    job_id = time.time()
    folder_path = f"/{job_id}"

    while success:
        frame_path = f"{folder_path}/{count}.jpg"
        image_bytes = cv2.imencode('.jpg', image)[1].tobytes()
        image_buffer = io.BytesIO(image_bytes)

        client.put_object("input", frame_path, image_buffer, len(image_bytes))
        processImage(job_id, count)
        success, image = vidcap.read()
        count += 1
    return job_id, count


def framesToVideo(job_id):
    objects = client.list_objects("input", f"/{job_id}/", recursive=True)
    tmp_path = f"/tmp/{job_id}"
    if not path.exists(tmp_path):
        mkdir(tmp_path)
    count = 0
    for obj in objects:
        url_temp = client.get_presigned_url(
            "GET",
            "output",
            f"/{job_id}/{count}.jpg",
            expires=timedelta(hours=2),
        )
        image = urlToImage(url_temp)
        cv2.imwrite(f"{tmp_path}/frame-{count}.jpg", image)
        count += 1

    system(
        f"cd {tmp_path} && ffmpeg -r 25 -i frame-%01d.jpg -c:v libx264 -r 30 -pix_fmt yuv420p result.mp4")
    client.fput_object("result", f"{job_id}.mp4", f"{tmp_path}/result.mp4")
    return True, tmp_path


# videoToFrames(
#    '/desarrollo/development/git/uaq/proyecto-final/media/video_example.mp4')
# framesToVideo('1637569490.6515784')
