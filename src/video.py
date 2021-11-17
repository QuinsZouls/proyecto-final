import cv2
vidcap = cv2.VideoCapture('video_example.mp4')
success, image = vidcap.read()
count = 0

if __name__ == '__main__':
    while success:
        cv2.imwrite("output/frame%d.jpg" %
                    count, image)     # save frame as JPEG file
        success, image = vidcap.read()
        print('Read a new frame: ', success)
        count += 1
