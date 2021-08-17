from PIL import Image 
import urllib.request
import io
import requests
import cv2

url = 'https://storage.cloud.google.com/sushant-julo-test-blucket/images/image1.png'
filename = url.split('/')[-1]

print(filename)

# im = Image.open(r"/home/sushantnigudkar/Desktop/fyusion/image1.png") 

img = cv2.imread("/home/sushantnigudkar/Desktop/fyusion/image1.png")
crop_img = img[70:170, 440:540]
cv2.imshow("cropped", crop_img)
cv2.waitKey(0)


# im1.save("/home/sushantnigudkar/Desktop/fyusion/image_save.png")
