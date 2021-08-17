from PIL import Image, ImageDraw

image_path_input = '/home/sushantnigudkar/Desktop/fyusion/'
image_path_output = '/home/sushantnigudkar/Desktop/fyusion/'

image_name_input = 'PNG_transparency_demonstration_1.png'

print(image_path_input + image_name_input)

im = Image.open(image_path_input + image_name_input)

im_width, im_height = im.size
print('im.size', im.size)
