import numpy as np
x = np.linspace(-4.0, 4.0, 240)
y = np.linspace(-3.0, 3.0, 180)
X, Y = np.meshgrid(x, y)
Z1 = np.exp(-2 * np.log(2) * ((X - 0.5) ** 2 + (Y - 0.5) ** 2) / 1 ** 2)
Z2 = np.exp(-3 * np.log(2) * ((X + 0.5) ** 2 + (Y + 0.5) ** 2) / 2.5 ** 2)
Z = 10.0 * (Z2 - Z1)

# ============================================================================================


# import numpy as np
# import matplotlib.pyplot as plt

# d = np.arange(100).reshape(10, 10)
# x, y = np.meshgrid(np.arange(11), np.arange(11))

# theta = 0.25*np.pi
# xx = x*np.cos(theta) - y*np.sin(theta)
# yy = x*np.sin(theta) + y*np.cos(theta)

# fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)
# ax1.set_aspect(1)
# ax1.pcolormesh(xx, yy, d)
# ax1.set_title("No Rasterization")

# ax2.set_aspect(1)
# ax2.set_title("Rasterization")

# m = ax2.pcolormesh(xx, yy, d)
# m.set_rasterized(True)

# ax3.set_aspect(1)
# ax3.pcolormesh(xx, yy, d)
# ax3.text(0.5, 0.5, "Text", alpha=0.2,
#          va="center", ha="center", size=50, transform=ax3.transAxes)

# ax3.set_title("No Rasterization")


# ax4.set_aspect(1)
# m = ax4.pcolormesh(xx, yy, d)
# m.set_zorder(-20)

# ax4.text(0.5, 0.5, "Text", alpha=0.2,
#          zorder=-15,
#          va="center", ha="center", size=50, transform=ax4.transAxes)

# ax4.set_rasterization_zorder(-10)

# ax4.set_title("Rasterization z$<-10$")


# # ax2.title.set_rasterized(True) # should display a warning

# plt.savefig("test_rasterization.pdf", dpi=150)
# plt.savefig("test_rasterization.eps", dpi=150)

# if not plt.rcParams["text.usetex"]:
#     plt.savefig("test_rasterization.svg", dpi=150)
#     # svg backend currently ignores the dpi



# ============================================================================================

# import numpy as np
# import matplotlib.pyplot as plt
# from numpy import array

# width = 100
# height = 80

# # The triangle
# d = np.array([ [ [0.9, 0.5], [0.5, 0.8], [0.1, 0.15] ] ])

# # Calculates the distance from the edge v0-v1 to point p
# def edgefunc(v0, v1, p):
#     px = p[:, 1]
#     py = p[:, 0]
#     return (v0[:,0] - v1[:,0])*px + (v1[:,1] - v0[:,1])*py + (v0[:,1]*v1[:,0] - v0[:,0]*v1[:,1])

# # Calculate the area of the parallelogram formed by the triangle
# area = edgefunc(d[:, 2, :], d[:, 1, :], d[:, 0, :])

# # Create a grid of sampling positions
# ys = np.linspace(0, 1, height, endpoint=False)
# xs = np.linspace(0, 1, width, endpoint=False)
# xpos = np.tile(xs, (height, 1))
# ypos = np.tile(ys, (width, 1)).transpose()

# # Reshape the sampling positions to a H x W x 2 tensor
# pos = np.moveaxis(array(list(zip(ypos, xpos))), 1, 2)
# pos = np.reshape(pos, (height*width, 2))

# # Evaluate the edge functions at every position
# w0 = edgefunc(d[:, 2, :], d[:, 1, :], pos)
# w1 = edgefunc(d[:, 0, :], d[:, 2, :], pos)
# w2 = edgefunc(d[:, 1, :], d[:, 0, :], pos)

# # Only pixels inside the triangle will have color
# mask = (w0 > 0) & (w1 > 0) & (w2 > 0)

# data = np.zeros((height * width, 3), dtype=np.uint8)
# data[:,0] = (w0 / area[0])*255*mask
# data[:,1] = (w1 / area[0])*255*mask
# data[:,2] = (w2 / area[0])*255*mask
# plt.imshow(np.reshape(data, (height, width, 3)), interpolation='nearest')
# plt.show()





# import cv2, numpy as np             # for image IO
# from contour import *
# from rasterizer import Rasterizer

# ## rasterize a polyline
# contour = Line.Contour([(4,4), (30,6), (10,14)])
# raster = Rasterizer(contour, 32, 32).get()
# raster = np.array(np.asarray(raster)*255+0.5, np.uint8)
# cv2.imwrite('/home/sushantnigudkar/Downloads/Line.png', raster)



# import os
# if not os.path.exists('data'): os.makedirs('data')
# if not os.path.exists('output'): os.makedirs('output')

# import tarfile
# from urllib.request import urlretrieve

# directory = './data/'
# tarfilename = directory + "landsat.tar.gz"

# if not os.path.exists(directory):
#     os.makedirs(directory)

# if not os.path.isfile(tarfilename):
#     url = 'https://www.dropbox.com/s/zb7nrla6fqi1mq4/LC81980242014260-SC20150123044700.tar.gz?dl=1'
#     urlretrieve(url, tarfilename)
#     tar = tarfile.open(tarfilename)
#     tar.extractall(path=directory)
#     tar.close()

# import rasterio
# import rasterio.plot
# import pyproj
# import numpy as np
# import matplotlib
# import matplotlib.pyplot as plt

# print('Landsat on Google:')

# filepath = '/home/sushantnigudkar/Downloads/LC08_L1TP_042034_20170616_20170629_01_T1_B4.TIF'
# with rasterio.open(filepath) as src:
# #     print(src.profile)
#     array = src.read(1)
#     print(array.shape)


# from matplotlib import pyplot
# pyplot.imshow(array, cmap='pink')
# pyplot.show()



# with rasterio.open(filepath) as src:
     
#    oviews = src.overviews(1) # list of overviews from biggest to smallest
#    print(oviews)
#    oview=81
#    #oview = oviews[-1] # let's look at the smallest thumbnail
#    print('Decimation factor= {}'.format(oview))
#    # NOTE this is using a 'decimated read' (http://rasterio.readthedocs.io/en/latest/topics/resampling.html)
#    thumbnail = src.read(1, out_shape=(1, int(src.height // oview), int(src.width // oview)))

# print('array type: ',type(thumbnail))
# print(thumbnail)

# plt.imshow(thumbnail)
# plt.colorbar()
# plt.title('Overview - Band 4 {}'.format(thumbnail.shape))
# plt.xlabel('Column #')
# plt.ylabel('Row #')


# ===========================================================================================

# import pylab as py
# arr = py.randn(100000, 2)
# py.plot(arr[:,0], arr[:,1], 'o', alpha=0.1, rasterized=False)
# py.savefig('dots_vector.pdf')

#  ==========================================================================================

# import numpy as np
# import matplotlib.pyplot as plt 


# def add_patch(ax, **kwargs):
#     if 'rasterized' in kwargs and kwargs['rasterized']:
#         ax.set_rasterization_zorder(0)
#     ax.fill_between(np.arange(1, 10), 1, 2, zorder=-1, **kwargs)
#     ax.set_xlim(0, 10) 
#     ax.set_ylim(0, 3)
#     if 'alpha' in kwargs and kwargs['alpha'] < 1:
#         txt = 'This patch is transparent!'
#     else:
#         txt = 'This patch is not transparent!'
#     ax.text(5, 1.5, txt, ha='center', va='center', fontsize=25, zorder=-2,
#             rasterized=True)

# fig, axes = plt.subplots(nrows=4, sharex=True)
# add_patch(axes[0], alpha=0.2, rasterized=False)
# add_patch(axes[1], alpha=0.2, rasterized=True)
# add_patch(axes[2], rasterized=False)
# add_patch(axes[3], rasterized=True)

# plt.tight_layout()
# plt.savefig('rasterized_transparency.eps')