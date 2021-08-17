import os
directory = '/home/sushantnigudkar/Desktop/fyusion/coding'

def ABC():
    list=[]
    for filename in os.listdir(directory):
        if filename.endswith(".py"):
            list.append(filename)
    return list

def test(a,b):
    print(a,b)

x = ABC()
#a,b = x[:2]
#test(a,b)
print(x)

for i in x[:2]:
    print(i)


for i in range(0,len(x),2):
  print(x[:2])

#for i in range(0, len(x), 1):
#    print(x[i])
   # print(i+1)
