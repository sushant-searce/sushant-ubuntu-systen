import json
import re
uri = "/home/sushantnigudkar/Desktop/dbserver1.inventory.customers+0+0000000009.json"

def stripslashes(s):
   r = re.sub(r"\\(n|r)", "\n", s)
   r = re.sub(r"\\", "", r)
   return r


with open(uri, "r") as f:

    data = stripslashes(f.read().replace("\n",""))
    
    test = data.replace('}""{', '},{')

    final_dictionary = json.loads("["+test[1:-1]+"]") 

    for i in final_dictionary:
        if 'before' in i['payload'] :
            print(i['payload']['op'])


        # if 'before' in i['payload'] :
        #     print(i['payload']['after']['first_name'])
        # if 'after' in i['payload'] :
        #     print(i['payload']['after']['first_name'])


# data = json.load(f)
# print(data)
# schema = data["schema"]
# print(schema)
# payload = data["payload"]
# # print(payload)
# bfr = payload["before"]
# aft = payload["after"]
# print(bfr, aft)

# if bfr is None:
#     print("this is insert query")
# else:
#     print("this is update or delete query")

