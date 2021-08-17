import json
import sys

import awswrangler as wr
import pandas as pd

from demjson import decode

def compare(params):
    print("Inside the Script:::",params)
    param_obj = decode(params)
    file1 = wr.s3.read_csv(param_obj["file_location_1"])
    file2 = wr.s3.read_csv(param_obj["file_location_2"])
    wr.s3.to_csv(pd.merge(file1, file2), param_obj["output_location"])
    print("Completed")


compare(sys.argv[3])
