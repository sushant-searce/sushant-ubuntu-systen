# -*- coding: utf-8 -*-
import yaml
from subprocess import call
import argparse
import time

def read_yaml(file):
    try:
        with open(file, 'r') as stream:
            data_loaded = yaml.load(stream, Loader=yaml.FullLoader)
    except FileNotFoundError:
        raise file + ' ' + FileNotFoundError
    except FileExistsError:
        raise file + ' ' + FileExistsError
    return data_loaded

# Decodes YAML file
def decode_yaml(data_loaded):
    python_file = data_loaded["general"]['scriptFile']
    inputs = data_loaded["input"]["source"]
    outputs = data_loaded["output"]["dest"]
    process = data_loaded["functional"]["process"]
    out = [python_file, inputs, outputs, process]
    return out


# Creates commands for execution
def cmds_creator(python_file, inputs, outputs, process, cmds = []):
    ips = []
    output = []
    # input_args = []
    # output_args = []
    if cmds == []:
        for i in inputs:
            ips.append([i["argName"], i["name"]])
        for j in outputs:
            output.append([j["argName"], j["name"]])
        temp_cmd = ['python', python_file] # creates 'python foo.py'
        for i in range(len(ips)):
            temp_cmd.append('--{}'.format(ips[i][0]))
            temp_cmd.append(ips[i][1])
        for i in range(len(output)):
            temp_cmd.append('--{}'.format(output[i][0]))
            temp_cmd.append(output[i][1])
        for key in process.keys():
            if 'dataType' in process[key]: # not a subparser
                temp_cmd.append('--{}'.format(key))
                if 'value' in process[key]:
                    temp_cmd.append(str(process[key]['value']))
            else: # a subparser
                temp_cmd.append(key)
                return cmds_creator(python_file,inputs,outputs,process[key],temp_cmd)
        cmds+=temp_cmd
        return [cmds]
    else:
        temp_cmd = []
        for key in process.keys():
            if 'dataType' in process[key]: # not a subparser
                temp_cmd.append('--{}'.format(key))
                if 'value' in process[key]:
                    temp_cmd.append(str(process[key]['value']))
            else: # a subparser
                temp_cmd.append(key)
                return cmds_creator(python_file,inputs,outputs,process[key],temp_cmd)
        cmds+=temp_cmd
        return [cmds]

# Executes the commands one by one.
# If needed, we can implement Threading features also
# def cmd_execution(cmds, data_loaded):
#     for _, cmd in enumerate(cmds):
#         start = time.time()
#         ret = call(cmd)
#         if ret != 0:
#             if ret < 0:
#                 print("Killed by signal", -ret)
#             else:
#                 print("Command failed with return code", ret)
#         else:
#             print("SUCCESS!!")
#             # print(data_loaded["output"]['status']['result'])

#         # print('Output file:{}  is created'.format(cmd[-1]))
#         print('time taken for execution:{} Sec'.format(time.time() - start))

def main(args):
    data_loaded = read_yaml(args.yaml)
    python_file, inputs, outputs, process = decode_yaml(data_loaded)
    cmds = cmds_creator(python_file, inputs, outputs, process)
    print('commands', cmds)
    # cmd_execution(cmds, data_loaded)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    # ap.add_argument("--yaml", required=True, help=" Enter the path of Yaml file")
    ap.add_argument("--yaml", default='Config.yaml', help="Enter the path of Yaml file")
    args = ap.parse_args()

    main(args)
