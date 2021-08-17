# SageMaker paths
prefix      = '/opt/ml/'
param_path  = os.path.join(prefix, 'input/config/hyperparameters.json')
data_path   = os.path.join(prefix, 'input/config/inputdataconfig.json')

# Read hyper parameters passed by SageMaker
with open(param_path, 'r') as params:
    hyperParams = json.load(params)

lr = float(hyperParams.get('lr', '0.1'))
batch_size = int(hyperParams.get('batch_size', '128'))
epochs = int(hyperParams.get('epochs', '10'))

# Read input data config passed by SageMaker
with open(data_path, 'r') as params:
    inputParams = json.load(params)