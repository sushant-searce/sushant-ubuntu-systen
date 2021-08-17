import awswrangler as wr
from datetime import datetime
import pandas as pd
from pandas_schema.validation import CustomElementValidation
from pandas_schema import Column, Schema
import pandas_schema

def get_validator(opts):
    VALIDATORS = {
        'decimal': (CustomElementValidation, [lambda d: isinstance(d, float), 'is not decimal']),
        'int': (CustomElementValidation, [lambda i: isinstance(i, int), 'is not integer']),
        'null': (CustomElementValidation, [lambda d: d is None, 'this field cannot be null']),
        'string': (CustomElementValidation, [lambda s: isinstance(s, str), 'This field is not a string']),
    }
    func, args = VALIDATORS[opts[0]]
    args.extend(opts[1:])
    return func(*args)
    
def main(event, context):
    # Step 1 - Fetch the s3 input Path. 
    input = event['input']
    output = event.get('output').get('dest')
    s3_output_location  = output.get('FilePath') + '/' + output.get('FileName')
    json_schema  = input['FieldValidations']
    input_s3_location = input['FilePath'] + '/'+ input['FileName']
    fields_to_process  = input['Fields']['need']

    data = wr.s3.read_csv(input_s3_location)
    data_clean = data[fields_to_process]

    # Step 3 - Do Validate. 
    column_list = [Column(k, [get_validator([v]) for v in value['conditions']]) for k, value in json_schema.items()]
    schema = Schema(column_list)
    errors = schema.validate(data)
    errors_index_rows = [e.row for e in errors]
    if(len(errors_index_rows) > 1):
        data_clean = data_clean.drop(index=errors_index_rows)
    
    #  Step 4 - Save CSV. 
    wr.s3.to_csv(data_clean, s3_output_location, header=False,index=False)
    
    return {
        "s3_output_location": s3_output_location,
        "ContentType": "text/csv"
    }

if __name__ == "__main__":
    main('', '')