import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

"""
A script contains the code that extracts data from sources, transforms it, and loads it into targets. AWS Glue runs a script when it starts a job.
The script is for a job that copies a ->
1. simple dataset from one MySQL RDS to another MySQL RDS.
2. MySQL RDS to Amazon Simple Storage Service (Amazon S3) location.

depending upon the 'jobType' variable we are running the respective job. 
"""

args = getResolvedOptions(sys.argv, ['jobType'])
jobType=args['jobType']

if jobType=="mysql_to_mysql":
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'mysql_source_endpoint', 'mysql_source_port', 'mysql_source_host_username', 'mysql_source_host_password', 'source_database', 'source_table', 'mysql_destination_endpoint', 'mysql_destination_port', 'mysql_destination_host_username', 'mysql_destination_host_password', 'destination_database', 'destination_table', 'columns'])


    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    mysql_source_endpoint = args['mysql_source_endpoint']
    mysql_source_port = args['mysql_source_port']
    mysql_source_host_username = args['mysql_source_host_username']
    mysql_source_host_password = args['mysql_source_host_password']
    source_database = args['source_database']
    source_table = args['source_table']
    mysql_destination_endpoint = args['mysql_destination_endpoint']
    mysql_destination_port = args['mysql_destination_port']
    mysql_destination_host_username = args['mysql_destination_host_username']
    mysql_destination_host_password = args['mysql_destination_host_password']
    destination_database = args['destination_database']
    destination_table = args['destination_table']
    col = args['columns']
    columns = col.replace("'","")
    table_column = columns.strip('][').split(', ')
    print(table_column)
    
    print("List number of columns included in list: {}".format(table_column))
    print("Type of column variable: {}".format(type(table_column)))

    connection_mysql5_options = {
        "url": "jdbc:mysql://{}:{}/{}".format(mysql_source_endpoint,mysql_source_port,source_database),
        "dbtable": "{}".format(source_table),
        "user": "{}".format(mysql_source_host_username),
        "password": "{}".format(mysql_source_host_password)}


    connection_mysql7_options = {
        "url": "jdbc:mysql://{}:{}/{}".format(mysql_destination_endpoint,mysql_destination_port,destination_database),
        "dbtable": "{}".format(destination_table),
        "user": "{}".format(mysql_destination_host_username),
        "password": "{}".format(mysql_destination_host_password)}

    datasource = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql5_options)

    try:
        tree_struct = datasource._jdf.schema().treeString()
        split_tree = tree_struct.replace("root","")
        replace_tree = split_tree.replace("|--","").replace(": ",":")
        x = replace_tree.split(" ")

        converted_list = []
        for element in x:
            converted_list.append(element.strip())

        while("" in converted_list) :
            converted_list.remove("")

        List=[]
        for i in converted_list:
            y = []
            a = i.split(":")
            for j in a:
                y.append(j)
            for z in a:
                y.append(z)

            y =tuple(y)
            List.append(y)
        print("This is the list from datasource: {}".format(List))

        Test = []
        for i in List:
            a = ''.join(i)
            for j in table_column:
                if j in a:
                    Test.append(i)
                        
        applymapping = ApplyMapping.apply(frame = datasource, mappings = Test, transformation_ctx = "applymapping")
        resolvechoice = ResolveChoice.apply(frame = applymapping, choice = "make_cols", transformation_ctx = "resolvechoice")
        dropnullfields = DropNullFields.apply(frame = resolvechoice, transformation_ctx = "dropnullfields")
        datasink = glueContext.write_dynamic_frame.from_options(frame = dropnullfields, connection_type="mysql", connection_options=connection_mysql7_options, transformation_ctx = "datasink")
        job.commit()
    except:
        datasink = glueContext.write_dynamic_frame.from_options(frame = datasource, connection_type="mysql", connection_options=connection_mysql7_options, transformation_ctx = "datasink")
        job.commit()

elif jobType=="mysql_to_s3":

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'mysql_source_endpoint', 'mysql_source_port', 'mysql_source_host_username', 'mysql_source_host_password', 'source_database', 'source_table', 'columns', 's3_path_to_store_data', 'format'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    mysql_source_endpoint = args['mysql_source_endpoint']
    mysql_source_port = args['mysql_source_port']
    mysql_source_host_username = args['mysql_source_host_username']
    mysql_source_host_password = args['mysql_source_host_password']
    source_database = args['source_database']
    source_table = args['source_table']
    s3_path_to_store_data = args['s3_path_to_store_data']
    format_ = args['format']

    col = args['columns']
    columns = col.replace("'","")
    table_column = columns.strip('][').split(', ')
    print(table_column)
    
    print("List number of columns included in list: {}".format(table_column))
    print("Type of column variable: {}".format(type(table_column)))

    connection_mysql5_options = {
        "url": "jdbc:mysql://{}:{}/{}".format(mysql_source_endpoint,mysql_source_port,source_database),
        "dbtable": "{}".format(source_table),
        "user": "{}".format(mysql_source_host_username),
        "password": "{}".format(mysql_source_host_password)}

    datasource = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql5_options)
    try:
        tree_struct = datasource._jdf.schema().treeString()
        split_tree = tree_struct.replace("root","")
        replace_tree = split_tree.replace("|--","").replace(": ",":")
        x = replace_tree.split(" ")

        converted_list = []
        for element in x:
            converted_list.append(element.strip())

        while("" in converted_list) :
            converted_list.remove("")

        List=[]
        for i in converted_list:
            y = []
            a = i.split(":")
            for j in a:
                y.append(j)
            for z in a:
                y.append(z)

            y =tuple(y)
            List.append(y)
        print("This is the list from datasource: {}".format(List))

        Test = []
        for i in List:
            a = ''.join(i)
            for j in table_column:
                if j in a:
                    Test.append(i)
                    
        applymapping = ApplyMapping.apply(frame = datasource, mappings = Test, transformation_ctx = "applymapping")
        resolvechoice = ResolveChoice.apply(frame = applymapping, choice = "make_cols", transformation_ctx = "resolvechoice")
        dropnullfields = DropNullFields.apply(frame = resolvechoice, transformation_ctx = "dropnullfields")
        dynamic_frame_with_less_partitions=dropnullfields.coalesce(1)
        datasink = glueContext.write_dynamic_frame.from_options(frame = dynamic_frame_with_less_partitions, connection_type ="s3", connection_options = {"path": s3_path_to_store_data}, format = format_, transformation_ctx = "datasink")
        job.commit()

    except:
        dynamic_frame_with_less_partitions=datasource.coalesce(1)
        datasink = glueContext.write_dynamic_frame.from_options(frame = dynamic_frame_with_less_partitions, connection_type ="s3", connection_options = {"path": s3_path_to_store_data}, format = format_, transformation_ctx = "datasink")
        job.commit()

else:
    print("Glue job")