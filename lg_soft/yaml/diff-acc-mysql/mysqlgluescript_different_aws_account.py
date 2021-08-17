import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'catalog_db', 'catalog_tablename', 'dest_conn', 'destination_table', 'dest_db', 'table_column'])

catalog_db = args['catalog_db']
catalog_tablename = args['catalog_tablename']
dest_conn = args['dest_conn']
destination_table = args['destination_table']
dest_db = args['dest_db']
table_column = args['table_column']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args['JOB_NAME'], args)

datasource = glueContext.create_dynamic_frame.from_catalog(database = catalog_db, table_name = catalog_tablename, transformation_ctx = "datasource")
try:

    tree = datasource._jdf.schema().treeString()
    a = tree.replace("root","")
    b = a.replace("|--","").replace(": ",":")
    c = b.split(" ")

    converted_list = []
    for element in c:
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
    print(List)

    Test = []
    for i in List:
        a = ''.join(i)
        for j in table_column:
            if j in a:
                Test.append(i)
                
    print(Test)

    applymapping = ApplyMapping.apply(frame = datasource, mappings = Test, transformation_ctx = "applymapping")
    resolvechoice = ResolveChoice.apply(frame = applymapping, choice = "make_cols", transformation_ctx = "resolvechoice")
    dropnullfields = DropNullFields.apply(frame = resolvechoice, transformation_ctx = "dropnullfields")
    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields, catalog_connection = dest_conn, connection_options = {"dbtable": destination_table, "database": dest_db}, transformation_ctx = "datasink")

except:
    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource, catalog_connection = dest_conn, connection_options = {"dbtable": destination_table, "database": dest_db}, transformation_ctx = "datasink")

job.commit()