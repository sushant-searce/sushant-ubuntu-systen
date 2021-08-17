import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
log = logging.getLogger(__name__)

class RedshiftUpsertOperatorProd(BaseOperator):
    
    @apply_defaults
    
    def __init__(self, src_redshift_conn_id, dest_redshift_conn_id,src_table, dest_table, column_name, src_keys, dest_keys, *args, **kwargs):
       # print(column_name)
       self.src_redshift_conn_id = src_redshift_conn_id
       self.dest_redshift_conn_id = dest_redshift_conn_id
       self.src_table = src_table
       self.column_name = column_name
       self.dest_table = dest_table 
       self.src_keys = src_keys
       self.dest_keys = dest_keys
       # print(kwargs)
       # print(args)
       self.cdc_prefix=kwargs['op_kwargs']['cdc_prefix']
       self.temp_prefix=kwargs['op_kwargs']['temp_prefix']
       self.main_prefix=kwargs['op_kwargs']['main_prefix']

       

       super(RedshiftUpsertOperatorProd , self).__init__(*args, **kwargs)
    
    def execute(self, context):
        coalesce_tables = ['product_discount','product_price','product_uom_detail']

        truncate_tables = ['product_discount']

        self.hook = PostgresHook(postgres_conn_id=self.src_redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        
        log.info("Connected with " + self.src_redshift_conn_id)
        # build the SQL statement
        sql_statement = "begin transaction; "
        sql_statement += "; "
        # Truncate temp table
        sql_statement += "truncate "+ self.temp_prefix+"."+ self.src_table +";"
        sql_statement += "; "


        #adding double quotes to column name
        mystring = self.column_name
        mystringArr = mystring.split(",")
        myjoinString = ""
        for key in mystringArr:
            myjoinString += "\""+key+"\", "

        #Delete for primary key
        myPK = self.src_keys
        myPKArr = myPK.split(",")
        myPKjoinString = " WHERE 1=1 "
        
        if self.src_table in coalesce_tables:
            for key in myPKArr:
                myPKjoinString += " AND coalesce("+self.temp_prefix+"."+ self.dest_table + "."+key+",'NULL') = coalesce("+ self.main_prefix+"."+ self.src_table + "."+key+",'NULL') "
        else :
            for key in myPKArr:
                myPKjoinString += " AND "+self.temp_prefix+"."+ self.dest_table + "."+key+" = "+ self.main_prefix+"."+ self.src_table + "."+key+" "


        # Insert into temp table latest values order by __ts_ms and partition for only one row per src_keys (primary key).
        sql_statement += "insert into "+ self.temp_prefix+"."+ self.src_table +" with cte as (select " + myjoinString[:-2] + ",\"__ts_ms\",\"__deleted\",ROW_NUMBER() over (partition by "+self.src_keys+" order by __ts_ms desc) as \"rnum\" from "+ self.cdc_prefix+"."+self.dest_table+")select " + myjoinString[:-2] + ",\"__ts_ms\",\"__deleted\" from cte where rnum=1;"
        sql_statement += "; "
        # Delete from Main table where id in temp table.
        sql_statement += "DELETE FROM "+ self.main_prefix+"."+ self.src_table + " WHERE EXISTS ( SELECT 1 FROM "+ self.temp_prefix+"."+ self.dest_table + myPKjoinString + " ); "
        sql_statement += "; "
        sql_statement += "; "
        
        if self.src_table in truncate_tables:
            truncateJoinString = " WHERE 1=1 "
        else :
            truncateJoinString = "  WHERE __deleted='false' "
        
        # Insert into main table values of __deleted==false from temp table.
        sql_statement += "insert into "+ self.main_prefix+"."+ self.src_table +" with cte as (select " + myjoinString[:-2] + " from "+ self.temp_prefix+"."+self.dest_table+truncateJoinString+")select " + myjoinString[:-2] + " from cte;"
        sql_statement += "; "
        # Truncate CDC table
        sql_statement += "truncate "+ self.cdc_prefix+"."+ self.src_table +";"
        sql_statement += "; "
        # Truncate temp table
        sql_statement += "truncate "+ self.temp_prefix+"."+ self.src_table +";"
        sql_statement += "; "
        sql_statement += " end transaction; "
        
        print(sql_statement)
        cursor.execute(sql_statement)
        cursor.close()
        conn.commit()
        log.info("Upsert command completed")
    
class RedshiftUpsertOperatorPluginProd(AirflowPlugin):
    name = "redshift_upsert_plugin_production"
    operators = [RedshiftUpsertOperatorProd]
