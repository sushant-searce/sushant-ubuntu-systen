import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
log = logging.getLogger(__name__)

class RedshiftUpsertOperator(BaseOperator):
    
    @apply_defaults
    
    def __init__(self, src_redshift_conn_id, dest_redshift_conn_id,src_table, dest_table, src_keys, dest_keys, column_name, *args, **kwargs):
       print(column_name)
       self.src_redshift_conn_id = src_redshift_conn_id
       self.dest_redshift_conn_id = dest_redshift_conn_id
       self.src_table = src_table
       self.column_name = column_name
       self.dest_table = dest_table 
       self.src_keys = src_keys
       self.dest_keys = dest_keys
       

       super(RedshiftUpsertOperator , self).__init__(*args, **kwargs)
    
    def execute(self, context):
        
        self.hook = PostgresHook(postgres_conn_id=self.src_redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        
        log.info("Connected with " + self.src_redshift_conn_id)
        # build the SQL statement
        sql_statement = "begin transaction; "
        sql_statement += "; "
        # Truncate temp table
        sql_statement += "truncate crowdsysprod_temp."+ self.src_table +";"
        sql_statement += "; "
        # Insert into temp table latest values order by __ts_ms and partition for only one row per src_keys (primary key).
        sql_statement += "insert into crowdsysprod_temp."+ self.src_table +" with cte as (select " + ",".join(self.column_name) + ",__ts_ms,__deleted,ROW_NUMBER() over (partition by "+self.src_keys+" order by __ts_ms desc) as rnum from crowdsysprod_cdc."+self.dest_table+")select " + ",".join(self.column_name) + ",__ts_ms,__deleted from cte where rnum=1;"
        sql_statement += "; "
        # Delete from Main table where id in temp table.
        sql_statement += "DELETE FROM crowdsysprod." + self.src_table + " WHERE id IN (SELECT id FROM crowdsysprod_temp." + self.dest_table + " ); "
        sql_statement += "; "
        sql_statement += "; "
        # Insert into main table values of __deleted==false from temp table.
        sql_statement += "insert into crowdsysprod."+ self.src_table +" with cte as (select " + ",".join(self.column_name) + " from crowdsysprod_temp."+self.dest_table+" where __deleted='false')select " + ",".join(self.column_name) + " from cte;"
        sql_statement += "; "
        # Truncate CDC table
        sql_statement += "truncate crowdsysprod_cdc."+ self.src_table +";"
        sql_statement += "; "
        # Truncate temp table
        sql_statement += "truncate crowdsysprod_temp."+ self.src_table +";"
        sql_statement += "; "
        sql_statement += " end transaction; "
        
        print(sql_statement)
        cursor.execute(sql_statement)
        cursor.close()
        conn.commit()
        log.info("Upsert command completed")
    
class RedshiftUpsertPlugin(AirflowPlugin):
        name = "redshift_upsert_plugin"
        operators = [RedshiftUpsertOperator]