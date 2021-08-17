import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
log = logging.getLogger(__name__)

class RedshiftUpsertOperatorMssql(BaseOperator):
    
    @apply_defaults
    
    def __init__(self, src_redshift_conn_id, dest_redshift_conn_id,src_table, *args, **kwargs):
       # print(column_name)
       self.src_redshift_conn_id = src_redshift_conn_id
       self.dest_redshift_conn_id = dest_redshift_conn_id
       self.src_table = src_table
       self.main_prefix=kwargs['op_kwargs']['main_prefix']

       

       super(RedshiftUpsertOperatorMssql , self).__init__(*args, **kwargs)
    
    def execute(self, context):
        

        self.hook = PostgresHook(postgres_conn_id=self.src_redshift_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        
        log.info("Connected with " + self.src_redshift_conn_id)
        # build the SQL statement
        sql_statement = "begin transaction; "
        sql_statement += "; "
        # Truncate temp table
        sql_statement += "truncate "+ self.main_prefix+"."+ self.src_table +";"
        sql_statement += "; "


        sql_statement += " end transaction; "
        
        print(sql_statement)
        cursor.execute(sql_statement)
        cursor.close()
        conn.commit()
        log.info("Upsert command completed")
    
class RedshiftUpsertOperatorPluginMssql(AirflowPlugin):
    name = "redshift_upsert_plugin_mssql"
    operators = [RedshiftUpsertOperatorMssql]
