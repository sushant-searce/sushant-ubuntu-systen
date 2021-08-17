import logging
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MySQLToPostgresOperator(BaseOperator):
    """
    Executes sql code in a Postgres database and inserts into another
    :param src_mysql_conn_id: reference to the source postgres database
    :type src_mysql_conn_id: string
    :param dest_postgress_conn_id: reference to the destination postgres database
    :type dest_postgress_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param parameters: a parameters dict that is substituted at query runtime.
    :type parameters: dict
    """

    template_fields = ('sql', 'parameters', 'pg_schema', 'pg_table',
                       'pg_preoperator', 'pg_postoperator')
    template_ext = ('.sql',)
    ui_color = '#336791'

    @apply_defaults
    def __init__(
            self,
            sql,
            pg_schema,
            pg_table,
            src_mysql_conn_id,
            dest_postgress_conn_id='postgres_default',
            pg_preoperator=None,
            pg_postoperator=None,
            parameters=None,
            fetch_size=1000,
            commit_every=1000,
            *args, **kwargs):
        super(MySQLToPostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.src_mysql_conn_id = src_mysql_conn_id
        self.dest_postgress_conn_id = dest_postgress_conn_id
        self.pg_schema = pg_schema
        self.pg_table = pg_table
        self.pg_preoperator = pg_preoperator
        self.pg_postoperator = pg_postoperator
        self.parameters = parameters
        self.fetch_size = fetch_size
        self.commit_every = commit_every

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        src_mysql = MySqlHook(mysql_conn_id=self.src_mysql_conn_id)
        dest_pg = PostgresHook(postgres_conn_id=self.dest_postgress_conn_id)

        log_text = f"Transferring MySQL query results from {self.src_mysql_conn_id} into Postgres database: {self.dest_postgress_conn_id}"
        logging.info(log_text)
        conn = src_mysql.get_conn()
        src_cursor = conn.cursor()
        src_cursor.execute(self.sql, self.parameters)

        if self.pg_preoperator:
            logging.info("Running Postgres preoperator")
            dest_pg.run(self.pg_preoperator)

        logging.info("Inserting rows into Postgres")
        if self.fetch_size is not None and self.fetch_size > 0:
            log_text = f"Load data using fetch size {self.fetch_size} and commit every {self.commit_every} rows"
            logging.info(log_text)
            while True:
                records = src_cursor.fetchmany(size=self.fetch_size)
                if not records:
                    break
                dest_pg.insert_rows(table=self.pg_table, rows=records, commit_every=self.commit_every)
        else:
            log_text = f"Load data without batch"
            logging.info(log_text)
            dest_pg.insert_rows(table=self.pg_table, rows=src_cursor)

        if self.pg_postoperator:
            logging.info("Running Postgres postoperator")
            dest_pg.run(self.pg_postoperator)

        logging.info("Done.")

