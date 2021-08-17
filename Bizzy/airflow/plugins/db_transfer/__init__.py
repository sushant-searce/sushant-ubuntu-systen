from airflow.plugins_manager import AirflowPlugin
from .operators.mysql_to_postgres_operator import MySQLToPostgresOperator
from .operators.generic_rdbms_to_rdbms_operator import GenericRdbmsToRdbmsTransferOperator
from .operators.mssql_exporter_to_ftp_operator import ASWExporterMsSqlToFTPOperator, CMIExporterMsSqlToFTPOperator, ASWExporterMsSqlToFTPOperatorV2


class DBTransferPlugin(AirflowPlugin):
    name = 'db_transfer_plugin'
    operators = [
        ASWExporterMsSqlToFTPOperator,
        CMIExporterMsSqlToFTPOperator,
        MySQLToPostgresOperator,
        GenericRdbmsToRdbmsTransferOperator,
        ASWExporterMsSqlToFTPOperatorV2
    ]

