import logging
import os
import pymssql
import pandas as pd
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pathlib import Path
from ftplib import FTP
from datetime import datetime, timedelta, date


class CMIExporterMsSqlToFTPOperator(BaseOperator):
    ui_color = '#336791'
    # template_fields = ['ftp_file_name', 'local_full_path', ]

    @apply_defaults
    def __init__(
            self,
            mssql_conn_id,
            ftp_conn_id,
            ftp_dir_name,
            local_dir_name,
            file_type,
            body,
            doc_header=None,
            doc_end=None,
            *args, **kwargs):
        super(CMIExporterMsSqlToFTPOperator, self).__init__(*args, **kwargs)

        self.doc_header = doc_header
        self.doc_end = doc_end
        self.mssql_conn_id = mssql_conn_id
        self.ftp_conn_id = ftp_conn_id
        self.body = body
        self.ftp_dir_name = ftp_dir_name
        self.local_dir_name = local_dir_name
        self.file_type = file_type
        self.mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        self.date = datetime.today().strftime('%Y%m%d')
        self.time = datetime.today().strftime('%H%M%S')

    def execute_each(self, b):
        ftp_conn = '%s' % b['FtpConnection']
        file_dir = '%s' % b['LocalDirName']
        file_name = '%s' % b['ExportDescInitial'] + \
            self.date + '_' + self.time + self.file_type
        #self.date + '_' + '%s' % b['BranchID'] + self.file_type
        local_full_path = str(file_dir + file_name)

        # Execute SQL body
        print('Executing SQL Body: ' + '%s' % b['QueryExec'] + ';')
        df_body = self.mssql.get_pandas_df('%s' % b['QueryExec'] + ';')

        print('%s' % b['FtpConnection'])

        # Create local files
        df_body.to_csv(local_full_path,
                       index=0, header=False, sep='|', mode='w+', line_terminator='\r\n', encoding='utf-8')

        # Put files to FTP
        # Initiating FTP Hook
        ftp = FTPHook(ftp_conn_id=ftp_conn)

        remote_full_path = str(os.path.join(self.ftp_dir_name, file_name))
        print(remote_full_path)
        print(local_full_path)
        ftp.store_file(remote_full_path=remote_full_path,
                       local_full_path_or_buffer=local_full_path)
        # Closing FTP Hook
        ftp.close_conn()
        # Deleting file local
        os.remove(local_full_path)

    def execute(self, context):
        # Create local directory if not exists
        if not os.path.exists(self.local_dir_name):
            os.makedirs(self.local_dir_name)
            print(f'Directory {self.local_dir_name} created')

        for b in self.body:
            logging.info('Executing for branch {0}'.format(b['BranchID']))
            self.execute_each(b)


class ASWExporterMsSqlToFTPOperator(BaseOperator):
    ui_color = '#336791'

    @apply_defaults
    def __init__(
            self,
            mssql_conn_id,
            ftp_conn_id,
            ftp_dir_name,
            local_dir_name,
            file_type,
            body,
            doc_header=None,
            doc_end=None,
            *args, **kwargs):
        super(ASWExporterMsSqlToFTPOperator, self).__init__(*args, **kwargs)

        self.doc_header = doc_header
        self.doc_end = doc_end
        self.mssql_conn_id = mssql_conn_id
        self.ftp_conn_id = ftp_conn_id
        self.body = body
        self.ftp_dir_name = ftp_dir_name
        self.local_dir_name = local_dir_name
        self.file_type = file_type
        self.mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        self.date = datetime.today().strftime('%Y%m%d')

    def execute_each(self, b):
        file_dir = self.local_dir_name
        file_name = '%s' % b['ExportDescInitial'] + \
            self.date + '_' + '%s' % b['FileDesc'] + self.file_type
        local_full_path = str(file_dir + file_name)

        # Execute SQL body
        print('Executing SQL Body: ' + '%s' % b['BranchID'])
        df_header = self.mssql.get_pandas_df('%s' % b['QueryHeader'])
        df_check = self.mssql.get_pandas_df('%s' % b['QueryCheck'])
        df_body = self.mssql.get_pandas_df('%s' % b['QueryExec'])
        df_end = self.mssql.get_pandas_df('%s' % b['QueryEnd'])

        # Create local files
        df_header.to_csv(file_dir + '_1' + self.file_type,
                         index=0, header=False, sep='|')

        df_check.to_csv(file_dir + '_2' + self.file_type,
                        index=0, header=False, sep='|')

        df_body.to_csv(file_dir + '_3' + self.file_type,
                       index=0, header=False, sep='|')

        df_end.to_csv(file_dir + '_4' + self.file_type,
                      index=0, header=False, sep='|')

        # Merge files
        with open(local_full_path, mode='w+', newline='\r\n', encoding='utf-8') as fout:
            print(f'Writing to : {local_full_path}')
            for num in range(1, 5):
                for line in open(file_dir + '_' + str(num) + self.file_type):
                    fout.write(line)
                f = (file_dir + '_' + str(num) + self.file_type)
                # Deleting files 1-4 after merge
                os.remove(f)

        # Initiating FTP Hook
        self.ftp = FTPHook(ftp_conn_id=self.ftp_conn_id)
        # print(self.ftp)

        # Put files to FTP
        remote_full_path = str(os.path.join('asw_exporter_test', file_name))
        print("ftp: " + remote_full_path)
        print("local: " + local_full_path)
        self.ftp.store_file(remote_full_path=remote_full_path,
                            local_full_path_or_buffer=local_full_path)

        # Close FTP Hook
        self.ftp.close_conn()

        # Deleting file local
        os.remove(local_full_path)

    def execute(self, context):
        # Create local directory if not exists
        if not os.path.exists(self.local_dir_name):
            os.makedirs(self.local_dir_name)
            print(f'Directory {self.local_dir_name} created')

        for b in self.body:
            logging.info('Executing for branch {0}'.format(b['BranchID']))
            self.execute_each(b)


class ASWExporterMsSqlToFTPOperatorV2(BaseOperator):
    ui_color = '#336791'

    @apply_defaults
    def __init__(
            self,
            branch_id,
            mssql_conn_id,
            ftp_conn_id,
            ftp_dir_name,
            local_dir_name,
            file_name,
            file_type,
            doc_check,
            doc_body,
            doc_header=None,
            doc_end=None,
            *args, **kwargs):
        super(ASWExporterMsSqlToFTPOperatorV2, self).__init__(*args, **kwargs)

        self.branch_id = branch_id
        self.doc_header = doc_header
        self.doc_check = doc_check
        self.doc_end = doc_end
        self.doc_body = doc_body
        self.doc_header = doc_header
        self.mssql_conn_id = mssql_conn_id
        self.ftp_conn_id = ftp_conn_id
        self.ftp_dir_name = ftp_dir_name
        self.local_dir_name = local_dir_name
        self.file_name = file_name
        self.file_type = file_type
        self.mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        self.date = datetime.today().strftime('%Y%m%d')

    def execute(self, context):
        # Create local directory if not exists
        if not os.path.exists(self.local_dir_name):
            os.makedirs(self.local_dir_name)
            print(f'Directory {self.local_dir_name} created')

        file_dir = self.local_dir_name
        file_name = self.file_name + self.file_type
        local_full_path = str(file_dir + file_name)

        print(file_name)

        # Execute SQL
        df_header = self.mssql.get_pandas_df(self.doc_header)
        df_check = self.mssql.get_pandas_df(self.doc_check)
        df_body = self.mssql.get_pandas_df(self.doc_body)
        df_end = self.mssql.get_pandas_df(self.doc_end)

        # # Create local files
        print(file_dir + self.file_name + '_1' + self.file_type)
        
        df_header.to_csv(file_dir + self.file_name + '_1' + self.file_type,
                         index=0, header=False, sep='|')

        df_check.to_csv(file_dir + self.file_name + '_2' + self.file_type,
                        index=0, header=False, sep='|')

        df_body.to_csv(file_dir + self.file_name + '_3' + self.file_type,
                       index=0, header=False, sep='|')

        df_end.to_csv(file_dir + self.file_name + '_4' + self.file_type,
                      index=0, header=False, sep='|')

        # # Merge files
        with open(local_full_path, mode='w+', newline='\r\n', encoding='utf-8') as fout:
            print(f'Writing to : {local_full_path}')
            for num in range(1, 5):
                for line in open(file_dir + self.file_name + '_' + str(num) + self.file_type):
                    fout.write(line)
                f = (file_dir + self.file_name + '_' + str(num) + self.file_type)
                # Deleting files 1-4 after merge
                os.remove(f)

        # # Initiating FTP Hook
        self.ftp = FTPHook(ftp_conn_id=self.ftp_conn_id)
        print(self.ftp)

        # # Put files to FTP
        remote_full_path = str(os.path.join('asw_exporter_test', file_name))
        print("ftp: " + remote_full_path)
        print("local: " + local_full_path)
        self.ftp.store_file(remote_full_path=remote_full_path,
                            local_full_path_or_buffer=local_full_path)

        # # Close FTP Hook
        self.ftp.close_conn()

        # # Deleting file local
        os.remove(local_full_path)

