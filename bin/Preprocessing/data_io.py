# contains all data io functions <-> read/write to db etc.

import os
import json
import datetime
import csv

import numpy as np
import pandas as pd
import gzip
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, UniqueConstraint, Boolean, insert, update
from sqlalchemy.pool import NullPool
from sqlalchemy import inspect
import zipfile

import zipfile
import shutil


class File_IO():



    def __init__(self):
        pass


    ########## Data IO for files ########

    # get patients folders
    def get_folder_structure(self, parent_folder):
        folder_paths = []
        for dirpath, dirnames, filenames in os.walk(parent_folder):
            for dirname in dirnames:
                folder_paths.append(os.path.join(dirpath, dirname))
        return folder_paths

    # get patients folders
    def get_folder_paths(self, parent_folder):
        folder_paths = []
        for dirpath, dirnames, filenames in os.walk(parent_folder):
            for dirname in dirnames:
                folder_paths.append(os.path.join(dirpath, dirname))
        return folder_paths


    # function to get the list of subfolders of a directory
    def getSubfolders(self, parentDirectory):
        subfolders = []
        for item in os.listdir(parentDirectory):
            itemPath = os.path.join(parentDirectory, item)
            if os.path.isdir(itemPath):
                subfolders.append(itemPath)
        return subfolders

    # get json files inside patient folders
    def find_files_with_ending(self, path, ending='.json'):
        json_files = []
        # r=root, d=directories, f = files
        for r, d, f in os.walk(path):
            for file in f:
                if file.endswith(ending):
                    json_files.append(os.path.join(r, file))
        return json_files


    def get_folder_structure(self, parent_folder):
        result = []
        parent_folder_index = len(os.path.abspath(parent_folder))+1
        for dirpath, dirnames, filenames in os.walk(parent_folder):

            for file in filenames:
                if file.startswith('.'): continue # ignore meta files
                f = os.path.join(dirpath, file)[parent_folder_index:]
                splitted = f.split('/')

                d = { **{f'level_{i}' : splitted[i] for i in range(0, len(splitted) - 1)} , **{'file' : splitted[-1]} }
                result.append(d)
        return result

    #merge both functions
    def get_dict_of_folder_and_json_files(self, parent_folder):
        folder_paths = self.getSubfolders(parent_folder)
        result = {}
        for folder in folder_paths:
            folder_name =  os.path.basename(os.path.normpath(folder)) 
            result[folder_name] = self.find_json_files(folder)
        return result

    def get_file_or_folder_name(self, path):
        return os.path.basename(os.path.normpath(path)) 

    def load_json(self, file):
        with open(file, 'r') as f:
            data = json.load(f)
        return data

    def pandas_to_csv(self, frame, outpath, index=False):
        frame.to_csv( outpath , index=index)

    def get_csv_files(self, folder):
        csv_files = []
        for filename in os.listdir(folder):
            if filename.endswith(".csv"):
                csv_files.append(os.path.join(folder, filename))
        return csv_files

    def read_csv_pandas(self, csv_file):
        with open(csv_file, "r") as f:
            df = pd.read_csv(f)
            return df



    def unzip_recursively(self, root_path, root_path_copy, unzip_all=True):
        """
            Recursively unzips all zip, tar, gz and tar.gz files in the root_path and copies the content to root_path_copy
        """

        if os.path.exists(root_path_copy):
            shutil.rmtree(root_path_copy)
        os.makedirs(root_path_copy)
        def recursive_unzip(root_path, root_path_copy):
            for root, dirs, files in os.walk(root_path):
                dir_name = os.path.basename(root)
                new_path = os.path.join(root_path_copy, dir_name)
                if not os.path.exists(new_path):
                    os.makedirs(new_path)
                for file in files:
                    if file.endswith(".zip") or file.endswith(".tar") or file.endswith(".gz") or file.endswith(".tar.gz"):
                        file_path = os.path.join(root, file)
                        if file.endswith(".gz") or file.endswith(".tar.gz"):
                            with gzip.open(file_path, 'rb') as f_in:
                                zip_dirname = os.path.splitext(file)[0]
                                unzip_dir = os.path.join(new_path, zip_dirname)
                                if unzip_all or not os.path.exists(unzip_dir):
                                    os.mkdir(unzip_dir)
                                with open(os.path.join(unzip_dir, file[:-3]), 'wb') as f_out:
                                    f_out.write(f_in.read())
                        else:
                            zip_ref = zipfile.ZipFile(file_path)
                            zip_dirname = os.path.splitext(file)[0]
                            if unzip_all or not os.path.exists(os.path.join(new_path, zip_dirname)):
                                zip_ref.extractall(os.path.join(new_path, zip_dirname))
                                recursive_unzip(os.path.join(new_path, zip_dirname), os.path.join(root_path_copy, dir_name, zip_dirname))
                            zip_ref.close()
                    else:
                        shutil.copy(os.path.join(root, file), new_path)
        recursive_unzip(root_path, root_path_copy)


##############################
######## Database io #########
##############################

# Handling read and write operation in order to access a database with stored patients data
# Purpose: inserting prepared csv files of well definied structure in the database
# Simple approach just using jsut one database, no tunnel or jump server. This can be extended if required.


class Database_IO():


    ######################################
    ######## Setup and connection ########
    ######################################

    def __init__(self, host_ip, port, db_user, db_pw, db_name):

        self.engine, self.metadata = self.connect_to_postgres_db(host_ip, port, db_user, db_pw, db_name)
        
        self.read_offsets = {} # providing a dict for offsets, which can be used e.g. for iterating a datbase table patient_wise

        # testing if the connection works out
        print(f'Connected to the database. List of schemes: {self.test_db_connection()}')



    def connect_to_postgres_db(self, host_ip, port, db_user, db_pw, db_name):
        '''
            Connects to a Postgresql database using the given credentials and database name. 
            host_ip can also be set to 'localhost' or to a domain identifier.
        '''

        engine_str=f"postgresql://{db_user}:{db_pw}@{host_ip}:{port}/{db_name}"

        engine = create_engine(engine_str) # echo=True
        metadata = MetaData()
        metadata.reflect(bind=engine)

        return engine, metadata



    def test_db_connection(self):
        '''
            Just priniting recognized schemas
        '''
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()
        return schemas



    #################################
    ######## Read operations ########
    #################################

    ####### Read Data from Data Source #######
    def query_data(self, query):
        frame = pd.read_sql(query, self.engine)
        return frame

    # you can also write a file containing specific queries, which can be used here.



    def read_db(self, source, limit_icustays, offset):
        '''
            Special function for tables with patient_id, but can be adjusted if required.
        '''
            
        query = f'SELECT * from {source} WHERE patient_id IN ( \
                        SELECT patient_id FROM {source} \
                            GROUP BY patient_id \
                            LIMIT {limit_icustays} OFFSET {offset}) ORDER BY patient_id, starttime;'

        result = pd.read_sql(query, self.engine)

        # provisorisch string columns to lowercase konvertieren TODO: fehlerhaft bei bool + NULL columns -> Zieltabelle neu generieren
        non_number_cols = dict(result.select_dtypes(include=[object]) ).keys() #result.select_dtypes(exclude=[np.number])
        for col in non_number_cols:
            result[col] = result[col].astype(str).str.lower()

        # convert icustay_id to string to match charite data types
        result['patient_id'] = result['patient_id'].astype(int)
        result['starttime'] = result['starttime'].astype('datetime64[ns]')

        return result



    def read_next_patient(self, source, limit_patients = 1):
        """
            Reads patients from database.
        """

        # current offset
        self.read_offsets[source] = 0 if not source in self.read_offsets else self.read_offsets[source]

        df = self.read_db(source, limit_patients, offset=self.read_offsets[source])
        self.read_offsets[source] += limit_patients
        return df



    #########################################
    ######## Write/Create operations ########
    #########################################
    
    ############### Create new DB Table ##############
    
    def get_df_column_types(self, data):
        '''
            Returns dict with column names and corresponding python data types.
        '''
        if len(data) == 0:
            print('Frame must contain at least one row of values in order to determin data types')
            return None
        first_row = data.reset_index().iloc[0].to_dict()
        type_dict = { col: type(first_row[col]) for col in first_row }
        
        return type_dict


    def convert_column_types_sqlalchemy(self, type_dict):
        '''
            Converts a type dict, e.g. generated with get_df_column_types to sqlalchemy types
        '''
        sqalchemy_col_types = []
        for col in type_dict.keys():
            if type_dict[col] == bool:
                sqalchemy_col_types.append( Column(col, Boolean) )
            elif type_dict[col] == float or type_dict[col] ==np.float64 or type_dict[col] == int or type_dict[col]==np.int64:
                sqalchemy_col_types.append( Column(col, Float) )
            elif type_dict[col] == str:
                sqalchemy_col_types.append( Column(col, String) )
            elif type_dict[col] == pd._libs.tslibs.timestamps.Timestamp \
                or type_dict[col] == 'datetime' or type_dict[col] == datetime.datetime: # convert from manual typ specification
                sqalchemy_col_types.append( Column(col, DateTime) )
            else:
                print(f'Error: Unknown Data type: {type_dict[col]}. No mapping to sqalchemy defined.')
                return None

        return sqalchemy_col_types



    def create_new_db_table(self, table_name, type_dict, unique_constrained = None): # unique_constrained can be an array of columns which have to be uniqze together
        '''
            Create new table in the specfified db. A dict with data types is required.
            Optional a constrained for unique together can be set.
        '''
        columns_sqalch = self.convert_column_types_sqlalchemy(type_dict)
        if not columns_sqalch: # error occured in converting columns
            return

        if unique_constrained: # add a unique together constrained in order to avoid duplicated entries by wrong usage
            specified_columns = set(type_dict.keys()).intersection(set(unique_constrained))
            assert len(specified_columns) == len(unique_constrained ) # check if all unique_constrained columns are in type_dict specified
            columns_sqalch.append( UniqueConstraint(*specified_columns, name=f'unique_{table_name}') )

        engine = self.engine
        meta = MetaData() # schema=db_config['schema']
        table = Table(table_name, meta, *columns_sqalch) # all table infos
        meta.create_all(engine) # creates table, columns and if specified unique constrained
        engine.dispose()
        del table, meta # delete of objects required, otherewise db ressources stay blocked and alter commands (create column) later do not work


    def create_new_index(self, table_name, index_name, index_cols):
        '''
            Runs a sql command to create an index (btree) by default.
        '''
        query = f"CREATE INDEX {table_name}_{index_name} ON {table_name} ({','.join(index_cols)});"
        engine = self.engine
        engine.execute(query)
        engine.dispose()


    ######## Add column to DB table #######

    def db_table_add_new_col(self, storage, table_name, col_name, col_type):
        query = f"ALTER TABLE {table_name} ADD {col_name} {col_type};"
        engine = self.engine
        engine.execute(query)
        engine.dispose()

    def db_table_fill_column(self, storage, table_name, pk_col, pkeys, set_column, fill_value):
        pkeys = [f"'{pkey}'" for pkey in pkeys] # make quote string for query
        query = f"UPDATE {table_name} SET {set_column} = '{fill_value}' WHERE {pk_col} in ({','.join(pkeys)});"
        #print(query)
        #return
        engine = self.engine
        engine.execute(query)
        engine.dispose()


    ################## Write results in existing DB Table ##################

    def append_frame_to_sql_table(self, table_name, data):
        data.to_sql(table_name, index=False,  con=self.engine, if_exists='append')  # , schema=db_config['schema']

