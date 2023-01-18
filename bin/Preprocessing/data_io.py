# contains all data io functions <-> read/write to db etc.

import os
import json
import datetime
import csv

import pandas as pd
import gzip
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, UniqueConstraint, Boolean, insert, update
from sqlalchemy.pool import NullPool
from sqlalchemy import inspect

class File_IO():



    def __init__(self):
        pass


    ########## Data IO for files ########

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
    def find_json_files(self, path):
        json_files = []
        # r=root, d=directories, f = files
        for r, d, f in os.walk(path):
            for file in f:
                if file.endswith('.json'):
                    json_files.append(os.path.join(r, file))
        return json_files


    #merge both functions
    def get_dict_of_folder_and_json_files(self, parent_folder):
        folder_paths = self.getSubfolders(parent_folder)
        result = {}
        for folder in folder_paths:
            folder_name =  os.path.basename(os.path.normpath(folder)) 
            result[folder_name] = self.find_json_files(folder)
        return result


    def json_to_csv(self, file):
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
            elif type_dict[col] == float or type_dict[col] == int:
                sqalchemy_col_types.append( Column(col, Float) )
            elif type_dict[col] == str:
                sqalchemy_col_types.append( Column(col, String) )
            elif type_dict[col] == pd._libs.tslibs.timestamps.Timestamp \
                or type_dict[col] == 'datetime': # convert from manual typ specification
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


    def create_new_index(self, storage, table_name, index_name, index_cols):
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

    def append_frame_to_sql_table(self, db, table_name, data):
        data.to_sql(table_name, index=True,  con=self.engine, if_exists='append')  # , schema=db_config['schema']

