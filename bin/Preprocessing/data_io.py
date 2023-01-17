# contains all data io functions <-> read/write to db etc.

import os
import json
import datetime
import csv

import pandas as pd
import gzip

class Data_IO():



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
