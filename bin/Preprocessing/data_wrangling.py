import pandas as pd
import os
import json
import datetime
import tqdm
import numpy as np
import time
import pytz
from multiprocessing.dummy import Pool as ThreadPool 

#try:from tqdm import tqdm_notebook as tqdm # if started via jupyter notebook use this
#except: from tqdm import tqdm # else use the standard terminal tqdm
#from tqdm import tqdm
from tqdm.autonotebook import tqdm


import concurrent.futures

import threading
lock = threading.Lock()

#from types import NoneType


class Data_Wrangling():

    def __init__(self, data_io):
        self.data_io = data_io
        self.mult_threading_processed_jobs = 0


    # internal helper to get filename (if more file related helper required -> embed to data_io)
    def get_name(self, filename):

        if not '_' in filename: return filename

        name = filename.split('_')[0]
        if not name.isalpha():
            name = filename.split('_')[1]
        return name



    ##### helper methdos ######


    def to_unix_time(self, date):
        # check if date_string is already in unix time
        if isinstance(date, int):
            return date
        
        if isinstance(date, str): # for string objects only
            # check if the date_string is in american format
            formats = ['%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%SZ']
            for fmt in formats:
                try:
                    date = datetime.datetime.strptime(date, fmt)
                    break
                except ValueError:
                    pass

        if isinstance(date, datetime.datetime):
            return int(time.mktime(date.timetuple())) * 1000 # convert the datetime object to unix time in miliseconds
        else:
            return None # unknown typ for date


    '''
    def to_unix_time(self, time):
        utc_offset = 0 # TODO: read UTC Offset from the UploadInfo file of the upload folder, otherwise assume 0
        return time + utc_offset
    '''
# Add check for NoneType


    def get_start_time(self, data, utc_offset=0):
        starttime = None
        if 'queuedon' in data:
            starttime = data['queuedon'] # queuedOn originaly, but we converted all to lowercase before
        elif 'timestamp' in data:
            starttime = data['timestamp']
        elif 'date' in data:
            starttime = data['date']
        #elif 'startdate' in data:
        #    starttime = data['startdate']

        #starttime = datetime.datetime.fromtimestamp((starttime + utc_offset) / 1000.0) # the starttime is provided in miliseconds format
        if starttime is not None:
            #utc_offset = datetime.timedelta(seconds=utc_offset)
            #starttime = float(starttime)
            #print(starttime, type(starttime))
            starttime = self.to_unix_time(starttime)
            if starttime:
                starttime = datetime.datetime.fromtimestamp((starttime + utc_offset) / 1000.0) # the starttime is provided in miliseconds format
            #else: starttime = None # datetime.datetime.now() # set starttime to the current time if it is None
            #print(starttime, type(starttime))

                '''
                if 'timezone' in data and data['timezone']:
                    tz = pytz.timezone( data['timezone'])
                    starttime.replace(tzinfo=tz)
                '''

        return starttime


    def get_start_date(self, data, utc_offset=0):
        startdate = None
        if 'startdate' in data and data['startdate']:
            startdate = data['startdate']

            # Added startdate
            startdate = self.to_unix_time(startdate)
            if startdate:
                startdate = datetime.datetime.fromtimestamp((startdate + utc_offset) / 1000.0) # the starttime is provided in miliseconds format

                '''
                if 'timezone' in data and data['timezone']:
                    tz = pytz.timezone( data['timezone'])
                    startdate.replace(tzinfo=tz)
                '''

        return startdate



    def get_duration(self, data):
        duration = None
        if 'duration' in data:
            duration = data['duration']
        return duration

    def get_isValid(self, data):
        isValid = None
        if 'isValid' in data:
            isValid = data['isvalid'] # isValid before
        return isValid

    def get_timeasseconds(self, data):
        timeasseconds = np.nan
        if isinstance(data, int):
            timeasseconds = data
        elif 'timeasseconds' in data:
            timeasseconds = data['timeasseconds']
        
            if 'time' in data: del data['time'] # if we additionally have the time stored in hour format we can delete this
            # we could additionally make here some sanity checks, if both match.

        return timeasseconds

    '''
    def get_timezone(self, data):
        timezone = None
        return
    '''

    def convert_to(self, variable, data_type, default=None):
        try: variable = data_type(variable)
        except: variable = default
        return variable

    #####################################################
    ##### Main unflatten of the open json structer ######
    #####################################################


    def convert_to_open_csv_data2(self, data, 
        parent_parameters={'starttime' : None, 'startdate' : None, 'duration' : None, 'isValid' : None, 'timeasseconds' : None, 'utc_offset' : 0, 'timezone' : ''}, 
        path='', starttime=None):

        '''
            Recursive function to unflatten the data.
            Finds timepoints/timezones and other relevant parameters in parent nested structures and and includes them accordingly to in nested childs.
            Output is the flattend data in fix columns.
        '''

        result_lines = []

        # if we have nested data in a dict format, but stored as string, we convert accordingly to a json like dict
        if isinstance(data, str) and len(data) > 0: 
            try: data = json.loads(data)
            except: pass # otherwise do nothing

        # handling list of elements
        if isinstance(data, list) and len(data) > 0: 
            for item in data: 
                result_lines.extend( self.convert_to_open_csv_data2(data=item, parent_parameters=parent_parameters, path=path) )

        # if its a dict
        elif isinstance(data, dict) and len(data) > 0:
            #sprint(data)
            # we know its a dict, and values must first proceed this dict function
            data = {key.strip().lower(): value for key, value in data.items()} # at first convert all key safe ot lowercase

            # update the parent parameters/scope
            parent_parameters['starttime'] = self.get_start_time(data)
            parent_parameters['startdate']  = self.get_start_date(data)
            parent_parameters['duration']  = self.get_duration(data)
            parent_parameters['isValid']  = self.get_isValid(data)
            parent_parameters['timeasseconds']  = self.get_timeasseconds(data)
            #parent_parameters['timezone']  = self.get_timezone(data) # just for internal time zone conversion usage

            for key, value in data.items():
                key = key.strip().lower() # make the keys undependent of the spelling
                # we have allready handled the time and special parameters
                if key not in ['queuedon', 'timestamp', 'date', 'duration', 'isvalid', 'startdate', 'timeasseconds', 'timezone']: # dicts, lists, or values 
                    newpath = path + '.' + key if path else key # update the path
                    result_lines.extend( self.convert_to_open_csv_data2(data=value, parent_parameters=parent_parameters, path=newpath) )
                    #data_stack.append((value, path + '.' + key if path else key))

        # Add base case
        elif data is None or str(data) == '': # or isinstance(data, (int, float)):
            return []

        else: # we reached the end of the nested structure and got a value in the end
            # no we have a real value (special values are filtered out before with the dict key filter) , dicts and list are handled before above

            entry = {
                        'path': path.strip().lower(), 
                        'value': self.convert_to(variable=data, data_type=float, default=np.nan), #float(data) if data and isinstance(data, (int, float)) else np.nan,
                        'value_str': str(data).strip() if data and not isinstance(data, (int, float)) else None,
                        'starttime': parent_parameters['starttime'],
                        # Added startdate
                        'startdate': parent_parameters['startdate'],
                        #'duration':  float(parent_parameters['duration']) if not parent_parameters['duration'] is None and isinstance(parent_parameters['duration'], (int, float)) else np.nan if not parent_parameters['duration'] is None else None, #float(parent_parameters['duration']) if not parent_parameters['duration'] is None and isinstance(parent_parameters['duration'], (int, float)) else np.nan, # and (not type(parent_parameters['duration']) is type(None)) 
                        'duration' : self.convert_to(variable=parent_parameters['duration'], data_type=float, default=np.nan),
                        'isValid': bool(parent_parameters['isValid']),
                        #'timeasseconds': float(parent_parameters['timeasseconds']) if parent_parameters['timeasseconds'] and isinstance(parent_parameters['timeasseconds'], (int, float)) else np.nan #self.get_timeasseconds(data)
                        'timeasseconds' : self.convert_to(variable=parent_parameters['timeasseconds'], data_type=int, default=np.nan),

                    }

            return [entry]

        return result_lines



    ############ Main execution wrapper ############



    def convert_one_file_to_csv(self, patient_id, file_path):
        '''
            Just proceeds one file.
        '''
        #print(f'Processing: {file_path}')

        json_data = self.data_io.load_json(file_path)
        file_name =  os.path.basename(os.path.normpath(file_path)) 
        
        # cleaning up the filename, assuming filename is given by one word, if the real identified is a_b we have to modify this
        if patient_id in file_name: file_name = file_name.split(patient_id)[1]
        #if '_' in file_name and len(file_name) > 1: file_name = file_name.split('_')[1]
        file_name = self.get_name(filename=file_name)
        if '.json' in file_name: file_name = file_name.split('.json')[0]

        csv_data = self.convert_to_open_csv_data2(data=json_data, path=file_name)
        csv_data = pd.DataFrame(csv_data)

        csv_data['patient_id'] = patient_id
        
        return csv_data



    def convert_files_to_csv(self, patient_id, json_files, results = None, index=-1,  tqdm=False):
        '''
            Works without multi threading, but can also be used as wrapper to proceeed a subset of selected files.
        '''

        patient_results = []
        for file_path in tqdm(json_files) if tqdm else json_files: # use tqdm only if multi threading is disabled
            csv_data = self.convert_one_file_to_csv(patient_id, file_path)
            patient_results.append(csv_data)
            
            if results: 
                lock.acquire(); self.mult_threading_processed_jobs += 1; lock.release() # updates the counter of proceeded files
            #break #  TODO: uncomment for testing purpose just process one file
        patient_frame =  pd.concat(patient_results) if len(patient_results) > 0 else pd.DataFrame()
        
        if results and index > -1: # tust the user with the input here
            results[index] = patient_frame

        return patient_frame



    ### same in multi threading


    def status_thread(self, size_of_jobs):
        '''
            Tracks the progress of the running jobs.
        '''
        pbar = tqdm(total=size_of_jobs)

        processed_jobs = 0
        last_processed_jobs = 0

        while processed_jobs < size_of_jobs:
            lock.acquire()
            processed_jobs = self.mult_threading_processed_jobs
            lock.release()
            if processed_jobs > last_processed_jobs:
                pbar.update(processed_jobs - last_processed_jobs)
                #print(f"Status: {processed_jobs} / {size_of_jobs}")
                last_processed_jobs = processed_jobs

            time.sleep(0.5)
        return




    def multi_t_convert_files_to_csv(self, patient_id, json_files, number_threads=4):
        
        self.mult_threading_processed_jobs = 0
        num_jobs_per_worker = round(len(json_files) / number_threads) + 1 # round up

        threads = []
        patient_results = [None] * number_threads # capture the results returned by the thread


        for i in range(0, number_threads):
            worker_files = json_files[ num_jobs_per_worker * i : num_jobs_per_worker * (i + 1) ]
            t = threading.Thread(target=self.convert_files_to_csv, args=(patient_id, worker_files, patient_results, i))
            threads.append(t)
        
        status_thread = threading.Thread( target=self.status_thread, args=(len(json_files),) )
        threads.append(status_thread)

        [t.start() for t in threads]
        [t.join() for t in threads]

        patient_frame =  pd.concat(patient_results) if len(patient_results) > 0 else pd.DataFrame()
        
        return patient_frame



    
    def group_patients_csv_data(self, patients_jsons_dict, outpath, number_threads=4, column_drop_list = ['startdate', 'timeasseconds']):
        '''
            Oportunity to directly drop columns by some passed names in column_drop_list.
            column names are all passed lowercase
        '''

        tqdm._instances.clear()

        for patient_id in patients_jsons_dict.keys():
            print(f'\nMerge data for patient {patient_id}')
            json_files = patients_jsons_dict[patient_id]

            if number_threads > 1: 
                patient_frame = self.multi_t_convert_files_to_csv(patient_id=patient_id, json_files=json_files, number_threads=number_threads)
            else: 
                patient_frame = self.convert_files_to_csv(patient_id=patient_id, json_files=json_files)

            availiable_columns = [col for col in column_drop_list if col in patient_frame.columns]

            if len(column_drop_list) > 0: patient_frame.drop(columns=availiable_columns)
            
            self.data_io.pandas_to_csv(frame=patient_frame, outpath=os.path.join(outpath, patient_id + ".csv") , index=False )


