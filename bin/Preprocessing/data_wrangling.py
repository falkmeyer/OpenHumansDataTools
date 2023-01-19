import pandas as pd
import os
import json
import datetime
import tqdm



class Data_Wrangling():

    def __init__(self, data_io):
        self.data_io = data_io



    def convert_to_open_csv_data(self, data, path='', starttime=None):
        df = pd.DataFrame(columns=['path', 'value', 'value_str', 'starttime', 'duration', 'isValid'])
        data_stack = [(data, path)]
        path_stack = []
        list_dicts = []

        while data_stack:
            data, path = data_stack.pop()
            if isinstance(data, dict):
                if 'queuedOn' in data:
                    starttime = data['queuedOn']
                elif 'timestamp' in data:
                    starttime = data['timestamp']
                elif 'date' in data:
                    starttime = data['date']
                
                duration = None
                if 'duration' in data:
                    duration = data['duration']

                isValid = None
                if 'isValid' in data:
                    isValid = data['isValid']

                for key, value in data.items():
                    if key not in ['queuedOn', 'timestamp', 'date', 'duration', 'isValid']:
                        path_stack.append(path + '.' + key if path else key)
                        data_stack.append((value, path + '.' + key if path else key))
            elif isinstance(data, list):
                for item in data:
                    data_stack.append((item, path if path else ''))
            else:
                if isinstance(data, datetime.date):
                    starttime = data
                elif data is not None or str(data) != '':
                    utc_offset = 0 # TODO: read UTC Offset from the UploadInfo file of the upload folder, otherwise assume 0
                    starttime = datetime.datetime.fromtimestamp((starttime + utc_offset) / 1000.0) # the starttime is provided in miliseconds format

                    list_dicts.append({'path': path.strip().lower(), 'value': float(data) if isinstance(data, (int, float)) else None,
                                    'value_str': str(data).strip() if not isinstance(data, (int, float)) else None,
                                    'starttime': starttime,
                                    'duration': float(duration),
                                    'isValid': bool(isValid)
                                    }
                                    )

        df = pd.DataFrame(list_dicts)

        return df


    def group_patients_csv_data(self, patients_jsons_dict, outpath):

        for patient_id in patients_jsons_dict.keys():
            print(f'\nMerge data for patient {patient_id}')
            json_files = patients_jsons_dict[patient_id]
            patient_results = []
            for file_path in json_files:
                #print(f'Processing: {file_path}')
                json_data = self.data_io.json_to_csv(file_path)
                file_name =  os.path.basename(os.path.normpath(file_path)) 
                
                # cleaning up the filename, assuming filename is given by one word, if the real identified is a_b we have to modify this
                if patient_id in file_name: file_name = file_name.split(patient_id)[1]
                if '_' in file_name and len(file_name) > 1: file_name = file_name.split('_')[1]
                if '.json' in file_name: file_name = file_name.split('.json')[0]

                csv_data = self.convert_to_open_csv_data(data=json_data, path=file_name)
                csv_data['patient_id'] = patient_id
                patient_results.append(csv_data)
            
            patient_frame =  pd.concat(patient_results) if len(patient_results) > 0 else pd.DataFrame()
            self.data_io.pandas_to_csv(frame=patient_frame, outpath=os.path.join(outpath, patient_id + ".csv") , index=False )
