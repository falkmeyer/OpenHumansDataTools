# Fixed syntax

# uses the restructured data after the data wrangling process and generates a summary
import os
import pandas as pd


####### Sumarization part

class Data_Summarization():

    def __init__(self, data_io):
        self.data_io = data_io


    ###########################
    ####### File usage ########
    ###########################

    


    def summarize_data(self, csv_files, outpath):


        #result_frames = {}
        # loop over each file
        for csv_file in csv_files:

            # create a dataframe for the analysis
            analysis_df = pd.DataFrame(columns=["path", "value_count", "mean", "max", "min", "mean_duration"])
            patient_id =  os.path.basename(os.path.normpath(csv_file)) 
            if '.csv' in patient_id: patient_id = patient_id.split('.csv')[0] # cleaning up ending

            # open file
            with open(csv_file, "r") as f:
                # read csv
                df = pd.read_csv(f)
                df["value"] = pd.to_numeric(df["value"],errors='coerce').fillna(0) #convert to numeric and fill null values with 0

                # group by path
                grouped_df = df.groupby("path")

                # loop over each group
                for path, group in grouped_df:
                    # check if value exists
                    if "value" in group:
                        # calculate statistics
                        value_count = len(group["value"])
                        mean = group["value"].mean()
                        max_value = group["value"].max()
                        min_value = group["value"].min()
                    else:
                        value_count = 0
                        mean = 0
                        max_value = 0
                        min_value = 0
                    
                    # calculate mean duration
                    if 'duration' in group:
                        mean_duration = group["duration"].mean()
                    else:
                        mean_duration = 0
                    
                    # add to analysis dataframe
                    analysis_df = analysis_df.append({"path": path, "value_count": value_count, "mean": mean, "max": max_value, "min": min_value, "mean_duration": mean_duration}, ignore_index=True)
                
                self.data_io.pandas_to_csv(frame=analysis_df, outpath=os.path.join(outpath, patient_id + ".csv"), index=False)
                #analysis_df.to_csv( os.path.join(outpath, patient_id + ".csv" ) , index=False)
        
                #return result_frames

        return


    ######## Some helper functions


    def merge_csv_files(self, file_list):
        df = pd.DataFrame()

        for file in file_list:
            temp_df = pd.read_csv(file)
            df = df.append(temp_df, ignore_index=True)
        return df


    
    ###############################
    ####### Database usage ########
    ###############################

    ########### Data summarization using queries on the Datbase #######