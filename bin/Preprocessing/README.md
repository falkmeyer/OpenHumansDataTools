This module implements preprocessing to prepare the uploaded historical patient data. It is assumed that the data is located in the folder OPENHUMANSDATATOOLS/data, although the path can be adjusted alternatively. The data should be divided into patient-wise folders and individual upload folders. An upload folder contains various json files with different information.
The script is divided into the following sections:

(1) Extract and list all variables in a file, with simple statistics such as mean, min, max. The file was then analyzed and relevant variables were saved in a reduced variable file.
(2) Selected variables in a passed variable file are extracted from all patient folders with timestamps and saved in an easily traceable csv file.
(3) Data Cleaning
(4) Data Summarization (plots, statistics, etc.)