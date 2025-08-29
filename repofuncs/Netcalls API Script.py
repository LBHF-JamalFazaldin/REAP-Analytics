# Library imports
import requests
import schedule
import os
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Fetching data method, from relvant API's
def fetch_data():
    load_dotenv(dotenv_path='path/to/key/ENV_key.env') # dotenv = where the API_key is saved/stored as a .ENV file
    api_key = os.getenv('API_KEY')

    # List of report ids containing the relavant information
    report_ids = [
        'b292905d',
        'a3fbba4f',
        '3b23699b',
        '97831374',
        '24ec8888'
    ]
    group_names = [
        'ChildrensSocialCareICAT',
        'HousingAdvice4321',
        'ParkingServices',
        'RentIncomeServices',
        'Repairs'
    ]
    groupings = dict(zip(report_ids,group_names))
    report = pd.DataFrame([])
    for report_id in report_ids:
        url = f"https://handfliberty.nccloud.co.uk/api/Liberty/2/Partitions/3/acd/SavedReports/{report_id}-a270-11ef-9748-005056a63f39?api-key={api_key}"
        response = requests.get(url)

        # Data transformations
        if response.status_code == 200:
            data = response.json()
            
            frames = {}
            for key in data.keys():
                frames[key] = pd.DataFrame(data[key])
                initial_df = pd.concat(frames, axis=0).reset_index(drop=True)
                initial_df = initial_df[initial_df['ViewBy'] != 'Total']
                initial_df['Group'] = groupings[report_id]

            report = pd.concat([report, initial_df], axis=0)

        else:
            print(f"Failed to fetch data: {response.status_code}")

        report.reset_index(drop=True, inplace=True)
        report_json = report.to_json() # To convert the final data frame back into a json format.

    '''
    ### The below is subject to change depending on how we decide to export the data! ###
    '''

    # Exporting the data
    directory = r"C:\Users\jf79\OneDrive - Office Shared Service\Documents\H&F Analysis\Python CSV Repositry" # Directory to my personal repositry, subject to change
    df_name = 'report'
    file_path = f'{directory}\\{df_name}.csv'
    file_exists = os.path.isfile(file_path)
    if file_exists:
        report.to_csv(file_path, header=not file_exists, mode='a', index=False) # Change to export to DW
    
    else:
        report.to_csv(file_path, mode='a', index=False) # Change to export to DW

    return


'''
### The below schedule/restricted_job part of the code is subject to change! ###
###       Based on how the BI Dev team tend to schedule their scripts!       ###

'''

def restricted_job():
    current_time = datetime.now().time()
    start_time = current_time.replace(hour=5, minute=0, second=0, microsecond=0) # 5am
    end_time = current_time.replace(hour=6, minute=0, second=0, microsecond=0) # 6am
    if start_time <= current_time < end_time:
        fetch_data()
        global ran_already
        ran_already = True

schedule.every(5).seconds.do(restricted_job) # Schedules the restricted job function to run when the run_pending() method is called

ran_already=False
while True:
    if ran_already == False:
        schedule.run_pending() # Runs any pending tasks that are within the schedule class
    else:
        time.sleep(3600) # Arbitary time to delay the machine
        schedule.run_pending() # Runs any pending tasks that are within the schedule class