import os
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(filename='processing_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_csv_files(start_date, end_date, users=None, base_folder='.'):
    """
    Process CSV.GZ files for specified users (or all users) within a given date range using Dask.

    :param start_date: Start date in 'YYYYMMDD' format
    :param end_date: End date in 'YYYYMMDD' format
    :param users: List of users, if None then process all users
    :param base_folder: Base folder path containing date folders
    """
    execution_start_time = datetime.now()
    logging.info(f"Execution started - Date range: {start_date} to {end_date}, Users: {'All users' if users is None else users}")

    # Start Dask client
    client = Client()
    logging.info(f"Dask client started with {client.ncores} cores")

    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    date_range = [start + timedelta(days=x) for x in range((end-start).days + 1)]

    all_data = {}
    missing_files = []
    missing_folders = []
    discontinuities = []

    with ThreadPoolExecutor() as executor:
        future_to_date = {executor.submit(process_date_folder, date, users, base_folder): date for date in date_range}
        for future in as_completed(future_to_date):
            date = future_to_date[future]
            try:
                result = future.result()
                if result['data']:
                    update_all_data(all_data, result['data'])
                missing_files.extend(result['missing'])
                if not result['data'] and not result['missing']:
                    missing_folders.append(date.strftime('%Y%m%d'))
            except Exception as exc:
                logging.error(f'Error processing folder {date.strftime("%Y%m%d")}: {exc}')

    # Check time continuity across folders
    check_cross_folder_continuity(all_data, discontinuities)

    # Output results
    output_results(all_data, start_date, end_date)
    log_processing_summary(missing_files, missing_folders, discontinuities)

    client.close()
    execution_end_time = datetime.now()
    execution_duration = execution_end_time - execution_start_time
    logging.info(f"Execution completed - Total duration: {execution_duration}")
    logging.info("-" * 50)  # Add a separator line to indicate the end of this execution

def process_date_folder(date, users, base_folder):
    """Process CSV.GZ files in a single date folder using Dask"""
    folder_name = date.strftime('%Y%m%d')
    folder_path = os.path.join(base_folder, folder_name)
    result = {'data': {}, 'missing': []}

    if not os.path.exists(folder_path):
        logging.warning(f"Folder does not exist: {folder_path}")
        return result  # Return empty result, but continue execution

    for file in os.listdir(folder_path):
        if file.endswith('.csv.gz'):
            user = file[:-7]  # Remove '.csv.gz' to get the user ID
            if users is None or user in users:
                file_path = os.path.join(folder_path, file)
                try:
                    df = dd.read_csv(file_path, compression='gzip', parse_dates=['timestamp'])
                    if not df.empty.compute():
                        result['data'][user] = {
                            'folder': folder_name,
                            'start_time': df['timestamp'].min().compute(),
                            'end_time': df['timestamp'].max().compute(),
                            'data': df
                        }
                    else:
                        result['missing'].append((folder_name, user))
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
                    result['missing'].append((folder_name, user))

    return result

def check_cross_folder_continuity(all_data, discontinuities):
    """Check time continuity across folders"""
    for user, folders_data in all_data.items():
        sorted_folders = sorted(folders_data.keys())
        for i in range(len(sorted_folders) - 1):
            current_folder = sorted_folders[i]
            next_folder = sorted_folders[i + 1]
            current_end_time = folders_data[current_folder]['end_time']
            next_start_time = folders_data[next_folder]['start_time']
            
            time_diff = (next_start_time - current_end_time).total_seconds()
            if time_diff > 15:  # Allow 15 seconds tolerance
                discontinuities.append({
                    'user': user,
                    'current_folder': current_folder,
                    'next_folder': next_folder,
                    'end_time': current_end_time,
                    'start_time': next_start_time,
                    'time_diff': time_diff
                })

def update_all_data(all_data, new_data):
    """Update the main data dictionary"""
    for user, data in new_data.items():
        if user not in all_data:
            all_data[user] = {}
        all_data[user][data['folder']] = data

def output_results(all_data, start_date, end_date):
    """Output processed data using Dask"""
    output_folder = 'processed_data'
    os.makedirs(output_folder, exist_ok=True)
    
    for user, folders_data in all_data.items():
        output_file = os.path.join(output_folder, f'{user}_{start_date}_{end_date}.csv.gz')
        combined_df = dd.concat([folder_data['data'] for folder_data in folders_data.values()])
        combined_df = combined_df.sort_values('timestamp')
        combined_df.to_csv(output_file, index=False, compression='gzip', single_file=True)
        logging.info(f"Data for user {user} has been output to {output_file}")

def log_processing_summary(missing_files, missing_folders, discontinuities):
    """Log processing summary"""
    logging.info("Processing Summary:")
    if missing_folders:
        logging.info("Missing folders:")
        for folder in missing_folders:
            logging.info(f"  Folder {folder} does not exist")
    
    if missing_files:
        logging.info("Missing files:")
        for folder, user in missing_files:
            logging.info(f"  File missing for user {user} in folder {folder}")
    
    if discontinuities:
        logging.info("Time discontinuities:")
        for disc in discontinuities:
            logging.info(f"  User {disc['user']} has time discontinuity between folders {disc['current_folder']} and {disc['next_folder']}")
            logging.info(f"    End time: {disc['end_time']}")
            logging.info(f"    Start time: {disc['start_time']}")
            logging.info(f"    Time difference: {disc['time_diff']} seconds")

if __name__ == "__main__":
    start_date = '20240101'
    end_date = '20240131'
    users = None  # Set to None to process all users, or provide a list of user UIDs
    process_csv_files(start_date, end_date, users)
