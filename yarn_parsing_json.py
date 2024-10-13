import json
import csv
import datetime
import sys
import traceback

# Function to calculate average allocated resources
def calculate_avg_resources(start_time, end_time, memory_seconds, vcore_seconds):
    if start_time and end_time:
        try:
            start_time_cal = datetime.datetime.fromtimestamp(start_time / 1000)
            end_time_cal = datetime.datetime.fromtimestamp(end_time / 1000)
            duration = (end_time_cal - start_time_cal).total_seconds()

            if duration > 0:
                avg_allocatedMB = round(float(memory_seconds) / duration, 1) if memory_seconds != -1 else ''
                avg_allocatedVCores = round(float(vcore_seconds) / duration, 1) if vcore_seconds != -1 else ''
            else:
                avg_allocatedMB, avg_allocatedVCores = '', ''
        except Exception:
            avg_allocatedMB, avg_allocatedVCores, duration = '', '', ''
    else:
        avg_allocatedMB, avg_allocatedVCores, duration = '', '', ''

    return duration, avg_allocatedMB, avg_allocatedVCores

# Load JSON data from file
try:
    with open('applications.json', 'r') as file:
        data = json.load(file)
except Exception as e:
    print(f"Failed to load JSON: {e}")
    sys.exit(1)

# Process applications and write to CSV
filename = 'applications_output.csv'
try:
    doc_list = [doc for doc in data['apps']['app']]

    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Writing header
        header = [
            "application_type", "applicationId", "name", "startTime", "endTime",
            "duration", "applicationTags", "user", "pool", "state",
            "progress", "allocatedMemorySeconds", "allocatedVcoreSeconds",
            "avg_allocatedMB", "avg_allocatedVCores", "runningContainers"
        ]
        csvwriter.writerow(header)

        for doc in doc_list:
            application_type = doc.get('applicationType', '').strip()
            applicationId = doc.get('id', '')
            name = doc.get('name', '')
            startTime = doc.get('startedTime', '')
            endTime = doc.get('finishedTime', '')
            user = doc.get('user', '')
            pool = doc.get('queue', '')
            state = doc.get('state', '')
            progress = doc.get('progress', '')
            allocatedMemorySeconds = doc.get('memorySeconds', '')
            allocatedVcoreSeconds = doc.get('vcoreSeconds', '')
            runningContainers = doc.get('runningContainers', '')
            applicationTags = doc.get('applicationTags', '')

            # Calculate duration and average allocated resources
            duration, avg_allocatedMB, avg_allocatedVCores = calculate_avg_resources(
                startTime, endTime, allocatedMemorySeconds, allocatedVcoreSeconds)

            # Prepare the row
            row = [
                application_type, applicationId, name, startTime, endTime,
                duration, applicationTags, user, pool, state, progress,
                allocatedMemorySeconds, allocatedVcoreSeconds, avg_allocatedMB,
                avg_allocatedVCores, runningContainers
            ]

            # Write the row to the CSV file
            csvwriter.writerow(row)

    print(f'File created -> {filename}')

except Exception as e:
    print('ERROR:', e.__class__.__name__, 'occurred on line', sys.exc_info()[-1].tb_lineno)
    traceback.print_exc()
