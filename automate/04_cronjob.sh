#!/bin/bash

# using curl (10 second timeout, retry up to 5 times):
curl -m 10 --retry 5 https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/dicoms_pacs_to_chile/start

# Start the first instance of the script
echo "Starting first instance of the script"
/root/miniconda3/envs/sql/bin/python /root/dicom_downloader/automate/04_db_downloading_dicoms.py &
pid1=$!

# Wait for 5 seconds
sleep 5

# Start the second instance of the script
echo "Starting second instance of the script"
/root/miniconda3/envs/sql/bin/python /root/dicom_downloader/automate/04_db_downloading_dicoms.py &
pid2=$!

# Wait for another 5 seconds
sleep 5

# Start the third instance of the script
echo "Starting third instance of the script"
/root/miniconda3/envs/sql/bin/python /root/dicom_downloader/automate/04_db_downloading_dicoms.py &
pid3=$!

# Wait for all scripts to finish
wait $pid1
status1=$?
wait $pid2
status2=$?
wait $pid3
status3=$?

# Check exit statuses
if [ $status1 -ne 0 ]; then
  echo "First instance of the script exited with a non-zero status: $status1"
fi

if [ $status2 -ne 0 ]; then
  echo "Second instance of the script exited with a non-zero status: $status2"
fi

if [ $status3 -ne 0 ]; then
  echo "Third instance of the script exited with a non-zero status: $status3"
fi

# Optionally, you can add logic here to handle non-zero exit statuses, such as exiting the script with an error code
if [ $status1 -ne 0 ] || [ $status2 -ne 0 ] || [ $status3 -ne 0 ]; then
  # using curl (10 second timeout, retry up to 5 times):
  curl -m 10 --retry 5 https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/dicoms_pacs_to_chile/1
  exit 1
fi

echo "All instances of the script completed successfully"

# using curl (10 second timeout, retry up to 5 times):
curl -m 10 --retry 5 https://hc-ping.com/hLLDZXOBn0N0ABea5bVJKQ/dicoms_pacs_to_chile/$?
