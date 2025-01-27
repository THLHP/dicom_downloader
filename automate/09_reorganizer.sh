#!/bin/bash

# Default debug mode is off
DEBUG=false
# Default to process files in parallel
PARALLEL_JOBS=4

# Parse command line arguments
while getopts "dj:q" opt; do
    case $opt in
        d) DEBUG=true ;;
        j) PARALLEL_JOBS=$OPTARG ;;
        q) QUIET=true ;;
        *) echo "Usage: $0 [-d] [-j num_jobs] [-q]" >&2
           exit 1 ;;
    esac
done

# Function to process each DICOM file
process_dicom() {
    local file="$1"
    
    # Extract each tag separately to avoid mixing up values
    local patient_id=$(dcmdump "$file" | grep "(0010,0020)" | sed -n 's/.*\[\([^]]*\)\].*/\1/p')
    local series_desc=$(dcmdump "$file" | grep "(0008,103e)" | sed -n 's/.*\[\([^]]*\)\].*/\1/p')
    local series_uid=$(dcmdump "$file" | grep "(0020,000e)" | sed -n 's/.*\[\([^]]*\)\].*/\1/p')
    
    # Add debug output to verify tag values
    if [ "$DEBUG" = true ]; then
        echo "File: $file"
        echo "Patient ID: $patient_id"
        echo "Series Desc: $series_desc"
        echo "Series UID: $series_uid"
    fi
    
    # Check if we got all required values
    if [ -z "$patient_id" ] || [ -z "$series_desc" ] || [ -z "$series_uid" ]; then
        echo "Error: Missing required DICOM tags for file: $file" >&2
        return 1
    fi
    
    # Clean up series description (remove spaces)
    series_desc=${series_desc// /_}
    
    # Process series UID (keep only parts after the first 6 groups)
    new_series_uid=$(echo "$series_uid" | cut -d'.' -f7-)
    
    # Create the destination path
    local dest_dir="/mnt/blockstorage/dicoms_inprogress/$patient_id/${series_desc}___${new_series_uid}"
    local dest_file="$dest_dir/$(basename "$file")"
    
    if [ "$DEBUG" = true ]; then
        echo "Would move $file to $dest_file"
        echo "-------------------"
    else
        # Use mkdir -p only when needed
        [ ! -d "$dest_dir" ] && mkdir -p "$dest_dir"
        mv "$file" "$dest_file"
        [ "$QUIET" != true ] && echo "Moved $file to $dest_file"
    fi
}

export -f process_dicom
export DEBUG
export QUIET

# Use find with xargs for parallel processing
find /mnt/blockstorage/dicoms_unorganized -type f -name "*.dcm" -print0 | \
    xargs -0 -P "$PARALLEL_JOBS" -I {} bash -c 'process_dicom "{}"'

echo "Processing complete!"