#!/bin/bash

snapshots_directory=${1}
output_directory=${2:-"mri_output"}
test_folder="$(pwd)"
mri_exec_folder="mri/target"

if [ ! -d "$snapshots_directory" ]; then
    echo "Directory '$snapshots_directory' does not exist"
    exit 1
fi

echo "Run test without resource limitation"
for file in "$snapshots_directory"/*.snapshot; do
    if [ -f "$file" ]; then
        echo "Processing file: $file"
        echo ""
        sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
        SNAPSHOT_PATH="$file" MRI_OUTPUT_FOLDER_PATH="$test_folder/$output_directory" MRI_EXEC_PATH="$mri_exec_folder" TEST_PATH="$test_folder" docker compose -f docker-compose-simple.yaml up
        docker rm -f -v "$(docker ps -aq)"

    fi
done
echo "Run test with IOPS limitation"
for file in "$snapshots_directory"/*.snapshot; do
    if [ -f "$file" ]; then
        echo "Processing file: $file"
        echo ""
        sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
        SNAPSHOT_PATH="$file" MRI_OUTPUT_FOLDER_PATH="$test_folder/$output_directory" MRI_EXEC_PATH="$mri_exec_folder" TEST_PATH="$test_folder" docker compose -f docker-compose-iops.yaml up
        docker rm -f -v "$(docker ps -aq)"
    fi
done

echo "gather docker tests results"
echo "${output_directory}/simple/load_time.csv"
cat "${output_directory}/simple/load_time.csv"
echo "${output_directory}/limit_iops/load_time.csv"
cat "${output_directory}/limit_iops/load_time.csv"

results_folder="tests_results"
rm -rf $results_folder && mkdir $results_folder
tail "${output_directory}/simple/load_time.csv" >> "${results_folder}/load_time.csv"
tail -n +2 "${output_directory}/limit_iops/load_time.csv" >> "${results_folder}/load_time.csv"
sudo rm "${output_directory}/simple/load_time.csv" "${output_directory}/limit_iops/load_time.csv"
sudo cp "${output_directory}"/simple/* "${results_folder}"
sudo cp "${output_directory}"/limit_iops/* "${results_folder}"

echo "Remove docker tests folders"
sudo rm -rf "${output_directory}"