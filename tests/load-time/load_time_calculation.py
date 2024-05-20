import argparse
import csv
import os
import re
import shutil
import statistics
import subprocess
import sys

from datetime import datetime


parser = argparse.ArgumentParser(description='Run tests and calculate load time.')

parser.add_argument('--mri', nargs='?', type=str, default="./mri/target/debug/mri", help='mri exec')
parser.add_argument('--mri-output', nargs='?', type=str, default="mri_output", help='path to mri output folder')
parser.add_argument('--mri-output-format', nargs='?', default="jsonl", type=str, help='mri output file format')
parser.add_argument('--qdrant', nargs='?', type=str, default="./target/debug/qdrant", help=' qdrant exec')
parser.add_argument('--test-prefix', nargs='?', type=str, default=None, help='test name prefix')
parser.add_argument('--snapshots', nargs='+', type=str, help='path to snapshot(s), or to a folder with snapshots')
args = parser.parse_args()

MRI_EXEC = args.mri
MRI_OUTPUT_FORMAT = args.mri_output_format if args.mri_output_format in ["jsonl", "html", "csv"] else "jsonl"
QDRANT_EXEC = args.qdrant
# MRI_METRICS = "-a status.rssanon -a io.read_bytes -a io.write_bytes -r stat.utime"
MRI_METRICS = "-a status.rssanon -r io.read_bytes -r io.write_bytes -r stat.utime"
MRI_OTHER_ARGS = "--relative-time -d 10 -i 2"
MRI_OUTPUT_FOLDER = args.mri_output
TEST_PREFIX = args.test_prefix
NUMBER_OF_LOOPS = 1


def get_snapshots_list(input_value):
    if not input_value or len(input_value) == 0:
        raise ValueError("Too few values found in snapshots")

    file_list = []
    if len(input_value) > 1:
        # if list of snapshots provided
        file_list = input_value
    elif os.path.isfile(input_value[0]):
        # if 1 snapshot provided
        file_list.append(input_value[0])
    else:
        # if 1 folder with snapshots provided
        for root, dirs, files in os.walk(input_value[0]):
            for file in files:
                if file.endswith(".snapshot"):
                    file_list.append(os.path.join(root, file))

    return [x + ":benchmark" for x in file_list]


def show_progress(count, total: int):
    # Calculate progress percentage
    progress = (count + 1) / total
    progress_bar_length = 50
    progress_bar = '[' + '=' * int(progress * progress_bar_length) + \
                   ' ' * (progress_bar_length - int(progress * progress_bar_length)) + \
                   ']'
    sys.stdout.write(f"\rProgress: {progress_bar} {progress * 100:.2f}%")
    sys.stdout.flush()


def calculate_load_time(start_time_str, end_time_str):
    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
    # Convert to Unix timestamp (in seconds) and then to milliseconds
    start_time_ms = int(start_time.timestamp() * 1000)

    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
    end_time_ms = int(end_time.timestamp() * 1000)
    return end_time_ms - start_time_ms


def remove_folder(folder_path):
    try:
        shutil.rmtree(folder_path)
        # print(f"Directory '{folder_path}' removed successfully.")
    except FileNotFoundError:
        print(f"Directory '{folder_path}' does not exist.")
    except Exception as e:
        print(f"Error occurred while removing directory '{folder_path}': {e}")


def clean_up_qdrant_run(parent_folder: str = None):
    if not parent_folder:
        # remove folder from current folder
        parent_folder = os.getcwd()

    directories_to_remove = ["storage", "snapshots"]
    for directory in directories_to_remove:
        folder_path = os.path.join(parent_folder, directory)
        remove_folder(folder_path)


def convert_to_milliseconds(timestamp):
    dt = datetime.fromisoformat(timestamp[:-1])  # Removing the 'Z' at the end
    return int(dt.timestamp() * 1000)


def run_mri_and_get_timestamps(qdrant_cmd, usage_output_file, keep_data: bool = True):
    # shell_command = f'{MRI_EXEC} {MRI_METRICS} --html "{usage_output_file}" -c "{qdrant_cmd}" {MRI_OTHER_ARGS}'
    if usage_output_file:
        shell_command = f'QDRANT__LOG_LEVEL=debug {MRI_EXEC} {MRI_METRICS} --{MRI_OUTPUT_FORMAT} "{usage_output_file}" -c "{qdrant_cmd}" {MRI_OTHER_ARGS}'
    else:
        shell_command = f'{MRI_EXEC} {MRI_METRICS} -c "{qdrant_cmd}" -d 30'
    process = subprocess.Popen(shell_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    timestamp_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+.+"
    regex = re.compile(timestamp_pattern)

    timestamps = []
    timestamped_lines = []
    all_lines = []
    for line in process.stdout:
        decoded_line = line.decode().rstrip()
        all_lines.append(decoded_line)
        # Only take those lines that start with the timestamp
        if regex.match(decoded_line):
            timestamped_lines.append(decoded_line)
            timestamp = decoded_line.split(" ")[0]
            timestamps.append(timestamp)

    if usage_output_file:
        with open(f"{usage_output_file.replace(MRI_OUTPUT_FORMAT, 'log')}", "w") as f:
            f.writelines('\n'.join(timestamped_lines))

    return_code = process.wait()

    if not keep_data:
        clean_up_qdrant_run()

    if return_code != 0:
        clean_up_qdrant_run()
        print(f"Mri run return code: {return_code}")
        exit(-1)
    return timestamps


def store_load_time(file_name, test_name, load_time):
    skip_header = False
    if os.path.exists(file_name):
        skip_header = True

    with open(file_name, "a") as f:
        fieldnames = ['item', 'load_time']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not skip_header:
            writer.writeheader()
        writer.writerow({'item': test_name, 'load_time': load_time})


def run_tests(test_name, output_folder, cmd_under_test, keep_storage):
    load_times = []
    if NUMBER_OF_LOOPS > 1:
        for i in range(NUMBER_OF_LOOPS):
            output_file = f"{output_folder}/{test_name}_{i}.{MRI_OUTPUT_FORMAT}"
            output_lines = run_mri_and_get_timestamps(cmd_under_test, output_file, keep_storage)
            load_times.append(calculate_load_time(output_lines[0], output_lines[-1]))
            show_progress(i, NUMBER_OF_LOOPS)
            median = statistics.median(load_times)
            st_dev = statistics.stdev(load_times)
            result = [
                f"\nLoad times: {load_times}\n",
                f"Median: {median} ms\n",
                f"St Deviation: {st_dev} ms\n"
            ]
            print(''.join(result))
            with open(f"{output_folder}/{test_name}.txt", "w") as f:
                f.writelines(result)
            store_load_time(f"{output_folder}/load_time.csv", test_name, median)
    else:
        output_file = f"{output_folder}/{test_name}.{MRI_OUTPUT_FORMAT}"
        output_lines = run_mri_and_get_timestamps(cmd_under_test, output_file, keep_storage)
        result = calculate_load_time(output_lines[0], output_lines[-1])
        print(f"Load time: {result}")
        store_load_time(f"{output_folder}/load_time.csv", test_name, result)


def load_time_simple_reload(init_snapshot) -> None:
    """
    Case when we re-start Qdrant using existing storage/ folder.
    @param init_snapshot: path to snapshot file to initialize storage/ folder.
    @return: None
    """
    def parse_string(init_snapshot):
        parts = init_snapshot.split('/')
        last_part = parts[-1]
        return last_part.split(':')[0].replace('.snapshot', '')

    if TEST_PREFIX:
        test_name = f'{TEST_PREFIX}_{parse_string(init_snapshot)}'
    else:
        test_name = parse_string(init_snapshot)

    cmd_under_test = f'{QDRANT_EXEC} --snapshot {init_snapshot} --force-snapshot'
    print(f"Initialize storage/ folder with {init_snapshot}")
    # run_mri_and_get_timestamps(cmd_under_test, f"{MRI_OUTPUT_FOLDER}/{test_name}_init.{MRI_OUTPUT_FORMAT}", True)
    run_mri_and_get_timestamps(cmd_under_test, None, True)

    print(f"Run tests for {init_snapshot}...")
    # cmd_under_test = f'RUST_LOG=debug {QDRANT_EXEC}'
    cmd_under_test = QDRANT_EXEC
    run_tests(test_name, MRI_OUTPUT_FOLDER, cmd_under_test, True)


def run_tests_and_calculate_load_times():
    snapshots_under_test = get_snapshots_list(args.snapshots)
    for item in snapshots_under_test:
        print(f"Execute tests for {item}")
        load_time_simple_reload(item)
        clean_up_qdrant_run()


if __name__ == "__main__":
    run_tests_and_calculate_load_times()
