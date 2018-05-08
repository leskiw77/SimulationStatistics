import argparse
import csv
import re
import subprocess
import sys
from os import listdir, path


class ExecutionTimeMeasurement:
    def __init__(self):
        self.logfile_regex = re.compile("Run time:\s+(\d+)\s+hours?\s+(\d+)\s+minutes?\s+(\d+)\s+seconds?")
        self.time_command_regex = re.compile("real\s+(\d+)\.(\d)+")

    def get_submit_statistics_for_log_directory(self, log_directory='workspace/log'):
        log_files = [path.join(log_directory, f) for f in listdir(log_directory) if
                     path.isfile(path.join(log_directory, f)) and f.startswith('output_')]

        time_tuple = self.collect_time_info(log_files)

        time_in_seconds = [3600 * h + 60 * m + s for h, m, s in time_tuple]

        return {'minimum': min(time_in_seconds), 'maximum': max(time_in_seconds),
                'average': sum(time_in_seconds) / float(len(time_in_seconds))}

    def collect_time_info(self, log_files):
        results = []
        for file in log_files:
            res = self.parse_file(file)
            if res is None:
                print('File {} do not provide run time'.format(file))
            else:
                results.append(res)
        return results

    def parse_file(self, file_name):
        with open(file_name) as f:
            for line in f:
                result = self.logfile_regex.search(line)
                if result is not None:
                    hours = int(result.group(1))
                    minutes = int(result.group(2))
                    seconds = int(result.group(3))
                    return hours, minutes, seconds

    def get_submit_statistics_for_workspace(self, workspace_directory='.'):
        run_directories = Utils.get_run_directories(workspace_directory)

        results = []

        for run_dir in run_directories:
            run_result_map = self.get_submit_statistics_for_log_directory(run_dir + '/workspace/log')
            run_result_map['directory'] = path.basename(path.normpath(run_dir))
            results.append(run_result_map)

        return results

    def append_collect_time(self, results, workspace_directory='.'):

        command = 'convertmc image --many "{}" {}'
        for result in results:
            current_directory = workspace_directory + '/' + result['directory']
            converter_input = current_directory + '/output/*'
            png_output = current_directory + '/png_output/'
            collect_time = self.calculate_command_time(command.format(converter_input, png_output))
            result['collect'] = collect_time

    def get_statistics_for_workspace(self, workspace_directory='.', measure_collect=False, csv_filename=None):
        results = self.get_submit_statistics_for_workspace(workspace_directory)

        fieldnames = ['directory', 'minimum', 'maximum', 'average']
        if measure_collect:
            self.append_collect_time(results, workspace_directory)
            fieldnames.append('collect')

        if csv_filename:
            self.save_results_to_csv(csv_filename, results, fieldnames)
        else:
            for record in results:
                print(record)

    def save_results_to_csv(self, csv_filename, results, fieldnames):
        with open(csv_filename, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            writer.writeheader()
            for record in results:
                writer.writerow(record)

    def calculate_command_time(self, command):
        time_command = 'time -p ' + command
        print('Run command: {}'.format(time_command))
        p = subprocess.Popen([time_command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        output = p.stderr.read().decode('utf-8')
        p.communicate()
        result = self.time_command_regex.search(output)

        # TODO: add better error handling
        if result is None:
            raise Exception(output)

        return float(result.group(1) + '.' + result.group(2))


class ScriptsRunner:
    def submit_all(self, workspace):
        self.take_common_action(workspace, '/submit.sh')

    def collect_all(self, workspace):
        self.take_common_action(workspace, '/collect.sh')

    @staticmethod
    def take_common_action(workspace, action):
        print(workspace)
        run_directories = Utils.get_run_directories(workspace)

        for run_directory in run_directories:
            print("Run command: " + run_directory + action)
            p = subprocess.Popen([run_directory + action])
            p.communicate()


class Utils:
    @staticmethod
    def get_run_directories(workspace_directory):
        return [path.join(workspace_directory, f) for f in listdir(workspace_directory) if
                path.isdir(path.join(workspace_directory, f)) and f.startswith('run_')]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('workspace',
                        type=str,
                        help='workspace directory, directory with many run_*_* directories')

    parser.add_argument('-csv',
                        type=str,
                        help='save results to csv',
                        nargs=1)

    parser.add_argument('-submit', '--submit_all',
                        action='store_true',
                        help='process submit script in every run directory')

    parser.add_argument('-collect', '--collect_all',
                        action='store_true',
                        help='process collect script in every run directory')

    parser.add_argument('-stats', '--statistics',
                        action='store_true',
                        help='output contains collect time info')

    parser.add_argument('-ct', '--collect_time',
                        action='store_true',
                        help='output contains collect time info')

    args = parser.parse_args(sys.argv[1:])

    workspace = args.workspace

    if args.submit_all:
        ScriptsRunner().submit_all(workspace)
    elif args.collect_all:
        ScriptsRunner().collect_all(workspace)
    elif args.statistics:
        csv_file = None
        if args.csv:
            csv_file = args.csv[0]
        ExecutionTimeMeasurement().get_statistics_for_workspace(workspace, args.collect_time, csv_file)
    else:
        raise argparse.ArgumentError('No option provided')
