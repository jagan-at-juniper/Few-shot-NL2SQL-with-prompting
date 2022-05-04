import argparse
import glob
import os
import sys
from pathlib import Path

import pandas as pd


def parse_aws_logs(input_dir, file_name, output_dir, desired_field):
    input_file_path = os.path.join(input_dir, file_name)
    log_file_df = pd.read_csv(input_file_path)
    if desired_field in log_file_df.columns:
        logs = list(log_file_df[log_field])
        output_file_name = str(Path(file_name).stem) + ".txt"
        output_file_path = os.path.join(output_dir, output_file_name)
        with open(output_file_path, 'w') as file:
            file.writelines("%s" % log for log in logs)
        print('Stored the logs at "{}".'.format(output_file_path))
    else:
        print('The file "{}" does not contain the field "{}".'.format(input_file_path, desired_field))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        type=str,
                        help='The directory where the log file exists. Leave blank for current directory.')
    parser.add_argument('-f',
                        type=str,
                        nargs='+',
                        help='The .csv log files to be parsed. Leave blank to parse all ".csv" files.')
    parser.add_argument('-o',
                        type=str,
                        help='The output directory. Leave blank for current directory.')
    parser.add_argument('-m',
                        type=str,
                        help='The field to be extracted from the logs. Leave blank for "@message".')

    args = parser.parse_args()

    input_dir = args.d
    if input_dir:
        if not os.path.isdir(input_dir):
            print('The directory "{}" does not exist.'.format(input_dir))
            sys.exit()
    else:
        input_dir = os.getcwd()

    input_files = args.f

    files_to_parse = list()
    if input_files:
        for file in input_files:
            file_path = os.path.join(input_dir, file)
            if os.path.isfile(file_path):
                if Path(file).suffix == '.csv':
                    files_to_parse.append(file)
                else:
                    print('The file "{}" is not a .csv file.'.format(file_path))
            else:
                print('The file path "{}" does not exist.'.format(file_path))
    else:
        files_to_parse = glob.glob(input_dir + "/*.csv")

    if not files_to_parse:
        print('No .csv files found within "{}".'.format(input_dir))
    else:
        output_dir = args.o
        if output_dir:
            if not os.path.isdir(output_dir):
                print('The directory "{}" does not exist.'.format(output_dir))
                sys.exit()
        else:
            output_dir = os.getcwd()

        log_field = args.m
        if not log_field:
            log_field = '@message'

        for file in files_to_parse:
            print("Parsing {}...".format(os.path.join(input_dir, file)))
            parse_aws_logs(input_dir, file, output_dir, log_field)
