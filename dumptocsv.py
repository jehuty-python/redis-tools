#!/bin/env python3

import csv
import time
import argparse
from collections import OrderedDict
from os import getcwd

#### GLOBAL VARS ####
DEBUG = False 
LOCAL_TIME = False
BOTH_TIMES = False
test_string = r'1596001890.037750 [0 w.x.y.z:dddd] "PSETEX" "XXXXX" "600000" "YYYYY"'

#### FUNCTIONS ####
def line_parser(s):
    result = OrderedDict()
    # extract unix timestamp
    unix_time, separator, remainder = s.partition(" ")
    unix_time = float(unix_time)
    if LOCAL_TIME:
        local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(unix_time))
        result.update({"local_time": local_time})
    elif BOTH_TIMES:
        result.update({"unix_time": unix_time})
        local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(unix_time))
        result.update({"local_time": local_time})
    else:
        result.update({"unix_time": unix_time})
    # extract whatever the next variable is before the host:port
    unknown, separator, remainder = remainder.lstrip("[").partition(" ")
    result.update({"unknown": unknown})
    # extract combined host and port data
    host_port, separator, remainder = remainder.partition(" ")
    # clean and split host_port into two ars host & port
    host, separator, port = host_port.rstrip("]").partition(":")
    result.update({"host": host, "port": port})
    # extract database command
    command, separator, remainder = remainder.partition(" ")
    # trim double-quotes
    command = command.replace('"','')
    result.update({"command": command})
    # many commands only have one argument, and therefore should not have any further spaces in a single line entry
    # NOTE: there may still be some edge cases where this logic breaks
    if " " in remainder:
        args = remainder.split(" ")
        args = [ s.replace('"','').lstrip().rstrip() for s in args ]
    else:
        args = [ remainder.rstrip().replace('"','') ] # nest both in lists so args_metadata_extractor
    result.update({"args": args})

    separator, remainder = (None, None)

    return result

def args_metadata_extractor(**kwargs):
    """
    Wrapper script that does the following:
    - creates metadata based on the 'args' keyword
    - purges the 'args' key-value pair from the original collection
    - preserves the other keyword arguments
    """

    try:
        result = OrderedDict(kwargs)

        if kwargs.get('args'):
            assert isinstance(kwargs['args'], list), "'args' must be a list object"
            args_count = len(kwargs['args'])

            longest_len = 0
            longest_pos = None

            for i, arg in enumerate(kwargs['args']):
                if len(arg) > longest_len:
                    longest_len = len(arg)
                    longest_pos = i
            
            # remove args from final result
            result.pop('args')

            # add new metrics
            result.update({"args_count": args_count})
            result.update({"longest_len": longest_len})
            result.update({"longest_pos": longest_pos})

            return result
        else:
            # if 'args' not found for some reason, just pass through kwargs as-is
            return result
    except AssertionError as ex:
        print(ex)
        return None

def csv_writerow(fh, header=False, **kwargs):
    """
    Wrapper function to make writing rows in the csv easier
    """
    try:
        fieldnames = kwargs.keys()
        writer = csv.DictWriter(fh, fieldnames=fieldnames)

        if header:
            writer.writeheader()
        
        kwargs_dict = OrderedDict(kwargs)
        if DEBUG:
            print(kwargs)
        writer.writerow(kwargs_dict)
    except Exception as ex:
        raise ex # not sure how this would break yet, but catch it anyway

def get_args():
    pass

def main(text=None):
    input_path = 'sample.txt'
    with open(input_path, 'r') as input_file:
        output_path = 'output.csv'
        output_file = open(output_path, 'w', newline="")

        count = 0
        #with open(output_path, 'w', newline='') as output_file:
        for line in input_file.readlines():
            header = True if count==0 else False
            count = 1
            stage1_output = line_parser(line)
            stage2_output = args_metadata_extractor(**stage1_output)
            if DEBUG:
                print(stage2_output)
            csv_writerow(output_file, header=header, **stage2_output)

        output_file.close()

if __name__ == '__main__':
    main()