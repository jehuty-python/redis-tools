#!/bin/env python3

import csv
import sys
import time
import subprocess
import json
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
    # handle artifacts created by Bash output
    if s.strip() in ("OK", ""):
        result.update({
            "unix_time": 0.0, "local_time": "",
            "unknown":"", "host": "", "port": 0,
            "command":"", "args":""
        })
        return result
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
    if remaind.strip() == "":
        args = [""]
    elif " " in remainder:
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

def get_pod_list():
    """
    No arguments
    Assumptions:
    - Openshift/OKD origin cli tool is installed on system
    - User running script has appropriate permissions on k8s/Openshift cluster
    """
    params = ['oc','get','pods','--all-namespaces','-o json']
    cmd = " ".join(params)
    result = json.loads(subprocess.check_output(cmd, shell=True))
    return result

def unpack_pod_ips(raw_data):
    try:
        ip_to_name_map = {}
        assert raw_data.get("items"), "Empty or malformed response from server, expecting dict with key 'items'"
        for item in raw_data["items"]:
            if item.get('status') and item['status'].get('podIP'):
                ip_to_name_map.update({
                    item["status"]["podIP"]: item["metadata"]["name"]
                })
            else:
                if DEBUG:
                    print("No podIP found for {}".format(item["metadata"]["name"]))
    except AssertionError as ex:
        print(ex)
        return None


def ip_resolver(mapping, **kwargs):
    """
    Always returns dict() of keyword args back to caller.
    Results will be enriched if any new data is found.

    Wrapper scipt that does the following:
    - resolve any "host" key provided in kwargs to PodName found in ip_to_name_map
    - preserves all other keyword args
    """
    result = OrderedDict(kwargs)

    # add new podName key to result if host/IP found in ip_to_name_map
    if kwargs.get('host') and mapping.get(kwargs['host']):
        result['podName'] = mapping[kwargs['host']]
    else:
        result['podName'] = "NotFound"
    return result


def csv_writerow(fh, header=False, **kwargs):
    """
    Wrapper function to make writing rows in the csv easier
    """
    try:
        fieldnames = kwargs.keys()
        writer = csv.DictWriter(fh, fieldnames=fieldnames)

        if header:
            writer.writeheader()
        
        if DEBUG:
            print(kwargs)
        writer.writerow(OrderedDict(kwargs))
    except Exception as ex:
        raise ex # not sure how this would break yet, but catch it anyway

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-f', '--file',
        type=str,
        help="specify input file"
    )

    parser.add_argument(
        '-o','--out',
        type=str,
        help="specify output file"
    )

    parser.add_argument(
        '-P','--pod-lookup',
        action='store_true',
        default=False,
        help="resolves IPs in raw to pod hostnames"
    )

    parser.add_argument(
        '--version',
        action='version',
        version="""
            %(prog)s 1.2,
            Author: Aaron C. Robinett;
            Last Updated 2020-12-10
            """
    )

    parsed_args = parser.parse_args()

    return parsed_args

def main():
    args = get_args()

    if args.file:
        input_path = args.file
    else:
        input_path = 'sample.txt'
    
    if args.out:
        output_path = args.out
    else:
        output_path = 'output.csv'
    
    if args.pod_lookup:
        ip_to_name_map = unpack_pod_ips(get_pod_list())

    with open(input_path, 'r') as input_file:
        with open(output_path, 'w', newline="") as output_file:
            count = 0
            for line in input_file.readlines():
                header = True if count==0 else False
                count = 1
                stage1_output = line_parser(line)
                stage2_output = args_metadata_extractor(**stage1_output)
                if args.pod_lookup:
                    stage3_output = ip_resolver(ip_to_name_map, **stage2_output)
                    csv_writerow(output_file, header=header, **stage3_output)
                else:
                    csv_writerow(output_file, header=header, **stage2_output)

if __name__ == '__main__':
    main()