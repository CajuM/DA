#!/usr/bin/env python

import json
import itertools
import math
import sys

def main():
    data = []

    for line in sys.stdin:
        data.append(json.loads(line))

    data.sort(key=lambda row: row['parameters']['iterations'])

    order_ok = True
    num_authorities_ok = True
    num_hubs_ok = True
    max_diff_authorities = 0.0
    max_diff_hubs = 0.0

    for group in itertools.groupby(data, key=lambda row: row['parameters']['iterations']):
        key = group[0]
        first = next(group[1])

        first_authorities = first['results']['authorities']
        first_hubs = first['results']['hubs']

        first_num_authorities = first['dataset']['num_authorities']
        first_num_hubs = first['dataset']['num_hubs']

        first_authorities.sort(key=lambda e: e[0])
        first_hubs.sort(key=lambda e: e[0])

        for row in group[1]:
            row_authorities = row['results']['authorities']
            row_hubs = row['results']['hubs']

            row_authorities.sort(key=lambda e: e[0])
            row_hubs.sort(key=lambda e: e[0])

            row_num_authorities = row['dataset']['num_authorities']
            row_num_hubs = row['dataset']['num_hubs']

            if first_num_authorities != row_num_authorities:
                num_authorities_ok = False

            if first_num_hubs != row_num_hubs:
                num_hubs_ok = False

            for idx in range(len(first_authorities)):
                if first_authorities[idx][0] != row_authorities[idx][0]:
                    order_ok = False

                if first_hubs[idx][0] != row_hubs[idx][0]:
                    order_ok = False

                diff_authorities = math.fabs(first_authorities[idx][1] - row_authorities[idx][1])
                diff_hubs = math.fabs(first_hubs[idx][1] - row_hubs[idx][1])

                if max_diff_authorities < diff_authorities:
                    max_diff_authorities = diff_authorities

                if max_diff_hubs < diff_hubs:
                    max_diff_hubs = diff_hubs

    print(f'oder_ok: {order_ok}; num_hubs_ok: {num_hubs_ok}; num_authorities_ok: {num_authorities_ok}; max_diff_authorities: {max_diff_authorities}; max_diff_hubs: {max_diff_hubs};')
                    
if __name__ == '__main__':
    main()
