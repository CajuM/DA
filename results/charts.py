#!/usr/bin/env python

import itertools
import json
import sys

import numpy as np

from matplotlib import pyplot as plt

def main():
    data = [json.loads(line) for line in sys.stdin]
    data = [{
        'function': row['parameters']['function'],
        'iterations': row['parameters']['iterations'],
        'vcpus': row['parameters']['cluster_vcpus'],
        'time': row['time']
    } for row in data]

    data.sort(key=lambda row: row['iterations'])

    for key, group in itertools.groupby(data, key=lambda row: row['iterations']):
        group = list(group)

        plt.figure()
        plt.title(f'{key} iteration/s')

        for function in ['mySALSA1', 'mySALSA2', 'mySALSA3']:
            d = [(row['vcpus'], row['time']) for row in group if row['function'] == function]
            d.sort(key=lambda row: row[0])

            x = np.array([row[0] for row in d])
            y = np.array([row[1] for row in d])

            plt.plot(x, y[0] / y, label=function)

        plt.legend()
        plt.savefig(f'scalability-iter-{key}.png')

    data.sort(key=lambda row: row['vcpus'])

    for key, group in itertools.groupby(data, key=lambda row: row['vcpus']):
        group = list(group)

        plt.figure()
        plt.title(f'{key} vCPU/s')

        for function in ['mySALSA1', 'mySALSA2', 'mySALSA3']:
            d = [(row['iterations'], row['time']) for row in group if row['function'] == function]
            d.sort(key=lambda row: row[0])

            x = np.array([row[0] for row in d])
            y = np.array([row[1] for row in d])

            plt.plot(x, y, label=function)

        plt.legend()
        plt.savefig(f'speed-vcpus-{key}.png')

if __name__ == '__main__':
    main()
