#!/usr/bin/env python

import json
import pathlib
import re
import sys

def main(glob):
    for log in pathlib.Path().glob(glob):
        # We messed up the last iteration, the cluster got 56 cores instead of 64
        m = re.match(r'.*/application_\d+_(\d+)/.*', str(log))
        if int(m.group(1)) > 120:
            continue

        row = {}
        in_hubs = False
        in_authorities = False
        with open(log, 'rt') as log:
            for line in log:
                m = re.match(r'\[PageRankBench\] Vertices: ([^;]+);', line)
                if m:
                    row['dataset'] = { 'num_vertices': int(m.group(1)) }

                m = re.match(r'\[PageRankBench\] Edges: ([^;]+);', line)
                if m:
                    row['dataset']['num_edges'] = int(m.group(1))

                m = re.match(r'\[PageRankBench\] ClusterSize: ([^;]+); Dataset: ([^;]+); Function: ([^;]+); Iterations: ([^;]+);', line)
                if m:
                    # Spark's defulatParallelism is max(2, totalCoreCount)
                    cluster_vcpus = int(m.group(1))
                    if cluster_vcpus == 2:
                        cluster_vcpus = 1

                    row['parameters'] = {
                            'cluster_vcpus': cluster_vcpus,
                            'dataset': m.group(2),
                            'function': m.group(3),
                            'iterations': int(m.group(4))
                    }

                m = re.match(r'\[PageRankBench\] Authorities: ([^;]+); Hubs: ([^;]+);', line)
                if m:
                    row['dataset'] = {
                        'num_authorities': int(m.group(1)),
                        'num_hubs': int(m.group(2))
                    }

                m = re.match(r'\[PageRankBench\]   (\d+) (\S+)', line)
                if m:
                    if in_authorities:
                        row['results']['authorities'].append((int(m.group(1)), float(m.group(2))))

                    if in_hubs:
                        row['results']['hubs'].append((int(m.group(1)), float(m.group(2))))

                else:
                    in_hubs = False
                    in_authorities = False

                m = re.match(r'\[PageRankBench\] Authority ranks:', line)
                if m:
                    in_authorities = True
                    row['results'] = { 'authorities': [] }

                m = re.match(r'\[PageRankBench\] Hub ranks:', line)
                if m:
                    in_hubs = True
                    row['results']['hubs'] = []

                m = re.match(r'\[PageRankBench\] Time: (\S+)', line)
                if m:
                    row['time'] = float(m.group(1))

        if row:
            print(json.dumps(row))

if __name__ == '__main__':
    main(sys.argv[1])
