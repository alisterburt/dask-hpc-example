import time

from dask.distributed import Client
from dask.distributed import progress
from dask_jobqueue import SLURMCluster

SCARF_CONFIG = {
    'queue': 'scarf',
    'cores': 24,
    'memory': '128GB',
}

# create a cluster
cluster = SLURMCluster(**SCARF_CONFIG)

# connect a client and scale to 10 workers
client = Client(cluster)
cluster.scale(20)


# some work
def slow_increment(x):
    time.sleep(1)
    return x + 1


futures = client.map(slow_increment, range(5000))
progress(futures)

# print job script
print(cluster.job_script())
