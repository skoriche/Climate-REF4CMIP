
# HPCExecutor -  Run REF on HPCs


HPCs generally do not let users run programs and apps that will consume many computational resources on their login nodes.  Users must submit a batch or interactive job to run computationally intensive programs on computer nodes. Therefore, the workflow for running REF on HPCs differs from that on users' local computers or workstations.

Here, we introduce the __HPCExecutor__ for running REF on on HPCs.

You could use HPCExecutor if:
  - The login nodes allow users to run a program for a long time like several hours with little computational resources (less than 25% of one CPU core and negligible memory)
  - You want to run REF under HPC workflow i.e., submitting batch jobs
  - The scheduler on your HPC is __slurm__. We may include other schedules in the future if needed

## Pre-requirements

Before running the REF on compute nodes using HPCExecutor, please pre-install the conda virtual environments and pre-fetch and pre-ingest all data needed by the REF on the login node, because the computer nodes on HPCs generally do not have external internet connections. You could try the scrip at [hpc_executor_prefetch.py](https://github.com/Climate-REF/climate-ref/blob/HPCExecutor/scripts/hpc-executor-prefetch.sh) by changing the REF environment variables to yours on the top of the script to download the data to the correct directories.

Also please clean (delete) everything under ${HOME/.cache/mamba/proc/. These files are used by the minimamba required by esmvaltools and PMP. 

## HPCExecutor

HPCExecutor will use the slurm provider and srun launch from parsl to submit jobs and run REF on computer nodes. The block is the basic computer unit of parsl. It could include a single node or several nodes. Within a node, parsl creates several workers that run a python app across multiple cores. Each worker will compute one or several diagnostics. If the number of diagnostics is larger than the worker number, some workers will run more than one diagnostics serially. However, the more workers, the more overheads brought by parsl. The steps to use HPCExecutor are as follows:

1. On the login nodes, if possible, open a `screen` or `tmux` session to keep the master process of HPCExecutor alive. Only the master process will be run on the login nodes with less than 10% of one CPU core and negligible memory, and real computations will be on computer nodes through the job submissions. If the queue time is short and the connection to the login nodes is stable, you may not need use the session of `screen` and `tmux`.
2. Edit the `ref.toml` under the config directory if not create one.
3. Change the ref executor to HPCExecutor as follows:
```
[executor]  
executor = "climate_ref.executor.HPCExecutor"
```
4. Add the configurations or options for the HPCExecutor based on your system and your account. The example of configuration used in the DOE Perlmutter HPC is as follows:
```
[executor.config]
scheduler = "slurm"  
account = "m2467"  
req_nodes = 1  
walltime = "00:30:00"  
username = "minxu"  
qos = "debug"  
scheduler_options = "#SBATCH -C cpu"  
cores_per_worker = 1  
max_workers_per_node = 64
```


!!! info " "

    If more sbatch derivatives are needed, they could be added in the scheduler_options with `\n` as a separator. There are also other configurations available, for example: `log_dir` to save the parsl log and stderr and stdout from the job; `cores_per_worker` its default is 1, but if the providers use dask or OpenMP, its value can be bigger; `mem_per_worker` generally do not need to set; `max_workers_per_node` to set the maximum worker in a node; `overriders`to add additional options for `srun` 

5. Run `ref solve` in the session of `screen` or `tmux` or directly on the login nodes. It will have a master process running on the login nodes to monitor and submit jobs to run the real diagnostics on computer nodes.
6. Check the logs under the `runinfo` directory and `${REF_CONFIGURATION}/log` to see if there are any errors, once the master process is done. Run ref executions list-groups to check the results or ref executions inspect id to see the error message from the providers.


### Performance benchmarking

Because the parallelism of HPCExecutor now is to distribute the diagnostics computations across the workers (i.e. cores), its performance is generally determined by the runtime of the slowest diagnostic on a single core and the overheads caused by parsl. However, it has good scalability with the increase in the number of diagnostics and fine parallelism (dask, OpenMP, and MPI) implemented in diagnostic providers in the future. So from the following table, the overheads of parsl is small.

#### Single node performance (58 diagnostics)


| HPC     | Executor        | CPUs              | No. of Workers | No. of Cores/Worker | Time (minutes)|
| :-------|---------------- | :-----------------|:-------------: | :-----------------: | ------------: |
| NERSC   | LocalExecutor   | AMD EPYC 7763     | 58             | 1                   | 18.2          | 
| NERSC   | HPCExecutor     | AMD EPYC 7763     | 64             | 1                   | 16.3          |
| NERSC   | HPCExecutor     | AMD EPYC 7763     | 32             | 1                   | 18.1          | 
| NERSC   | HPCExecutor     | AMD EPYC 7763     | 16             | 1                   | 28.6          | 

