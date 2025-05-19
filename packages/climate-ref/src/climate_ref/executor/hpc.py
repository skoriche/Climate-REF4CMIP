"""This is HPC-based Executor to use job schedulers.

  If you want to 
     - run REF under the HPC workflows
     - run REF in multiple nodes

"""
# mypy: disable-error-code="import-untyped"
from typing import Any
from loguru import logger

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.models import Execution
from climate_ref_core.diagnostics import ExecutionDefinition, ExecutionResult
from climate_ref_core.exceptions import ExecutionError
from climate_ref_core.executor import execute_locally
from climate_ref_core.logging import add_log_handler
from climate_ref_core.slurm import SlurmChecker
import parsl

from parsl.config import Config as ParslConfig
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl import python_app


@python_app
def _process_run(definition: ExecutionDefinition, log_level: str) -> ExecutionResult:
    # This is a catch-all for any exceptions that occur in the process
    try:
        return execute_locally(definition=definition, log_level=log_level)
    except Exception:  # pragma: no cover
        # This isn't expected but if it happens we want to log the error before the process exits
        logger.exception("Error running diagnostic")
        # This will kill the process pool
        raise

class HPCExecutor:
    """
    Run diagnostics by submitting a job script

    """

    name = "hpc"

    def __init__( 
        self,
        *, 
        database: Database | None = None,
        config: Config | None = None,
        scheduler: str = "slurm", 
        account: str,
        partition: str | None,
        qos: str | None, 
        nodes: int, 
        walltime: str, 
        log_dir: str = "slurm.log",
    ) -> None:

        if config is None:
            config = Config.default()
        if database is None:
            database = Database.from_config(config, run_migrations=False)

        self.config = config
        self.database = database

        self.scheduler = scheduler
        self.account = account
        self.partition = partition
        self.qos = qos
        self.nodes = nodes
        self.walltime = walltime
        self.log_dir = log_dir

   

        # pyslurm to check the node configurations

        slurm_checker = SlurmChecker()
        if not slurm_checker.get_account_info(self.account):
            raise ValueError(f"Account: {self.account} not valid")

        if partition is not None:
            if not slurm_checker.get_partition_info(partition):
                raise ValueError(f"Partition: {partition} not valid")
              
            if not slurm_checker.can_account_use_partition(
                account, 
                partition):
                raise ValueError(f"Account: {account} cannot access partiton: {partition}")
            partition_limits = slurm_checker.get_partition_limits(partition)
            node_info =  slurm_checker.get_node_from_partition(partition)
        else:
            partition_limits = None
            node_info = None

        if qos is not None:
            if not slurm_checker.get_partition_info(qos):
                raise ValueError(f"QOS: {qos} not valid")

            if not slurm_checker.can_account_use_qos(
                account, 
                qos):
                raise ValueError(f"Account: {account} cannot access qos: {qos}")
            qos_limits =  slurm_checker.get_qos_limits(qos)
        else:
            qos_limits = None



        max_cores_per_node = float(node_info["cpus"]) if node_info else 64

        max_walltime_partition = partition_limits["max_time_minutes"] if partition_limits else walltime
        max_walltime_qos = qos_limits["max_time_minutes"] if qos_limits else walltime

        max_walltime_minutes  = min(max_walltime_partition, max_walltime_qos)

        hours, minutes, seconds = map(int, walltime.split(':'))
        total_minutes = hours * 60 + minutes + seconds / 60

        if total_minutes > float(max_walltime_minutes):
            raise ValueError(f"walltime: {walltime} exceed the maximum time " + 
                             f"{max_walltime_minutes} allowed by {partition} and {qos}")


        slurm_config = ParslConfig(
            executors=[
                HighThroughputExecutor(
                    label = 'ref_hpc_executor',
                    cores_per_worker = max_cores_per_node,
                    provider=SlurmProvider(
                        account = self.account, 
                        partition = self.partition,
                        qos = self.qos,
                        nodes_per_block = self.nodes,
                        scheduler_options = '#SBATCH -C cpu',
                        worker_init='uv .venv/bin/activate',
                        launcher=SrunLauncher(),
                        walltime=self.walltime,
                        cmd_timeout=120,
                    ),
                )
            ]
        )
        parsl.load(slurm_config)
   
        self.parsl_results: list[Any] = []


    def run(
        self,
        definition: ExecutionDefinition,
        execution: Execution | None = None,
    ) -> None:
        """
        Run a diagnostic in process

        Parameters
        ----------
        definition
            A description of the information needed for this execution of the diagnostic
        execution
            A database model representing the execution of the diagnostic.
            If provided, the result will be updated in the database when completed.
        """
        # Submit the execution to the process pool
        # and track the future so we can wait for it to complete
        result = _process_run(
            definition=definition,
            log_level=self.config.log_level,
        )

    def join(self) -> None:
        pass
