import climate_ref_core.slurm as sl





def test_slurm_checker():

    slurm_checker = sl.SlurmChecker()

    slurm_checker.check_account_partition_access_with_limits("aaa", "bbb")
    pass
