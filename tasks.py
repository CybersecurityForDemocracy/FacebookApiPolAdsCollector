from invoke import task

from c4d_utils.jobs import CronJob


@task
def fetch_crowdtangle(c, config):
    with CronJob(c, venv='crowdtangle/venv') as j:
        j.run_python(f'-m crowdtangle.run_fetch_crowdtangle --config_path {config} --runner DirectRunner --direct_num_workers=0 --direct_running_mode=multi_threading')
