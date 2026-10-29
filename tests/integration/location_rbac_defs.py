"""Two tiny code locations for the location-scoped RBAC end-to-end test."""

from dagster import Definitions, ScheduleDefinition, asset, job, op


@op
def noop():
    pass


@job
def job_a():
    noop()


@job
def job_b():
    noop()


@asset
def asset_a():
    return 1


@asset
def asset_b():
    return 1


defs_a = Definitions(
    assets=[asset_a],
    jobs=[job_a],
    schedules=[ScheduleDefinition(name="schedule_a", job=job_a, cron_schedule="0 * * * *")],
)
defs_b = Definitions(
    assets=[asset_b],
    jobs=[job_b],
    schedules=[ScheduleDefinition(name="schedule_b", job=job_b, cron_schedule="0 * * * *")],
)
