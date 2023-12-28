from datetime import timedelta

import pendulum

from pendulum import UTC, Date, DateTime, Time
from typing import Optional
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

class TwinDateSchedule(Timetable):
    
    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        if run_after.month == run_after.day:
            delta = timedelta(minutes=15)
        else:
            delta = timedelta(hours=1)
        return DataInterval(start=run_after, end=run_after + delta)
    
    
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            if last_start.month == last_start.day: # If previous period started at 6:00, next period will start at 16:30 and end at 6:00 following day
                delta = timedelta(minutes=15)
            else: # If previous period started at 16:30, next period will start at 6:00 next day and end at 16:30
                delta = timedelta(hours=1)
            next_start = last_start
            next_end = last_start + delta
        else:  # This is the first ever run on the regular schedule. First data interval will always start at 6:00 and end at 16:30
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            if last_start.month == last_start.day: # If previous period started at 6:00, next period will start at 16:30 and end at 6:00 following day
                delta = timedelta(minutes=15)
            else: # If previous period started at 16:30, next period will start at 6:00 next day and end at 16:30
                delta = timedelta(hours=1)
            next_end = next_start + delta
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)
    

class UnevenIntervalsTimetablePlugin(AirflowPlugin):
    name = "uneven_intervals_timetable_plugin"
    timetables = [TwinDateSchedule]