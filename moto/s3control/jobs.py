import csv
import datetime
import io
import time
import traceback
import uuid
from copy import copy
from enum import Enum
from functools import cached_property
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

from moto.core.common_models import BaseModel
from moto.moto_api._internal.managed_state_model import ManagedState
from moto.s3.exceptions import MissingBucket
from moto.s3.models import s3_backends

from .exceptions import InvalidJobOperation, ValidationError

restoration_delay_in_seconds = 60

JOBS_RETENTION_TIME_IN_MINUTES = 15


def log(msg):
    dt = datetime.datetime.now()
    dt_str = dt.strftime("%d/%b/%Y %H:%M:%S")
    print(f"[{dt_str}] s3control jobs: {msg}")  # noqa: T201


def set_restoration_delay(value_in_seconds):
    global restoration_delay_in_seconds
    restoration_delay_in_seconds = value_in_seconds


def validate_job_status(target_job_status: str, valid_job_statuses: List[str]) -> None:
    if target_job_status not in valid_job_statuses:
        raise ValidationError(
            (
                "1 validation error detected: Value at 'current_status' failed "
                "to satisfy constraint: Member must satisfy enum value set: {valid_statues}"
            ).format(valid_statues=valid_job_statuses)
        )


# https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-job-status.html
class JobStatus(str, Enum):
    ACTIVE = "Active"
    CANCELLED = "Cancelled"
    CANCELLING = "Cancelling"
    COMPLETE = "Complete"
    COMPLETING = "Completing"
    FAILED = "Failed"
    FAILING = "Failing"
    NEW = "New"
    PAUSED = "Paused"
    PAUSING = "Pausing"
    PREPARING = "Preparing"
    READY = "Ready"
    SUSPENDED = "Suspended"

    @classmethod
    def job_statuses(cls) -> List[str]:
        return sorted([item.value for item in JobStatus])

    @classmethod
    def status_transitions(cls) -> List[Tuple[Optional[str], str]]:
        return [
            (JobStatus.NEW.value, JobStatus.PREPARING.value),
            (JobStatus.PREPARING.value, JobStatus.SUSPENDED.value),
            (JobStatus.SUSPENDED.value, JobStatus.READY.value),
            (JobStatus.PREPARING.value, JobStatus.READY.value),
            (JobStatus.READY.value, JobStatus.ACTIVE.value),
            (JobStatus.PAUSING.value, JobStatus.PAUSED.value),
            (JobStatus.CANCELLING.value, JobStatus.CANCELLED.value),
            (JobStatus.FAILING.value, JobStatus.FAILED.value),
            (JobStatus.ACTIVE.value, JobStatus.COMPLETE.value),
        ]


class JobDefinition:
    def __init__(
        self,
        account_id: str,
        description: str,
        partition: str,
        operation_name: str,
        operation_definition: Dict[str, Any],
        params: Dict[str, Any],
        manifest_arn: str,
        manifest_etag: str | None,
        manifest_format: str,
        manifest_fields: str,
        report_bucket: str | None,
        report_enabled: bool,
        report_format: str | None,
        report_prefix: str | None,
        report_scope: str | None,
    ):
        self.account_id = account_id
        self.description = description
        self.partition = partition
        self.operation_name = operation_name
        self.operation_definition = operation_definition
        self.params = params
        self.manifest_arn = manifest_arn
        self.manifest_etag = manifest_etag
        self.manifest_format = manifest_format
        self.manifest_fields = manifest_fields
        self.report_bucket = report_bucket
        self.report_enabled = report_enabled
        self.report_format = report_format
        self.report_prefix = report_prefix
        self.report_scope = report_scope

    @classmethod
    def from_dict(
        cls, account_id: str, partition: str, params: Dict[str, Any]
    ) -> "JobDefinition":
        operation_tuple = list(params["Operation"].items())[0]
        operation_name = operation_tuple[0]
        operation_params = operation_tuple[1]
        description = params.get("Description", "Description not given")
        manifest = params["Manifest"]
        manifest_location = manifest["Location"]
        manifest_location_etag = manifest_location.get("ETag")
        manifest_location_object_arn = manifest_location.get("ObjectArn")
        manifest_spec = manifest["Spec"]
        manifest_fields = manifest_spec["Fields"]
        if isinstance(manifest_fields, dict) and "member" in manifest_fields:
            manifest_fields = manifest_fields["member"]
        manifest_format = manifest_spec["Format"]
        report = params["Report"]
        report_bucket = report.get("Bucket")
        report_enabled = report.get("Enabled")
        report_format = report.get("Format")
        report_prefix = report.get("Prefix")
        report_scope = report.get("ReportScope")
        operation_definition = params["Operation"]
        if report_enabled is not None:
            report_enabled = report_enabled.lower() == "true"
        return JobDefinition(
            account_id=account_id,
            description=description,
            partition=partition,
            operation_name=operation_name,
            operation_definition=operation_definition,
            params=operation_params,
            manifest_arn=manifest_location_object_arn,
            manifest_etag=manifest_location_etag,
            manifest_format=manifest_format,
            manifest_fields=manifest_fields,
            report_bucket=report_bucket,
            report_enabled=report_enabled,
            report_format=report_format,
            report_prefix=report_prefix,
            report_scope=report_scope,
        )

    @property
    def manifest_bucket_name(self):
        return self.manifest_arn.split("/")[0].split(":")[-1]

    @property
    def manifest_path(self):
        return "/".join(self.manifest_arn.split("/")[1:])

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.__dict__}"


class JobExecutor(Thread):
    OPERATION = None

    def __init__(self, job: "Job"):
        Thread.__init__(self)
        self.definition = job.definition
        self.job = job
        self.stop_requested = False

    @classmethod
    def create_executor(cls, job: "Job"):
        classes = [
            subclazz
            for subclazz in cls.__subclasses__()
            if subclazz.OPERATION == job.definition.operation_name
        ]
        if classes:
            return classes[0](job)
        else:
            raise InvalidJobOperation(
                f"Unsupported operation: {job.definition.operation_name}"
            )

    @cached_property
    def _bucket_index_in_csv(self):
        return self.definition.manifest_fields.index("Bucket")

    @cached_property
    def _key_index_in_csv(self):
        return self.definition.manifest_fields.index("Key")

    def _buckets_and_keys_from_csv(self, file_obj):
        stream = io.StringIO(file_obj.value.decode(encoding="utf-8"))
        for row in csv.reader(stream):
            yield row[self._bucket_index_in_csv], row[self._key_index_in_csv]


class Job(BaseModel, ManagedState):
    def __init__(self, job_id, definition: JobDefinition):
        ManagedState.__init__(
            self,
            "s3control::job",
            JobStatus.status_transitions(),
        )

        self.job_id = job_id
        self.definition = definition
        self.creation_time = datetime.datetime.now()
        self.finish_time = None
        self.failure_reasons = []
        self.number_of_tasks_succeeded = 0
        self.number_of_tasks_failed = 0
        self.total_number_of_tasks = 0
        self.elapsed_time_in_active_seconds = 0
        self.executor = JobExecutor.create_executor(self)

    def start(self):
        self.executor.start()

    def stop(self):
        if not self.executor:
            return
        self.executor.stop_requested = True
        log(f"joining executor for {self.job_id}")
        self.executor.join(timeout=2)
        if not self.executor.is_alive():
            log(f"releasing executor for {self.job_id}")
            self.executor = None

    def try_cleanup(self):
        if not self.executor:
            return
        if not self.executor.is_alive():
            log(f"joining executor for {self.job_id}")
            self.executor.join(timeout=2)
        if not self.executor.is_alive():
            log(f"releasing executor for {self.job_id}")
            self.executor = None

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} job_id={self.job_id} "
            f"definition={self.definition} creation_time={self.creation_time}>"
        )


class RestoreObjectJob(JobExecutor):
    OPERATION = "S3InitiateRestoreObject"

    @cached_property
    def _expiration_days(self):
        if "ExpirationInDays" in self.definition.operation_definition:
            try:
                return int(self.definition.operation_definition["ExpirationInDays"])
            except ValueError:
                raise ValidationError("ExpirationInDays")
        return 1

    def run(self):
        succeeded = []
        failed = []
        try:
            self.job.status = JobStatus.PREPARING.value
            backend = s3_backends[self.definition.account_id][self.definition.partition]

            manifest_file_obj = None
            try:
                manifest_file_obj = backend.get_object(
                    self.definition.manifest_bucket_name, self.definition.manifest_path
                )  # type: ignore[attr-defined]
            except MissingBucket:
                pass

            if self.definition.report_enabled:
                report_bucket = self.definition.report_bucket.split(":")[-1]
                try:
                    backend.get_bucket(report_bucket)
                except MissingBucket:
                    self.job.failure_reasons.append(
                        {
                            "code": "NoSuchBucket",
                            "reason": f"Invalid manifest was provided. Target bucket {report_bucket} does not exist.",
                        }
                    )
                    self.job.status = JobStatus.FAILED.value
                    return

            if manifest_file_obj is None:
                self.job.failure_reasons.append(
                    {
                        "code": "ManifestNotFound",
                        "reason": "Manifest object was not found",
                    }
                )
                self.job.status = JobStatus.FAILED.value
                return

            self.job.status = JobStatus.ACTIVE.value

            expiration = datetime.datetime.now() + datetime.timedelta(
                self._expiration_days, restoration_delay_in_seconds
            )

            buckets_and_keys = list(self._buckets_and_keys_from_csv(manifest_file_obj))

            for bucket, key in buckets_and_keys:
                self.job.total_number_of_tasks += 1
                try:
                    key_obj = backend.get_object(bucket, key)
                except MissingBucket:
                    continue
                if key_obj is not None:
                    key_obj.status = "IN_PROGRESS"
                    key_obj.set_expiry(expiration)
                    self.job.number_of_tasks_succeeded += 1

            self.job.number_of_tasks_failed = (
                self.job.total_number_of_tasks - self.job.number_of_tasks_succeeded
            )
            self.job.status = JobStatus.COMPLETE.value

            sleep_time = datetime.datetime.now() + datetime.timedelta(
                0, restoration_delay_in_seconds
            )
            while datetime.datetime.now() < sleep_time:
                time.sleep(0.5)
                if self.stop_requested:
                    return

            for bucket_and_key in buckets_and_keys:
                try:
                    key_obj = backend.get_object(*bucket_and_key)
                except MissingBucket:
                    failed.append(bucket_and_key)
                    continue
                if key_obj is not None:
                    key_obj.restore(self._expiration_days)
                    succeeded.append(bucket_and_key)
                else:
                    failed.append(bucket_and_key)
        except Exception as exc:
            log(f"Exception in job {self.job.job_id}: {exc}\n")
            log(f"Stacktrace: {traceback.format_exc()}\n")
        finally:
            self.job.finish_time = datetime.datetime.now()


class JobsController:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._cleanup_job = None
        self._stop_requested = False
        self._jobs_lock = Lock()

    def stop(self):
        log("stop")
        for job in self._jobs.values():
            if job.status not in (JobStatus.FAILED, JobStatus.COMPLETE):
                job.stop()
        self._stop_requested = True
        self._cleanup_job.join(2)

    def submit_job(
        self, account_id: str, partition: str, params: Dict[str, Any]
    ) -> str:
        self.ensure_cleanup_job()
        job_definition = JobDefinition.from_dict(account_id, partition, params)
        job_id = str(uuid.uuid4())
        job = Job(job_id, job_definition)
        with self._jobs_lock:
            self._jobs[job_id] = job
        job.start()
        log(f"job submitted: {job=}")
        return job_id

    def get_job(self, job_id):
        with self._jobs_lock:
            return self._jobs.get(job_id)

    def ensure_cleanup_job(self):
        if not self._cleanup_job:
            self._stop_requested = False
            self._cleanup_job = Thread(group=None, target=self.do_cleanup)
            self._cleanup_job.start()

    def do_cleanup(self):
        while not self._stop_requested:
            with self._jobs_lock:
                jobs = copy(self._jobs)
            for job in jobs.values():
                job.try_cleanup()
            with self._jobs_lock:
                now = datetime.datetime.now()
                pre_jobs_count = len(self._jobs)
                self._jobs = {
                    job_id: job
                    for job_id, job in self._jobs.items()
                    if not job.finish_time
                    or now - job.finish_time
                    < datetime.timedelta(minutes=JOBS_RETENTION_TIME_IN_MINUTES)
                }
                post_jobs_count = len(self._jobs)
            if pre_jobs_count != post_jobs_count:
                log(f"do_cleanup: jobs {pre_jobs_count} -> {post_jobs_count}")
            time.sleep(1)

    def list_jobs(self, job_statuses_filter):
        with self._jobs_lock:
            return [
                job
                for job in self._jobs.values()
                if job.status in job_statuses_filter or not job_statuses_filter
            ]
