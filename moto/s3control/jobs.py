import binascii
import csv
import dataclasses
import datetime
import io
import json
import secrets
import time
import traceback
import uuid
from copy import copy
from enum import Enum
from functools import cached_property
from hashlib import md5
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

from moto.core.common_models import BaseModel
from moto.moto_api._internal.managed_state_model import ManagedState
from moto.s3.exceptions import MissingBucket
from moto.s3.models import s3_backends

from ..s3.exceptions import InvalidRequest, S3AccessDeniedError
from ..utilities.arns import parse_arn
from .exceptions import InvalidJobOperation, ValidationError

restoration_delay_in_seconds = 60

JOBS_RETENTION_TIME_IN_MINUTES = 15
ASSUMED_BATCH_ROLE = "s3batch-role"

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
        batch_role_arn: str | None,
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
        self.batch_role_arn = batch_role_arn
        self._validate_definition()

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
        batch_role_arn = params["RoleArn"]
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
            batch_role_arn=batch_role_arn,
        )

    def _validate_definition(self):
        def _validate_arn(arn: str, account_id: str):
            try:
                parsed = parse_arn(arn)
            except ValueError:
                raise InvalidRequest("Invalid role arn")
            if parsed.partition != self.partition:
                raise S3AccessDeniedError()
            if parsed.account and parsed.account != account_id:
                raise S3AccessDeniedError()

        _validate_arn(self.manifest_arn, self.account_id)
        _validate_arn(self.report_bucket, self.account_id)
        _validate_arn(self.batch_role_arn, self.account_id)

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

    @cached_property
    def _version_index_in_csv(self):
        try:
            index = self.definition.manifest_fields.index("VersionId")
        except ValueError:
            index = None
        return index

    def _manifest_fields_from_csv(self, file_obj):
        stream = io.StringIO(file_obj.value.decode(encoding="utf-8"))
        version_id_index = self._version_index_in_csv
        for row in csv.reader(stream):
            yield (
                row[self._bucket_index_in_csv],
                row[self._key_index_in_csv],
                row[version_id_index] if version_id_index is not None else None,
            )


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


@dataclasses.dataclass(slots=True)
class RestoreError:
    bucket: str
    key: str
    error_code: int
    http_status_code: str
    result_message: str


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

    @property
    def s3_backend(self):
        return s3_backends[self.definition.account_id][self.definition.partition]

    def run(self):
        succeeded = []
        errors = []
        try:
            self.job.status = JobStatus.PREPARING.value
            backend = self.s3_backend

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

            reason_code, reason_message = self.validate_manifest(manifest_file_obj)
            if reason_code:
                self.job.failure_reasons.append(
                    {"code": reason_code, "reason": reason_message}
                )
                self.job.status = JobStatus.FAILED.value
                return

            batch_role_arn = parse_arn(self.job.definition.batch_role_arn)
            if batch_role_arn.resource_id != ASSUMED_BATCH_ROLE:
                self.job.failure_reasons.append(
                    {
                        "code": "AccessDenied",
                        "reason": f"Could not assume the IAM role: {self.job.definition.batch_role_arn}, "
                                  "Please ensure that a trust policy is attached to the role that allows "
                                  "batchoperations.s3.amazonaws.com to assume the role",
                    }
                )
                self.job.status = JobStatus.FAILED.value
                return

            self.job.status = JobStatus.ACTIVE.value

            expiration = datetime.datetime.now() + datetime.timedelta(
                self._expiration_days, restoration_delay_in_seconds
            )

            manifest_fields = list(self._manifest_fields_from_csv(manifest_file_obj))

            for bucket, key, version_id in manifest_fields:
                self.job.total_number_of_tasks += 1
                # this is just to simulate some errors based on key name
                key_error = self.error_for_key(bucket, key)
                if key_error:
                    errors.append(key_error)
                else:
                    try:
                        key_obj = backend.get_object(bucket, key, version_id=version_id)
                    except MissingBucket:
                        continue
                    if key_obj is not None:
                        key_obj.status = "IN_PROGRESS"
                        key_obj.set_expiry(expiration)
                        self.job.number_of_tasks_succeeded += 1

            self.job.number_of_tasks_failed = (
                self.job.total_number_of_tasks - self.job.number_of_tasks_succeeded
            )
            self.create_report_file(errors)
            self.job.status = JobStatus.COMPLETE.value

            sleep_time = datetime.datetime.now() + datetime.timedelta(
                0, restoration_delay_in_seconds
            )
            while datetime.datetime.now() < sleep_time:
                time.sleep(0.5)
                if self.stop_requested:
                    return

            for mf in manifest_fields:
                try:
                    key_obj = backend.get_object(*mf)
                except MissingBucket:
                    continue
                if key_obj is not None:
                    key_obj.restore(self._expiration_days)
                    succeeded.append(mf)
        except Exception as exc:
            log(f"Exception in job {self.job.job_id}: {exc}\n")
            log(f"Stacktrace: {traceback.format_exc()}\n")
        finally:
            self.job.finish_time = datetime.datetime.now()

    def validate_manifest(self, manifest_file_obj):
        if manifest_file_obj is None:
            return "ManifestNotFound", "Manifest object was not found"

        if manifest_file_obj.etag != f'"{self.definition.manifest_etag}"':
            return "ManifestNotFound", "Etag mismatch reading manifest"

        if (
            manifest_file_obj.encryption is not None
            and manifest_file_obj.encryption != "AES256"
        ):
            return (
                "InvalidManifestContent",
                "Unsupported encryption type SSE_KMS used for manifest",
            )

        return None, None

    @staticmethod
    def error_for_key(bucket, key):
        # let's take the "file name" of the key
        key_last_part = key.split("/")[-1]
        if not key_last_part.startswith("fail_with_"):
            return None
        # It is string like AccessDenied but for some reason Amazon call it http status code,
        # this code just keeps their naming.
        # Example file name: fail_with_AccessDenied_123 - the code takes AccessDenied from the file name
        http_status_code = key_last_part.removeprefix("fail_with_").split("_")[0]
        error_code = 400
        result_message = "Unknown error occurred"
        match http_status_code:
            case "AccessDenied":
                error_code = 403
                result_message = (
                    "User: arn:aws:sts::012345678910:assumed-role/s3batch-role/s3-batch-operations_some-id "
                    f'is not authorized to perform: s3:ListBucket on resource: "arn:aws:s3:::{bucket}" '
                    "because no identity-based policy allows the s3:ListBucket action"
                )
            case "InvalidObjectState":
                error_code = 403
                result_message = (
                    "Restore is not allowed for the object's current storage class"
                )
            case "RestoreAlreadyInProgress":
                error_code = 409
                result_message = "Object restore is already in progress"
        result_message_suffix = (
            f" (Service: Amazon S3; Status Code: {error_code}; Error Code: {http_status_code}; "
            "Request ID: some-id; S3 Extended Request ID: some-id; Proxy: null)"
        )
        result_message += result_message_suffix
        return RestoreError(bucket, key, error_code, http_status_code, result_message)

    def create_report_file(self, errors):
        if not self.definition.report_enabled:
            return

        report_prefix = f"{self.definition.report_prefix}/job-{self.job.job_id}"
        report_bucket = self.definition.report_bucket.removeprefix(f"arn:{self.definition.partition}:s3:::")
        manifest_dict = {
            "Format": "Report_CSV_20180820",
            "ReportCreationDate": datetime.datetime.now(datetime.UTC).strftime(
                "%Y-%m-%dT%H:%M:%S:%fZ"
            ),
            "Results": [],
            "ReportSchema": "Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage",
        }
        if errors:
            hex_string = binascii.hexlify(secrets.token_bytes(20)).decode("utf-8")
            result_key_name = f"{report_prefix}/results/{hex_string}.csv"
            md5_checksum = self.create_failures_result_file(
                report_bucket, result_key_name, errors
            )
            manifest_dict["Results"].append(
                {
                    "TaskExecutionStatus": "failed",
                    "Bucket": report_bucket,
                    "MD5Checksum": md5_checksum,
                    "Key": result_key_name,
                }
            )

        manifest_content = json.dumps(manifest_dict).encode("utf-8")
        manifest_key_name = f"{report_prefix}/manifest.json"
        self.s3_backend.put_object(report_bucket, manifest_key_name, manifest_content)
        manifest_md5_key_name = f"{manifest_key_name}.md5"
        manifest_md5_content = md5(manifest_content).hexdigest().encode("utf-8")
        self.s3_backend.put_object(
            report_bucket, manifest_md5_key_name, manifest_md5_content
        )

    def create_failures_result_file(self, bucket_name, key_name, errors):
        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n")
        for error in errors:
            writer.writerow(
                [
                    error.bucket,
                    error.key,
                    "",
                    "failed",
                    error.error_code,
                    error.http_status_code,
                    error.result_message,
                ]
            )
        content = output.getvalue().encode("utf8")
        md5_checksum = md5(content).hexdigest()
        self.s3_backend.put_object(bucket_name, key_name, content)
        return md5_checksum


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
            try:
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
            except Exception as exc:
                log(f"do_cleanup: failed with {exc}")
            time.sleep(1)

    def list_jobs(self, job_statuses_filter):
        with self._jobs_lock:
            return [
                job
                for job in self._jobs.values()
                if job.status in job_statuses_filter or not job_statuses_filter
            ]
