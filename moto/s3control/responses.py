import json
from typing import Any, Dict, Tuple

import xmltodict

from moto.core.common_types import TYPE_RESPONSE
from moto.core.responses import BaseResponse
from moto.s3.responses import S3_PUBLIC_ACCESS_BLOCK_CONFIGURATION

from ..core.utils import iso_8601_datetime_without_milliseconds
from .exceptions import InvalidJobOperation, JobNotFoundException
from .models import S3ControlBackend, s3control_backends


class S3ControlResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="s3control")

    @property
    def backend(self) -> S3ControlBackend:
        return s3control_backends[self.current_account][self.partition]

    def get_public_access_block(self) -> str:
        account_id = self.headers.get("x-amz-account-id")
        public_block_config = self.backend.get_public_access_block(
            account_id=account_id
        )
        template = self.response_template(S3_PUBLIC_ACCESS_BLOCK_CONFIGURATION)
        return template.render(public_block_config=public_block_config)

    def put_public_access_block(self) -> TYPE_RESPONSE:
        account_id = self.headers.get("x-amz-account-id")
        pab_config = self._parse_pab_config(self.body)
        self.backend.put_public_access_block(
            account_id, pab_config["PublicAccessBlockConfiguration"]
        )
        return 201, {"status": 201}, json.dumps({})

    def delete_public_access_block(self) -> TYPE_RESPONSE:
        account_id = self.headers.get("x-amz-account-id")
        self.backend.delete_public_access_block(account_id=account_id)
        return 204, {"status": 204}, json.dumps({})

    def _parse_pab_config(self, body: str) -> Dict[str, Any]:
        parsed_xml = xmltodict.parse(body)
        parsed_xml["PublicAccessBlockConfiguration"].pop("@xmlns", None)

        return parsed_xml

    def create_access_point(self) -> str:
        account_id, name = self._get_accountid_and_name_from_accesspoint(self.uri)
        params = xmltodict.parse(self.body)["CreateAccessPointRequest"]
        bucket = params["Bucket"]
        vpc_configuration = params.get("VpcConfiguration")
        public_access_block_configuration = params.get("PublicAccessBlockConfiguration")
        access_point = self.backend.create_access_point(
            account_id=account_id,
            name=name,
            bucket=bucket,
            vpc_configuration=vpc_configuration,
            public_access_block_configuration=public_access_block_configuration,
        )
        template = self.response_template(CREATE_ACCESS_POINT_TEMPLATE)
        return template.render(access_point=access_point)

    def create_job(self) -> str:
        account_id, _ = self._get_accountid_and_job_id_from_job(self.uri)
        params = xmltodict.parse(self.body)["CreateJobRequest"]
        job_id = self.backend.create_job(account_id=account_id, params=params)
        template = self.response_template(CREATE_JOB_TEMPLATE)
        return template.render(job_id=job_id)

    def describe_job(self) -> str:
        account_id, job_id = self._get_accountid_and_job_id_from_job(self.uri)
        print(f"describe_job: {self.uri} -> {account_id=} {job_id=}\n")
        job = self.backend.get_job(job_id=job_id)
        if not job:
            raise JobNotFoundException()

        if job.definition.operation_name != "S3InitiateRestoreObject":
            raise InvalidJobOperation(
                f"Unsupported operation: {job.definition.operation_name}"
            )

        operation_template = self.response_template(
            GET_JOB_INITIATE_S3_RESTORE_TEMPLATE
        )
        operation = operation_template.render(
            expiration_in_days=job.definition.operation_definition.get(
                "ExpirationInDays"
            ),
            glacier_job_tier=job.definition.operation_definition.get("GlacierJobTier"),
        )

        template = self.response_template(GET_JOB_TEMPLATE)
        return template.render(
            job_id=job_id,
            account_id=account_id,
            operation=operation,
            description=job.definition.description,
            creation_time=iso_8601_datetime_without_milliseconds(job.creation_time),
            failure_reasons=job.failure_reasons,
            region_name=self.backend.region_name,
            manifest_etag=job.definition.manifest_etag,
            manifest_object_arn=job.definition.manifest_arn,
            manifest_format=job.definition.manifest_format,
            manifest_fields=job.definition.manifest_fields,
            number_of_tasks_failed=job.number_of_tasks_failed,
            number_of_tasks_succeeded=job.number_of_tasks_succeeded,
            elapsed_time_in_active_seconds=job.elapsed_time_in_active_seconds,
            total_number_of_tasks=job.total_number_of_tasks,
            report_bucket=job.definition.report_bucket,
            report_enabled=job.definition.report_enabled,
            report_format=job.definition.report_format,
            report_prefix=job.definition.report_prefix,
            report_scope=job.definition.report_scope,
            status=job.status,
        )

    def list_jobs(self) -> str:
        statuses = self._get_multi_param("jobStatuses")
        max_results = self._get_int_param("maxResults")
        next_token = self._get_param("nextToken")
        list_jobs, next_token = self.backend.list_jobs(
            job_statuses_filter=statuses, max_results=max_results, next_token=next_token
        )
        jobs = [
            {
                "creation_time": job.creation_time.timestamp(),
                "description": job.definition.description,
                "job_id": job.job_id,
                "operation": job.definition.operation_name,
                "priority": 10,
                "number_of_tasks_failed": 123,
                "number_of_tasks_succeeded": 456,
                "elapsed_time_in_active_seconds": 789,
                "total_number_of_tasks": 135,
                "status": job.status,
                "termination_date": (
                    job.finish_time.timestamp() if job.finish_time else None
                ),
            }
            for job in list_jobs
        ]
        template = self.response_template(LIST_JOBS_TEMPLATE)
        return template.render(jobs=jobs, next_token=next_token)

    def get_access_point(self) -> str:
        account_id, name = self._get_accountid_and_name_from_accesspoint(self.uri)

        access_point = self.backend.get_access_point(account_id=account_id, name=name)
        template = self.response_template(GET_ACCESS_POINT_TEMPLATE)
        return template.render(access_point=access_point)

    def delete_access_point(self) -> TYPE_RESPONSE:
        account_id, name = self._get_accountid_and_name_from_accesspoint(self.uri)
        self.backend.delete_access_point(account_id=account_id, name=name)
        return 204, {"status": 204}, ""

    def put_access_point_policy(self) -> str:
        account_id, name = self._get_accountid_and_name_from_policy(self.uri)
        params = xmltodict.parse(self.body)
        policy = params["PutAccessPointPolicyRequest"]["Policy"]
        self.backend.put_access_point_policy(account_id, name, policy)
        return ""

    def get_access_point_policy(self) -> str:
        account_id, name = self._get_accountid_and_name_from_policy(self.uri)
        policy = self.backend.get_access_point_policy(account_id, name)
        template = self.response_template(GET_ACCESS_POINT_POLICY_TEMPLATE)
        return template.render(policy=policy)

    def delete_access_point_policy(self) -> TYPE_RESPONSE:
        account_id, name = self._get_accountid_and_name_from_policy(self.uri)
        self.backend.delete_access_point_policy(account_id=account_id, name=name)
        return 204, {"status": 204}, ""

    def get_access_point_policy_status(self) -> str:
        account_id, name = self._get_accountid_and_name_from_policy(self.uri)
        self.backend.get_access_point_policy_status(account_id, name)
        template = self.response_template(GET_ACCESS_POINT_POLICY_STATUS_TEMPLATE)
        return template.render()

    def _get_accountid_and_name_from_accesspoint(
        self, full_url: str
    ) -> Tuple[str, str]:
        url = full_url
        if full_url.startswith("http"):
            url = full_url.split("://")[1]
        account_id = url.split(".")[0]
        name = url.split("v20180820/accesspoint/")[-1]
        return account_id, name

    def _get_accountid_and_name_from_policy(self, full_url: str) -> Tuple[str, str]:
        url = full_url
        if full_url.startswith("http"):
            url = full_url.split("://")[1]
        account_id = url.split(".")[0]
        name = self.path.split("/")[-2]
        return account_id, name

    def _get_accountid_and_job_id_from_job(self, full_url: str) -> Tuple[str, str]:
        url = full_url
        if full_url.startswith("http"):
            url = full_url.split("://")[1]
        account_id = url.split(".")[0]
        job_id = url.split("v20180820/jobs/")[-1]
        return account_id, job_id


CREATE_ACCESS_POINT_TEMPLATE = """<CreateAccessPointResult>
  <ResponseMetadata>
    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
  </ResponseMetadata>
  <Alias>{{ access_point.alias }}</Alias>
  <AccessPointArn>{{ access_point.arn }}</AccessPointArn>
</CreateAccessPointResult>
"""


GET_ACCESS_POINT_TEMPLATE = """<GetAccessPointResult>
  <ResponseMetadata>
    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
  </ResponseMetadata>
  <Name>{{ access_point.name }}</Name>
  <Bucket>{{ access_point.bucket }}</Bucket>
  <NetworkOrigin>{{ access_point.network_origin }}</NetworkOrigin>
  {% if access_point.vpc_id %}
  <VpcConfiguration>
      <VpcId>{{ access_point.vpc_id }}</VpcId>
  </VpcConfiguration>
  {% endif %}
  <PublicAccessBlockConfiguration>
      <BlockPublicAcls>{{ access_point.pubc["BlockPublicAcls"] }}</BlockPublicAcls>
      <IgnorePublicAcls>{{ access_point.pubc["IgnorePublicAcls"] }}</IgnorePublicAcls>
      <BlockPublicPolicy>{{ access_point.pubc["BlockPublicPolicy"] }}</BlockPublicPolicy>
      <RestrictPublicBuckets>{{ access_point.pubc["RestrictPublicBuckets"] }}</RestrictPublicBuckets>
  </PublicAccessBlockConfiguration>
  <CreationDate>{{ access_point.created }}</CreationDate>
  <Alias>{{ access_point.alias }}</Alias>
  <AccessPointArn>{{ access_point.arn }}</AccessPointArn>
  <Endpoints>
      <entry>
          <key>ipv4</key>
          <value>s3-accesspoint.us-east-1.amazonaws.com</value>
      </entry>
      <entry>
          <key>fips</key>
          <value>s3-accesspoint-fips.us-east-1.amazonaws.com</value>
      </entry>
      <entry>
          <key>fips_dualstack</key>
          <value>s3-accesspoint-fips.dualstack.us-east-1.amazonaws.com</value>
      </entry>
      <entry>
          <key>dualstack</key>
          <value>s3-accesspoint.dualstack.us-east-1.amazonaws.com</value>
      </entry>
  </Endpoints>
</GetAccessPointResult>
"""


GET_ACCESS_POINT_POLICY_TEMPLATE = """<GetAccessPointPolicyResult>
  <ResponseMetadata>
    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
  </ResponseMetadata>
  <Policy>{{ policy }}</Policy>
</GetAccessPointPolicyResult>
"""


GET_ACCESS_POINT_POLICY_STATUS_TEMPLATE = """<GetAccessPointPolicyResult>
  <ResponseMetadata>
    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
  </ResponseMetadata>
  <PolicyStatus>
      <IsPublic>true</IsPublic>
  </PolicyStatus>
</GetAccessPointPolicyResult>
"""


CREATE_JOB_TEMPLATE = """<CreateJobResult>
  <ResponseMetadata>
    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
  </ResponseMetadata>
   <JobId>{{ job_id }}</JobId>
</CreateJobResult>
"""


GET_JOB_TEMPLATE = """
<?xml version="1.0" encoding="UTF-8"?>
<DescribeJobResult>
  <Job>
    <ConfirmationRequired>False</ConfirmationRequired>
    <CreationTime>{{ creation_time }}</CreationTime>
    <Description>{{ description }}</Description>
    <FailureReasons>
    {% for failure_reason in failure_reasons %}
      <JobFailure>
        <FailureCode>{{ failure_reason["code"] }}</FailureCode>
        <FailureReason>{{ failure_reason["reason"] }}</FailureReason>
      </JobFailure>
    {% endfor %}
    </FailureReasons>
    <JobArn>arn:aws:s3:{{ region_name }}:{{ account_id }}:job/{{ job_id }}</JobArn>
    <JobId>{{ job_id }}</JobId>
    <Manifest>
      <Location>
        <ETag>{{ manifest_etag }}</ETag>
        <ObjectArn>{{ manifest_object_arn }}</ObjectArn>
      </Location>
      <Spec>
        <Fields>
          {% for manifest_field in manifest_fields %}
            <member>{{ manifest_field }}</member>
          {% endfor %}
        </Fields>
        <Format>{{ manifest_format }}</Format>
      </Spec>
    </Manifest>
      <Operation>
         {{ operation }}
      </Operation>
      <Priority>10</Priority>
      <ProgressSummary>
         <NumberOfTasksFailed>{{ number_of_tasks_failed }}</NumberOfTasksFailed>
         <NumberOfTasksSucceeded>{{ number_of_tasks_succeeded }}</NumberOfTasksSucceeded>
         <Timers>
            <ElapsedTimeInActiveSeconds>{{ elapsed_time_in_active_seconds }}</ElapsedTimeInActiveSeconds>
         </Timers>
         <TotalNumberOfTasks>{{ total_number_of_tasks }}</TotalNumberOfTasks>
      </ProgressSummary>
      <Report>
         <Bucket>{{ report_bucket }}</Bucket>
         <Enabled>{{ report_enabled }}</Enabled>
         <Format>{{ report_format }}</Format>
         <Prefix>{{ report_prefix }}</Prefix>
         <ReportScope>{{ report_scope }}</ReportScope>
      </Report>
      <RoleArn>arn:aws:iam::{{ account_id }}:role/s3batch-role</RoleArn>
      <Status>{{ status }}</Status>
   </Job>
</DescribeJobResult>
"""


GET_JOB_INITIATE_S3_RESTORE_TEMPLATE = """
<S3InitiateRestoreObject>
  {% if expiration_in_days is not none %}
    <ExpirationInDays>{{ expiration_in_days }}</ExpirationInDays>
  {% endif %}
  {% if glacier_job_tier is not none %}
    <GlacierJobTier>{{ glacier_job_tier }}</GlacierJobTier>
  {% endif %}
</S3InitiateRestoreObject>
"""

LIST_JOBS_TEMPLATE = """
<?xml version="1.0" encoding="UTF-8"?>
<ListJobsResult>
  {% if next_token is not none %}
    <NextToken>{{ next_token }}</NextToken>
  {% endif %}
  <Jobs>
    {% for job in jobs %}
      <JobListDescriptor>
         <CreationTime>{{ job["creation_time"] }}</CreationTime>
         <Description>{{ job["description"] }}</Description>
         <JobId>{{ job["job_id"] }}</JobId>
         <Operation>{{ job["operation"] }}</Operation>
         <Priority>{{ job["priority"] }}</Priority>
         <ProgressSummary>
            <NumberOfTasksFailed>{{ job["number_of_tasks_failed"] }}</NumberOfTasksFailed>
            <NumberOfTasksSucceeded>{{ job["number_of_tasks_succeeded"] }}</NumberOfTasksSucceeded>
            <Timers>
               <ElapsedTimeInActiveSeconds>{{ job["elapsed_time_in_active_seconds"] }}</ElapsedTimeInActiveSeconds>
            </Timers>
            <TotalNumberOfTasks>{{ job["total_number_of_tasks"] }}</TotalNumberOfTasks>
         </ProgressSummary>
         <Status>{{ job["status"] }}</Status>
         {% if job["termination_date"] is not none %}
           <TerminationDate>{{ job["termination_date"] }}</TerminationDate>
         {% endif %}
      </JobListDescriptor>
    {% endfor %}
  </Jobs>
</ListJobsResult>
"""
