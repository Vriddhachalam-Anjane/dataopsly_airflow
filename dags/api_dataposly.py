# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations
import requests
import time
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.context import Context


# class DataopslyRunJobOperatorLink(BaseOperatorLink):
#     """Allows users to monitor the triggered job run directly in dbt Cloud."""

#     name = "Monitor Job Run"

#     def get_link(self, operator: BaseOperator, *, ti_key=None):
#         return XCom.get_value(key="job_run_url", ti_key=ti_key)


class DataopslyJobRunOperator(BaseOperator):

    def __init__(
        self,
        *,
        dataopsly_conn_id: str = None,
        job_id: int = None,
        # run_id: int = None,
        token: str = None,
        trigger_reason: str = None,
        wait_for_termination: bool = True,
        timeout: int = 60 * 60, # Timeout set to 60 minutes (3600 seconds)
        check_interval: int = 10,
        reuse_existing_run: bool = False,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataopsly_conn_id = dataopsly_conn_id
        self.job_id = job_id
        # self.run_id = run_id
        self.token = token
        self.trigger_reason = trigger_reason
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.reuse_existing_run = reuse_existing_run
        self.deferrable = deferrable

    def trigger_job_run(self):
        endpoint_url = f"{self.dataopsly_conn_id}"
        print("Y trigger_job_run END_POINT_URL", endpoint_url)
        print("this is the self", self.dataopsly_conn_id)

        headers = {
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json',
        }

        payload = {
            'job_id': self.job_id
        }

        trigger_response = requests.post(endpoint_url, headers=headers, json=payload)

        print("trigger_response", trigger_response.json())

        if trigger_response.status_code == 200:
            trigger_response_data = trigger_response.json()
            run_id = trigger_response_data["run_id"]

            if run_id:
                start_time = time.time()  # Record the start time
                try:
                    while True:
                        elapsed_time = time.time() - start_time
                        if elapsed_time > self.timeout:
                            raise Exception(f"Timeout: Job run exceeded: {self.timeout} seconds ")

                        status_response = requests.get(endpoint_url.replace("-job", "") + f'/{run_id}/', headers=headers)
                        self.log.info(f"Status code: {status_response.status_code}")
                        self.log.info(f"Status response: {status_response.json()}")

                        if status_response.status_code == 200:
                            status_response_data = status_response.json()
                            status = status_response_data['status']
                            print("-------RUN_STATUS------:", status)

                            if status == 'completed':
                                print('STATUS == ', status)
                                break
                            elif status in ['error', 'cancelled', 'rejected']:
                                print('STATUS == ', status)
                                raise Exception(f"JOB {status.capitalize()}")
                        else:
                            status_response_data = status_response.json()

                        time.sleep(self.check_interval)
                    return {"run_id": status_response_data["id"], "status": status_response_data['status']}
                except Exception as e:
                    self.log.error(str(e))
                    raise e
            else:
                self.log.error("Run ID not found in the response.")
                raise Exception("Run ID not found in the response.")
        else:
            # Log an error if the request was not successful
            self.log.error(f"Failed to retrieve Status code: {trigger_response.status_code}")
            raise Exception(f"Failed to retrieve Status code: {trigger_response.status_code}")

    def execute(self, context):
        self.trigger_job_run()

class DataopslyRunStatusOperator(BaseOperator):

    def __init__(
        self,
        *,
        dataopsly_conn_id: str = None,
        # job_id: int = None,
        run_id: int = None,
        token: str = None,
        trigger_reason: str = None,
        wait_for_termination: bool = True,
        timeout: int = 60 * 60,
        check_interval: int = 10,
        reuse_existing_run: bool = False,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataopsly_conn_id = dataopsly_conn_id
        # self.job_id = job_id
        self.run_id = run_id
        self.token = token
        self.trigger_reason = trigger_reason
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.reuse_existing_run = reuse_existing_run
        self.deferrable = deferrable

    def run_status(self):
        endpoint_url = f"{self.dataopsly_conn_id}"

        headers = {
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json',
        }

        if self.run_id:
            start_time = time.time()  # Record the start time
            try:
                while True:
                    elapsed_time = time.time() - start_time
                    if elapsed_time > self.timeout:
                        raise Exception(f"Timeout: Job run exceeded: {self.timeout} seconds ")

                    status_response = requests.get(endpoint_url + f'/{self.run_id}/', headers=headers)
                    self.log.info(f"Status code: {status_response.status_code}")
                    self.log.info(f"Status response: {status_response.json()}")

                    if status_response.status_code == 200:
                        status_response_data = status_response.json()
                        status = status_response_data['status']
                        print("-------RUN_STATUS------:", status)

                        if status == 'completed':
                            print('STATUS == ', status)
                            break
                        elif status in ['error', 'cancelled', 'rejected']:
                            print('STATUS == ', status)
                            # raise Exception(f"JOB {status.capitalize()}")
                            break        
                    else:
                        status_response_data = status_response.json()
                    time.sleep(self.check_interval)
                return {"run_id": status_response_data["id"], "status": status_response_data['status']}
            except Exception as e:
                self.log.error(str(e))
                raise e
        else:
            self.log.error("Run ID not found in the response.")
            raise Exception("Run ID not found in the response.")

    def execute(self, context):
        self.run_status()



class DataopslyListJobsOperator(BaseOperator):

    def __init__(
        self,
        *,
        dataopsly_conn_id: str = None,
        token: str = None,
        # check_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataopsly_conn_id = dataopsly_conn_id
        self.token = token
        # self.check_interval = check_interval

    def list_jobs(self):
        endpoint_url = f"{self.dataopsly_conn_id}"

        headers = {
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json',
        }
        list_response = requests.get(endpoint_url, headers=headers)
        self.log.info(f"List Job Status code: {list_response.status_code}")
        self.log.info(f"List Job Status response: {list_response.json()}")

        if list_response.status_code == 200:
            list_response_data = list_response.json()
        else:
            list_response_data = list_response.json()
        return {"job_list": list_response_data}

    def execute(self, context):
        self.list_jobs()

