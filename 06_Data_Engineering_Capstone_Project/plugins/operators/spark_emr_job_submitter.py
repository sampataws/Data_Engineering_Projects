from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

from airflow import AirflowException

import requests,json,time


class SubmitSparkJobToAwsEmrViaLivy(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,aws_emr_cluster_conn,spark_job_file,enable_logging='False',job_kind="spark",
                 *args, **kwars):
        super(SubmitSparkJobToAwsEmrViaLivy, self).__init__(*args, **kwars)
        self.aws_emr_cluster_conn = aws_emr_cluster_conn
        self.spark_job_file = spark_job_file
        self.enable_logging = enable_logging
        self.kind = job_kind

    def create_spark_session(self,aws_emr_master_dns):
        # 8998 is the port on which the Livy server runs
        host = 'http://' + aws_emr_master_dns + ':8998'
        data = {'kind': self.kind,
                "conf": {
                         "spark.dynamicAllocation.enabled": "true",
                         "spark.shuffle.service.enabled": "true",
                         "spark.dynamicAllocation.initialExecutors": "3",
                         "spark.jars.packages": "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                         "spark.driver.extraJavaOptions": "-Dlog4jspark.root.logger=WARN,console"
                         }
                }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
        self.log.info("Spark Session Creation Response = {}".format(response.json()))
        return response.headers

    def wait_for_idle_spark_session(self, aws_emr_master_dns, response_headers):
        # wait for the session to be idle or ready for job submission
        status = ''
        host = 'http://' + aws_emr_master_dns + ':8998'
        self.log.info("Response Headers is -> "+response_headers)
        session_url = host + response_headers['location']
        while status != 'idle':
            time.sleep(3)
            status_response = requests.get(session_url, headers=response_headers)
            status = status_response.json()['state']
            self.log.info('Session status: ' + status)
        return session_url

    def submit_spark_statement(self, spark_session_url, statement_path):
        statements_url = spark_session_url + '/statements'
        with open(statement_path, 'r') as f:
            code = f.read()
        data = {'code': code}
        response = requests.post(statements_url, data=json.dumps(data),
                                 headers={'Content-Type': 'application/json'})
        self.log.info("POST response for statement ->".format_map(response.json()))
        return response

    def track_statement_progress(self,master_dns, response_headers):
        statement_status = ''
        host = 'http://' + master_dns + ':8998'
        session_url = host + response_headers['location'].split('/statements', 1)[0]
        # Poll the status of the submitted scala code
        while statement_status != 'available':
            # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
            statement_url = host + response_headers['location']
            statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
            statement_status = statement_response.json()['state']
            self.log.info('Statement status: ' + statement_status)

            if 'progress' in statement_response.json():
                self.log.info('Progress: ' + str(statement_response.json()['progress']))
            time.sleep(10)
        final_statement_status = statement_response.json()['output']['status']
        if final_statement_status == 'error':
            self.log.info('Statement exception: ' + statement_response.json()['output']['evalue'])
            for trace in statement_response.json()['output']['traceback']:
                self.log.info(trace)
            raise ValueError('Final Statement Status: ' + final_statement_status)

            # logging the logs
        lines = requests.get(session_url + '/log',
                             headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            self.log.info(line)
        self.log.info('Final Statement Status: ' + final_statement_status)
        return lines

    def kill_spark_session(self, spark_session_url):
        requests.delete(spark_session_url, headers={'Content-Type': 'application/json'})

    def execute(self, context):
        self.log.info("Processing spark file = {0}".format(self.spark_job_file))
        clusterId = Variable.get("cluster_id")
        response = self.aws_emr_cluster_conn.describe_cluster(ClusterId=clusterId)
        cluster_dns = response['Cluster']['MasterPublicDnsName']
        headers = self.create_spark_session(aws_emr_master_dns=cluster_dns)
        session_url = self.wait_for_idle_spark_session(aws_emr_master_dns=cluster_dns,response_headers=headers)
        execution_date = context.get("execution_date")
        execution_month = execution_date.strftime("%m")
        execution_year = execution_date.strftime("%Y")
        file_month_year = execution_date.strftime("%b").lower() + execution_date.strftime("%y")
        self.log.info("Execution date of the task submit spark job is {0}".format(execution_date))
        self.log.info("Execution year and month of submit spark job is  {0}".format(execution_year + execution_month))
        statement_response = self.submit_statement(session_url, self.file,
                                                   "year_month='{0}' \nfile_month_year='{1}' \n".format(execution_year + execution_month,
                                                                                                        file_month_year))
        logs = self.track_statement_progress(cluster_dns, statement_response.headers)
        self.kill_spark_session(session_url)
        for line in logs:
            if 'FAIL' in str(line):
                self.logging.error(line)
                raise AirflowException("Spark Job execution Failed : {0}".format(self.file))
            else:
                self.log.info(line)

        self.log.info("Spark Job completed for the file {0}".format(self.file))




