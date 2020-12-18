from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException
from airflow.models import Variable

class TerminateAwsEmrSparkCluster(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_emr_cluster_conn,
                 *args, **kwargs):

        super(TerminateAwsEmrSparkCluster, self).__init__(*args, **kwargs)
        self.aws_emr_cluster_conn = aws_emr_cluster_conn

    def terminate_aws_spark_emr_cluster(self, clusterId):
        try:
            self.aws_emr_cluster_conn.terminate_job_flows(JobFlowIds=[clusterId])
        except Exception as e:
            self.logger.error("Failed attemmpt to delete the AWS EMR Cluster", exc_info=True)
            raise AirflowException("Unsucessful !! termination of the Spark EMR cluster")

    def execute(self, context):
        clusterId = Variable.get("cluster_id")
        self.log.info("Deleting the cluster SPARK AWS EMR cluster cluster id={0}".format(clusterId))
        self.terminate_aws_spark_emr_cluster(clusterId)