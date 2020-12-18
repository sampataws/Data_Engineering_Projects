from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

import datetime
import boto3

boto3.setup_default_session(profile_name='sampatawsadmin')


class EMRClusterCreateOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, aws_region_name, aws_emr_cluster_conn, aws_emr_cluster_name, aws_emr_release_lable='6.0.0',
                 aws_emr_master_instance_type='m3.xlarge', num_emr_worker_nodes=2,
                 aws_emr_worker_nodes_instance_type='m3.2xlarge',
                 *args, **kwars):
        super(EMRClusterCreateOperator, self).__init__(*args, **kwars)
        self.aws_region_name = aws_region_name
        self.aws_emr_cluster_conn = aws_emr_cluster_conn
        self.aws_emr_cluster_name = aws_emr_cluster_name
        self.aws_emr_release_lable = aws_emr_release_lable
        self.aws_emr_master_instance_type = aws_emr_master_instance_type
        self.aws_emr_worker_nodes_instance_type = aws_emr_worker_nodes_instance_type
        self.num_emr_worker_nodes = num_emr_worker_nodes

    def get_ec2_security_group_id(self, group_name):
        ec2 = boto3.client(service_name="ec2", region_name=self.aws_region_name)
        """ :type : pyboto3.ec2 """
        response = ec2.describe_security_groups(GroupNames=[group_name])
        return response['SecurityGroups'][0]['GroupId']

    def create_aws_emr_cluster(self):
        aws_master_sercurity_group_id = self.get_ec2_security_group_id('AirflowEMRMasterSG')
        aws_worker_sercurity_group_id = self.get_ec2_security_group_id('AirflowEMRWorkerSG')
        try:
            cluster_response = self.aws_emr_cluster_conn.run_job_flow(
                Name='Airflow-' + self.aws_emr_cluster_name + "-" + str(datetime.datetime.utcnow()),
                ReleaseLabel=self.aws_emr_release_lable,
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': "Master nodes",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': self.aws_emr_master_instance_type,
                            'InstanceCount': 1
                        },
                        {
                            'Name': "Worker nodes",
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': self.aws_emr_worker_nodes_instance_type,
                            'InstanceCount': self.num_emr_worker_nodes
                        }
                    ],
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'Ec2KeyName': 'dataengineering-capstone-cloudformation',
                    'EmrManagedMasterSecurityGroup': aws_master_sercurity_group_id,
                    'EmrManagedSlaveSecurityGroup': aws_worker_sercurity_group_id
                },
                VisibleToAllUsers=True,
                JobFlowRole='EmrEc2InstanceProfile',
                ServiceRole='EmrRole',
                Applications=[
                    {'Name': 'hadoop'},
                    {'Name': 'spark'},
                    {'Name': 'hive'},
                    {'Name': 'livy'}
                ]
            )
            final_cluster_response = cluster_response['JobFlowId']
        except Exception as e:
            self.logger.error("Failed to Create EMR Cluster !!!", exc_info=True)
            raise AirflowException("EMR Cluster Creation Exception!")
        return final_cluster_response

    def execute(self, context):
        self.logger.info("Launching EMR cluster instance -> cluster={0} at region={1}".format(self.cluster_name, self.region_name))
        self.logger.info("EMR cluster number_of_worker_nodes={0}".format(self.num_core_nodes))
        task_instance = context['task_instance']
        cluster_id = self.create_cluster();
        Variable.set("cluster_id", cluster_id)
        task_instance.xcom_push('cluster_id', cluster_id)
        self.logger.info("ClusterId EMR is  = {0}".format(cluster_id))
        return cluster_id








