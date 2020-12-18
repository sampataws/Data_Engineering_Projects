
import plugins.operators as operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class SparkEmrCustomPlugins(AirflowPlugin):
    name = "udacity_de_project_plugin"
    operators = [
        operators.CreateAWSEmrClusterOperator,
        operators.TerminateAWSEMRClusterOperator,
        operators.SubmitSparkJobToAWSEmrOperator
]