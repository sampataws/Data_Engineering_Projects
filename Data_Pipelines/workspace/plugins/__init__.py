from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import workspace.plugins.operators as operators
import workspace.plugins.helpers as helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
