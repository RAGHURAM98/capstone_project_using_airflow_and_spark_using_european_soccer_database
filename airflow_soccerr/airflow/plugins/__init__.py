from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class redshiftplugin(AirflowPlugin):
    name = "redshiftplugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateTableRedShiftOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.DataQualitySqlQueries
    ]
