#The purpose of this init script: define a custom plugin 

from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):#Inherit from AirflowPlugin
    name = "udacity_plugin" #Determin how you import the code: from udacity_plugin, import the following operators and helpers
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
