class SparkETLException(Exception):
    """Base class for exception raised from spark_etl package"""
    def __init__(self, message, cause=None):
        super(SparkETLException, self).__init__(message)
        self.cause = cause

class SparkETLDeploymentFailure(SparkETLException):
    """spark-etl failed to deploy an application"""
    def __init__(self, message, cause=None):
        super(SparkETLDeploymentFailure, self).__init__(message, cause=cause)

class SparkETLLaunchFailure(SparkETLException):
    """spark-etl failed to launch an application"""
    def __init__(self, message, cause=None):
        super(SparkETLLaunchFailure, self).__init__(message, cause=cause)

