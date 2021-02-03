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

class SparkETLKillFailure(SparkETLException):
    """spark-etl failed to kill an application run"""
    def __init__(self, message, cause=None):
        super(SparkETLKillFailure, self).__init__(message, cause=cause)

class SparkETLGetStatusFailure(SparkETLException):
    """spark-etl failed to get status of a launched application"""
    def __init__(self, message, cause=None):
        super(SparkETLGetStatusFailure, self).__init__(message, cause=cause)
