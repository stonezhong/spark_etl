class SparkETLException(Exception):
    def __init__(self, message, cause=None):
        super(SparkETLException, self).__init__(message)
        self.cause = cause

class SparkETLDeploymentFailure(SparkETLException):
    # Failed to deploy an application
    def __init__(self, message, cause=None):
        super(SparkETLDeploymentFailure, self).__init__(message, cause=cause)

class SparkETLLaunchFailure(SparkETLException):
    # Failed to launch an application
    def __init__(self, message, cause=None):
        super(SparkETLLaunchFailure, self).__init__(message, cause=cause)

class SparkETLKillFailure(SparkETLException):
    # Failed to kill an application run
    def __init__(self, message, cause=None):
        super(SparkETLKillFailure, self).__init__(message, cause=cause)

class SparkETLGetStatusFailure(SparkETLException):
    # Failed to get status of a launched application (aka run)
    def __init__(self, message, cause=None):
        super(SparkETLGetStatusFailure, self).__init__(message, cause=cause)
