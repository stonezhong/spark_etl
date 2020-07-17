class AbstractJobSubmitter:
    def __init__(self, config):
        self.config = config

    def run(self, deployment_location, options={}, args={}):
        """
        run an application

        Parameters
        -----------
        deployment_location: str
            the location the application build is deployed.
        options: dict
            The runtime options, it could be vendor specific. The job submitter understands it. Not passed to application.
        args: dict
            the runtime argument that passed to application.
        
        return:
        it should return a dict with 2 keys.
            state :  If the job succeeded, it should be SUCCEEDED, otherwise it should be FAILED
            run_id:  A string of unique ID of the run
        """
        raise NotImplementedError()
