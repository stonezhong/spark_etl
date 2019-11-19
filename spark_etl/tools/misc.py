def retry(action, max_time):
    """Retrying an action.

        Calling action up to max_time, if the action succeeded with 
        max_time, the function return what the action returns, otherwise
        it throw the last exception the action throws.

        Note: Call has to make sure calling action multiple time does
              not have side effects

        This function retry on any exception, very simple rule.

        Parameters
        ----------
        action   : A function
        max_time : an integer
    """
    assert isinstance(max_time, int)
    assert max_time >= 1

    for i in range(0, max_time):
        try:
            return action()
        except:
            if i == max_time - 1:
                raise
            