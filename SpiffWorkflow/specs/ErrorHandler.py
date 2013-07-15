

from SpiffWorkflow.specs.TaskSpec import TaskSpec


class ErrorHandler(TaskSpec):
    MATCH_ALL = lambda E, e: True
    id_seq = 0

    @classmethod
    def next_id(cls):
        cls.id_seq += 1
        return cls.id_seq

    def __init__(self, parent, name=None, match=None, resolution='fail', 
                retry_times=None, retry_interval=None, 
                retry_period=None, retry_seq=None):
        # TODO: parse timedelta=<number>(s|m|h) or find better solution datatime.timedelta=
        # TODO: accept retry_times=<number> and retry_interval=<timedelta>
        # TODO: accept retry_seq=[ <timedelta>, ... ]
        # TODO: accept retry_interval=<timedelta> and retry_period=<timedelta>  

        super(ErrorHandler, self).__init__(parent, name or 'Except #%s' % ErrorHandler.next_id())
        self._matcher = match or self.MATCH_ALL
        assert resolution in ('retry', 'fail', 'complete')
        self.resolution = resolution
        #self.compensate_task = compensate_task
        self._matched = {}

    def match(self, task, e):
        if task in self._matched:
            if task.resolution != 'retry':
                return False
            times = self._matched[task]
            if times >= 1:
                return False
        return True
        # FIXME: TypeError: <lambda>() takes exactly 2 arguments (3 given)
        return self._matcher(type(e), e)


    def _on_complete(self, task):
        if not task in self._matched:
            self._matched[task] = 0
        self._matched[task] += 1
        
