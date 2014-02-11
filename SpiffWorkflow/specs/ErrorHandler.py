import logging

import SpiffWorkflow
from SpiffWorkflow.Task import Task
from SpiffWorkflow.specs import TaskSpec


LOG = logging.getLogger(__name__)


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
        # TODO: accept retry_times with retry_interval=<timedelta>
        # TODO: accept retry_seq=[ <timedelta>, ... ]
        # TODO: accept retry_interval=<timedelta> with retry_period=<timedelta>  

        super(ErrorHandler, self).__init__(parent, name or 'Error handler #%s' % ErrorHandler.next_id())
        self._matcher = match or self.MATCH_ALL
        assert resolution in ('retry', 'fail', 'complete')
        self.resolution = resolution
        self.retry_times = retry_times


    def _create_subworkflow(self, my_task):
        from SpiffWorkflow.specs import WorkflowSpec
        wf_spec = WorkflowSpec(my_task.get_name())
        for ts in self.outputs:
            wf_spec.start.connect(ts) 
        outer_workflow = my_task.workflow.outer_workflow
        return SpiffWorkflow.Workflow(wf_spec, parent=outer_workflow)


    def _on_ready_before_hook(self, my_task):
        error_workflow    = self._create_subworkflow(my_task)
        error_workflow.completed_event.connect(self._on_error_workflow_completed, my_task)
        my_task.parent.internal_data['error_workflow'] = error_workflow

        #my_task._sync_children(error_workflow.task_tree.children, Task.FUTURE)
        my_task.children = error_workflow.task_tree.children

        '''
        for child in error_workflow.task_tree.children:
            my_task.children.insert(0, child)
            child.parent = my_task
            child.task_spec._update_state(child)
        '''

    def _on_ready_hook(self, my_task):
        # Assign variables, if so requested.
        error_workflow = my_task.parent.internal_data['error_workflow']
        LOG.debug("Error handler workflow inheriting data from '%s'", self.name)
        error_workflow.task_tree.set_data(**my_task.data)


    def _on_error_workflow_completed(self, error_workflow, my_task):
            failed_task = my_task.parent
            #LOG.debug("'%s' inheriting data from finished compensator '%s'", 
            #            failed_task.get_name(), self.name)
            #failed_task.set_data(**error_workflow.data)

            if self.resolution == 'complete':
                failed_task._set_state(Task.COMPLETED)
                for child in failed_task.children:
                    if child.task_spec in failed_task.task_spec.outputs:
                        # Alright, abusing that hook is just evil but it works.
                        child.task_spec._update_state_hook(child)

            elif self.resolution == 'fail':
                failed_task._set_state(Task.FAILED)

            elif self.resolution == 'retry':
                my_data = failed_task.internal_data['error_data'][self]
                if self.retry_times:
                    my_data['retry_times'] += 1
                self._retry_failed_task(failed_task)
                #self._retry_failed_task_clone(failed_task)


    def _retry_failed_task(self, failed_task):
        '''
        Retry failed task by setting it's state to FUTURE
        '''
        failed_task._set_state(Task.FUTURE, force=True)
        failed_task.task_spec._update_state(failed_task)


    def _retry_failed_task_clone(self, failed_task):
        '''
        Retry failed task by cloning it, leaving exisitng task as is. 
        (marat): personally a like this way much better, then _retry_failed_task(), but 
        there is a problem with leaving failed_task in ERROR state, 
        cause workflow becomes incompleted forever
        '''
        cloned_task = Task(failed_task.workflow, failed_task.task_spec, 
                        parent=failed_task.parent, state=Task.FUTURE, 
                        internal_data={'error_data': failed_task.internal_data['error_data']})
        # Correct position
        siblings = failed_task.parent.children 
        siblings.pop()
        siblings.insert(siblings.index(failed_task) + 1, cloned_task)

        for child in failed_task.children[:]:
            if child.task_spec in failed_task.task_spec.outputs:
                cloned_task.children.append(child)
                failed_task.children.remove(child)

        cloned_task.task_spec._update_state(cloned_task)


    def match(self, failed_task, e):
        '''
        Should return True if matches error handling criterias for failed_task
        '''
        ret = True
        if self.resolution == 'retry':
            ret = self._match_retry(failed_task, e)

        # return True
        # FIXME: TypeError: <lambda>() takes exactly 2 arguments (3 given)
        return ret and self._matcher(type(e), e)


    def _match_retry(self, failed_task, e):
        datas = failed_task.internal_data.get('error_data', {})
        if not datas.get(self):
            return True

        my_data = datas[self]
        if self.retry_times and my_data['retry_times'] < self.retry_times:
            return True

        return False


    def select(self, parent_task):
        '''
        Called when it will be used for error handling for parent_task
        '''
        parent_task.internal_data['error_handler'] = self

        if not 'error_data' in parent_task.internal_data:
            parent_task.internal_data['error_data'] = {}
        datas = parent_task.internal_data['error_data']

        if not self in datas:
            my_data = {
                'retry_times': 0,
                'retry_interval': None,
                'retry_period': None,
                'retry_seq': None            
            }
            datas[self] = my_data


    def _on_complete_hook(self, my_task):
        # TODO: Handle time deltas here and return WAITING 
        print 'ErrorHandler.__on_complete_hook: ', my_task.children
        #for child in my_task.children:
        #    child.task_spec._update_state(child)
        #
        super(ErrorHandler, self)._on_complete_hook(my_task)
        
