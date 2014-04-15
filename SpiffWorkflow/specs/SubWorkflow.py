# Copyright (C) 2007 Samuel Abels
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
import os
from SpiffWorkflow.Task import Task
from SpiffWorkflow.exceptions import WorkflowException
from SpiffWorkflow.operators import valueof, FuncAttrib
from SpiffWorkflow.specs.TaskSpec import TaskSpec
import SpiffWorkflow

import logging

LOG = logging.getLogger(__name__)

def _eval_assign(assign, scope):
    if isinstance(assign, FuncAttrib):
        return valueof(scope, assign)
    else:
        return dict((left, valueof(scope, right)) 
                for left, right in assign.items())


class SubWorkflow(TaskSpec):
    """
    A SubWorkflow is a task that wraps a WorkflowSpec, such that you can
    re-use it in multiple places as if it were a task.
    If more than one input is connected, the task performs an implicit
    multi merge.
    If more than one output is connected, the task performs an implicit
    parallel split.
    """

    def __init__(self,
                 parent,
                 name,
                 file,
                 serializer_cls=None,
                 in_assign = None,
                 out_assign = None,
                 **kwargs):
        """
        Constructor.

        :type  parent: TaskSpec
        :param parent: A reference to the parent task spec.
        :type  name: str
        :param name: The name of the task spec.
        :type  file: str
        :param file: The name of a file containing a workflow.
        :type  in_assign: dict(str: Operator | str)
        :param in_assign: The names of data fields to carry over.
        :type  out_assign: dict(str: Operator | str)
        :param out_assign: The names of data fields to carry back.
        :type  kwargs: dict
        :param kwargs: See L{SpiffWorkflow.specs.TaskSpec}.
        """
        assert parent is not None
        assert name is not None
        super(SubWorkflow, self).__init__(parent, name, **kwargs)
        self.file       = None
        self.in_assign  = in_assign or {}
        self.out_assign = out_assign or {}
        if file is not None:
            dirname   = os.path.dirname(parent.file)
            self.file = os.path.abspath(os.path.join(dirname, file))
            if not serializer_cls:
                if self.file.endswith('.wf'):
                    from server.workflow_ext.dsl import DslSerializer
                    serializer_cls = DslSerializer
                else:
                    from SpiffWorkflow.storage import XmlSerializer
                    serializer_cls = XmlSerializer
            self.serializer_cls = serializer_cls

    def test(self):
        TaskSpec.test(self)
        if self.file is not None and not os.path.exists(self.file):
            raise WorkflowException(self, 'File does not exist: %s' % self.file)

    def _predict_hook(self, my_task):
        outputs = [task.task_spec for task in my_task.children]
        for output in self.outputs:
            if output not in outputs:
                outputs.insert(0, output)
        if my_task._is_definite():
            my_task._sync_children(outputs, Task.FUTURE)
        else:
            my_task._sync_children(outputs, my_task.state)

    def _create_subworkflow(self, my_task):
        from SpiffWorkflow.specs import WorkflowSpec
        file           = valueof(my_task, self.file)
        serializer     = self.serializer_cls()
        s_state        = open(file).read()
        wf_spec        = WorkflowSpec.deserialize(serializer, s_state, filename = file)
        outer_workflow = my_task.workflow.outer_workflow
        return SpiffWorkflow.Workflow(wf_spec, parent = outer_workflow)

    def _on_ready_before_hook(self, my_task):
        subworkflow    = self._create_subworkflow(my_task)
        subworkflow.completed_event.connect(self._on_subworkflow_completed, my_task)

        # Integrate the tree of the subworkflow into the tree of this workflow.
        my_task._sync_children(self.outputs, Task.FUTURE)
        
        # This was the cause of https://scalr-labs.atlassian.net/browse/FAM-11
        #for child in my_task.children:
        #    child.task_spec._update_state(child)
        #    child._inherit_data()

        for child in subworkflow.task_tree.children:
            my_task.children.insert(0, child)
            child.parent = my_task

        my_task._set_internal_data(subworkflow = subworkflow)


    def _on_ready_hook(self, my_task):
        # Assign variables, if so requested.
        subworkflow = my_task._get_internal_data('subworkflow')
        data = _eval_assign(self.in_assign, my_task)

        LOG.debug("Assign data to {0} and it's children: {1}".format(subworkflow, data))
        subworkflow.data = data
        for child in subworkflow.task_tree.children:
            child.set_data(**data)

        self._predict(my_task)
        for child in subworkflow.task_tree.children:
            child.task_spec._update_state(child)

    def _on_subworkflow_completed(self, subworkflow, my_task):
        data = _eval_assign(self.out_assign, subworkflow)

        # Assign variables, if so requested.
        for child in my_task.children:
            if child.task_spec in self.outputs:
                child._inherit_data()
                # assign out
                child.set_data(**data)

                '''
                for assignment in self.out_assign:
                    LOG.debug('Assign {2}.{0} from {3}.{1}'.format(
                        assignment.left_attribute, assignment.right_attribute, 
                        child, subworkflow
                    ))
                    assignment.assign(subworkflow, child)
                '''

                # Alright, abusing that hook is just evil but it works.        
                try:
                    child.task_spec._update_state_hook(child)
                except TaskError, e:
                    child.task_spec._on_error(child)

    def _on_complete_hook(self, my_task):
        # print 'SubWorkflow._on_complete_hook: {0}'.format(my_task)
        for child in my_task.children:
            if child.task_spec in self.outputs:
                continue
            child.task_spec._update_state(child)

    def serialize(self, serializer):
        return serializer._serialize_sub_workflow(self)

    @classmethod
    def deserialize(self, serializer, wf_spec, s_state):
        return serializer._deserialize_sub_workflow(wf_spec, s_state)
