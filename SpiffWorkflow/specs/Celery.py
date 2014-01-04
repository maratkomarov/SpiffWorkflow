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
from __future__ import absolute_import

import logging

from SpiffWorkflow.Task import Task
from SpiffWorkflow.exceptions import WorkflowException, TaskError
from SpiffWorkflow.specs.TaskSpec import TaskSpec
from SpiffWorkflow.operators import valueof, Attrib, PathAttrib, FuncAttrib
from SpiffWorkflow.util import merge_dictionary

try:
    from celery.app import current_app
    from celery.result import AsyncResult
except ImportError:
    have_celery = False
else:
    have_celery = True


LOG = logging.getLogger(__name__)


def _eval_args(args, my_task):
    """Parses args and evaluates any Attrib entries"""
    results = []
    for arg in args:
        if isinstance(arg, Attrib) or isinstance(arg, PathAttrib) \
                or isinstance(arg, FuncAttrib):
            results.append(valueof(my_task, arg))
        else:
            results.append(arg)
    return results


def _eval_kwargs(kwargs, my_task):
    """Parses kwargs and evaluates any Attrib entries"""
    results = {}
    for kwarg, value in kwargs.iteritems():
        if isinstance(value, Attrib) or isinstance(value, PathAttrib) \
                or isinstance(value, FuncAttrib):
            results[kwarg] = valueof(my_task, value)
        else:
            results[kwarg] = value
    return results


def Serializable(o):
    """Make sure an object is JSON-serializable
    Use this to return errors and other info that does not need to be
    deserialized or does not contain important app data. Best for returning
    error info and such"""
    if type(o) in [basestring, dict, int, long]:
        return o
    else:
        try:
            s = json.dumps(o)
            return o
        except:
            LOG.debug("Got a non-serilizeable object: %s", o)
            return o.__repr__()


class Celery(TaskSpec):
    # FIXME: Handle case when agent receive unregistered task. Now _try_fire got PENDING state forever, and task hangs

    """This class implements a celeryd task that is sent to the celery queue for
    completion."""

    def __init__(self, parent, name, call=None, call_args=None, call_queue=None, call_server_id=None,
                call_result_key=None, merge_results=True, **kwargs):
        """Constructor.

        The args/kwargs arguments support Attrib classes in the parameters for
        delayed evaluation of inputs until run-time. Example usage:
        task = Celery(wfspec, 'MyTask', 'celery.call',
                 call_args=['hello', 'world', Attrib('number')],
                 any_param=Attrib('result'))

        For serialization, the celery task_id is stored in internal_data,
        but the celery async call is only storred as an attr of the task (since
        it is not always serializable). When deserialized, the async_call attr
        is reset in the _try_fire call.

        :type  parent: TaskSpec
        :param parent: A reference to the parent task spec.
        :type  name: str
        :param name: The name of the task spec.
        :type  call: str
        :param call: The name of the celery task that needs to be called.
        :type  call_args: list
        :param call_args: args to pass to celery task.
        :type  call_result_key: str
        :param call_result_key: The key to use to store the results of the call in
                task.data. If None, then dicts are expanded into
                data and values are stored in 'result'.
        :param merge_results: merge the results in instead of overwriting existing
                fields.
        :type  kwargs: dict
        :param kwargs: kwargs to pass to celery task.
        """
        if not have_celery:
            raise Exception("Unable to import python-celery imports.")
        assert parent  is not None
        assert name    is not None
        if call is None:
            call = name

        TaskSpec.__init__(self, parent, name, **kwargs)
        self.description = kwargs.pop('description', '')
        self.call = call
        self.args = call_args
        self.call_queue = call_queue
        self.call_server_id = call_server_id
        self.merge_results = merge_results
        skip = 'data', 'defines', 'pre_assign', 'post_assign', 'lock'
        self.kwargs = dict(i for i in kwargs.iteritems() if i[0] not in skip)
        self.call_result_key = call_result_key
        LOG.debug("Celery task '%s' created to call '%s'", name, call)

    def _send_call(self, my_task):
        """Sends Celery asynchronous call and stores async call information for
        retrieval laster"""
        args, kwargs, queue = [], {}, None
        if self.args:
            args = _eval_args(self.args, my_task)
        if self.kwargs:
            kwargs = _eval_kwargs(self.kwargs, my_task)
        if self.call_server_id:
            queue = 'server.{0}'.format(valueof(my_task, self.call_server_id))
        elif self.call_queue:
            queue = valueof(my_task, self.call_queue)
        LOG.debug("%s (task id %s) calling %s", self.name, my_task.id,
                self.call, extra=dict(data=dict(args=args, kwargs=kwargs)))
        # Add current workflow information
        kwargs['workflow'] = {
            'data': my_task.workflow.data
        }
        async_call = current_app().send_task(self.call, args=args, kwargs=kwargs, queue=queue)
        my_task.internal_data['task_id'] = async_call.task_id
        my_task.internal_data['async_call'] = async_call
        LOG.debug("'%s' called: %s", self.call, async_call.task_id)

    def _retry_fire(self, my_task):
        """ Abort celery task and retry it"""
        if not my_task._has_state(Task.WAITING):
            raise WorkflowException(my_task, "Cannot refire a task that is not"
                    "in WAITING state")
        # Check state of existing call and abort it (save history)
        if my_task.internal_data.get('task_id') is not None:
            if not 'async_call' in my_task.internal_data:
                task_id = my_task.internal_data['task_id']
                my_task.internal_data['async_call'] = default_app.AsyncResult(task_id)
                my_task.internal_data['deserialized'] = True
                my_task.internal_data['async_call'].state  # manually refresh
            async_call = my_task.internal_data['async_call']
            if async_call.state == 'FAILED':
                pass
            elif async_call.state in ['RETRY', 'PENDING', 'STARTED']:
                async_call.revoke()
                LOG.info("Celery task '%s' was in %s state and was revoked", 
                    async_call.state, async_call)
            elif async_call.state == 'SUCCESS':
                LOG.warning("Celery task '%s' succeeded, but a refire was "
                        "requested", async_call)
            self._clear_celery_task_data(my_task)
        # Retrigger
        return self._try_fire(my_task)

    def _clear_celery_task_data(self, my_task):
        """ Clear celery task data """
        # Save history
        if 'task_id' in my_task.internal_data:
            # Save history for diagnostics/forensics
            history = my_task.internal_data.get('task_history', [])
            history.append(my_task.internal_data['task_id'])
            del my_task.internal_data['task_id']
            my_task.internal_data['task_history'] = history
        if 'error' in my_task.data: # ?
            del my_task.data['error']  #?
        if 'async_call' in my_task.internal_data:
            del my_task.internal_data['async_call']
        if 'deserialized' in my_task.internal_data:
            del my_task.internal_data['deserialized']

    def _try_fire(self, my_task, force=False):
        """Returns False when successfully fired, True otherwise"""

        # Deserialize async call if necessary
        if not 'async_call' in my_task.internal_data and \
                my_task.internal_data.get('task_id'):
            task_id = my_task.internal_data['task_id']
            my_task.internal_data['async_call'] = default_app.AsyncResult(task_id)
            my_task.internal_data['deserialized'] = True
            LOG.debug("Reanimate AsyncCall %s", task_id)

        # Make the call if not already done
        if 'async_call' not in my_task.internal_data:
            self._send_call(my_task)

        # Get call status (and manually refresh if deserialized)
        if my_task.internal_data.get('deserialized'):
            my_task.internal_data['async_call'].state  # must manually refresh if deserialized
        async_call = my_task.internal_data['async_call']
        LOG.debug('AsyncCall.state: %s', async_call.state)
        if async_call.state == 'FAILURE':
            LOG.debug("Async Call for task '%s' failed: %s", 
                    my_task.get_name(), async_call.result)
            my_task.internal_data['error'] = async_call.result
            if not isinstance(async_call.result, TaskError):
                raise TaskError(async_call.result)
            else:
                raise async_call.result

        elif async_call.state == 'PROGRESS':
            # currently the console outputs "WAITING" for this state
            # so we expect to see "WAITING, 50%"

            #try:
            progress = result["progress"]
            #except TypeError:
            #    # WTF, the task was clearly setting meta to dict:
            #    # progress = result["percentage"]
            #    # TypeError: string indices must be integers
            LOG.debug("Meta: %s", meta)
            LOG.debug("progress=%s, TryFire for '%s' returning False", progress, my_task.get_name())
            my_task.progress = progress

        elif async_call.ready():
            result = async_call.result
            if isinstance(async_call.result, Exception):
                LOG.warn("Celery call %s failed: %s", self.call, result)
                my_task.data['error'] = result
                return False
            LOG.debug("Completed celery call %s with result=%s", self.call,
                    result)
            # Format result
            if self.call_result_key:
                data = data_i = {}
                path = self.call_result_key.split('.')
                for key in path[:-1]:
                    data_i[key] = {}
                    data_i = data_i[key]
                data_i[path[-1]] = result
            else:
                if isinstance(result, dict):
                    data = result
                else:
                    data = {'result': result}
            # Load formatted result into attributes
            if self.merge_results:
                merge_dictionary(my_task.data, data)
            else:
                my_task.set_data(**data)
            return True
        else:
            LOG.debug("async_call.ready()=%s. TryFire for '%s' "
                    "returning False", async_call.ready(), my_task.get_name())
            return False

    def _update_state_hook(self, my_task):
        if not self._try_fire(my_task):
            if not my_task._has_state(Task.WAITING):
                LOG.debug("'%s' going to WAITING state", my_task.get_name())
                my_task.state = Task.WAITING
            return
        super(Celery, self)._update_state_hook(my_task)

    def serialize(self, serializer):
        return serializer._serialize_celery(self)

    @classmethod
    def deserialize(self, serializer, wf_spec, s_state):
        spec = serializer._deserialize_celery(wf_spec, s_state)
        return spec
