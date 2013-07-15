
import time
from nose.tools import nottest

from SpiffWorkflow import Workflow, Task
from SpiffWorkflow.specs import Transform, WorkflowSpec, StartTask, Simple, ErrorHandler
from SpiffWorkflow.exceptions import TaskError

def run_workflow(spec):
    wflow = Workflow(spec)
    for _ in range(0, 4):
        wflow.task_tree.dump()
        print '----'
        wflow.complete_next(False)


def create_worlflow():
    spec = WorkflowSpec()
    a1 = Transform(spec, 'do what you want', [
            'from SpiffWorkflow.exceptions import TaskError\n' + 
            'if len(my_task.parent.children) < 2: raise TaskError("test")'])
    a1.follow(spec.start)

    return spec, a1    


@nottest
def test_1_fail():
    spec, a1 = create_worlflow()

    a2 = Simple(spec, 'Next task post error')
    a2.follow(a1)

    eh = ErrorHandler(spec)

    c1 = Simple(spec, 'comp 1')
    c1.follow(eh)

    #c2 = Simple(spec, 'comp 2')
    #c2.follow(eh)

    a1.connect_error_handler(eh)

    run_workflow(spec)

@nottest
def test_2_complete():
    spec, a1 = create_worlflow()

    eh = ErrorHandler(spec, resolution='complete')

    c1 = Simple(spec, 'compensator #1')
    c1.follow(eh)
    a1.connect_error_handler(eh)

    a2 = Simple(spec, 'Next task post error')
    a2.follow(a1)

    run_workflow(spec)


def test_3_retry():
    spec, a1 = create_worlflow()

    eh = ErrorHandler(spec, resolution='retry')

    c1 = Simple(spec, 'compensator #1')
    c1.follow(eh)
    a1.connect_error_handler(eh)

    a2 = Simple(spec, 'Next task post error')
    a2.follow(a1)

    run_workflow(spec)


