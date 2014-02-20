Spiff Workflow
==============
Spiff Workflow is a library implementing a framework for workflows.
It is based on http://www.workflowpatterns.com and implemented in pure Python.

In addition, Spiff Workflow provides a parser and workflow emulation
layer that can be used to create executable Spiff Workflow specifications
from Business Process Model and Notation (BPMN) documents.

For documentation please refer to:

  https://github.com/knipknap/SpiffWorkflow/wiki


Contact
-------
Mailing List: http://groups.google.com/group/spiff-devel/



Data Patterns Implementation
----------------------------

Workflow.data - implements [Workflow Data](http://www.workflowpatterns.com/patterns/data/visibility/wdp7.php)
SubWorkflow.in_assign, out_assign - implements [Block Data](http://www.workflowpatterns.com/patterns/data/visibility/wdp2.php)
Task.data - implements [Task Data](http://www.workflowpatterns.com/patterns/data/visibility/wdp1.php)
Error handler is executed in a subworkflow. It's block Data inherited from failed task, and not comes back to parent workflow