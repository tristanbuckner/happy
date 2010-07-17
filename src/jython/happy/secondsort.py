#========================================================================
# Copyright (c) 2008, Metaweb Technologies, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY METAWEB TECHNOLOGIES ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL METAWEB TECHNOLOGIES BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ========================================================================
#

import happy
from com.freebase.happy.util import TextInt

class TaskWrapper:
    def __init__(self, task):
        self.textint = TextInt()
        self.task = task

    def collect(self, key, order, value):
        self.textint.setString(key)
        self.textint.setInt(order)
        self.task.collect(self.textint, value)

    def getInputPath(self):
        return self.task.getInputPath()

    def progress(self):
        self.task.progress()

    def setStatus(self, message):
        self.task.setStatus()

class SecondSortJob(happy.HappyJob):
    def __init__(self):
        happy.HappyJob.__init__(self)
        self._sinitialized = True
        self.jobargs["mapred.output.value.groupfn.class"] = "com.freebase.happy.util.TextInt$TextComparator"
        self.jobargs["mapred.partitioner.class"] = "com.freebase.happy.util.TextInt$TextPartitioner"
        self.mapoutputkey = "com.freebase.happy.util.TextInt"

    def map(self, records, task):
        self.smap(records, TaskWrapper(task))

    def reduce(self, key, values, task):
        skey = key.getString()
        self.sreduce(skey, values, task)

    def run(self, target=None):
        # by default, we try to run ourselves if no target is given.
        if target is None: target = self

        # did the initializer get called?
        if not hasattr(self, "_sinitialized"): raise Exception("SecondSortJob.__init__() must be called before SecondSortJob.run()")
        if not hasattr(target.__class__, "smap"): raise Exception("Target is missing smap function: " + str(target))
        if not hasattr(target.__class__, "sreduce"): raise Exception("Target is missing sreduce function: " + str(target))
        return happy.HappyJob.run(self, target)
