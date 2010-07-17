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

import sys, happy.log
from java.io import File
from java.lang import System, Class
from org.apache.hadoop.fs import Path
from org.apache.hadoop.io import Text
from org.apache.hadoop.mapred import JobConf, TextInputFormat, KeyValueTextInputFormat, SequenceFileInputFormat, TextOutputFormat, SequenceFileOutputFormat, FileInputFormat, FileOutputFormat
from org.apache.hadoop.mapred import HappyJobClient
from com.freebase.happy import HappyBase, HappyMap, HappyCombine, HappyReduce, HappyJobRunner
from com.freebase.happy.util import JarUtil, PyObjectSerializer, TextOrSequenceInputFormat, ResultSerializer
from com.freebase.happy.json import JSONWritable, JSONInputFormat

_log = happy.log.getLogger("HappyJob")

# global include paths that are included in all jobs:
if len(sys.argv) > 0:
    _scriptname = sys.argv[0]
    path = [_scriptname]
else:
    _scriptname = None
    path = []

# parse out the include paths in the environment:
for includepath in System.getProperty("happy.path", "").split(":"):
    if len(includepath) > 0: path.append(includepath)

# this contains current job information when a job is running on the server.
job = None
_jobconf = None

# This contains result data that will be sent back to the client job.
results = {}

# This sets whether all jobs and the filesystem are set to be local - good for debugging:
allLocalJobs = False
allLocalFs = False

def getJobConf():
    """
    Tries to find a global JobConf, or creates one if needed.
    """
    global _jobconf
    if _jobconf is None:
        global job
        if job is not None: _jobconf = job.getJobConf()
        else: _jobconf = JobConf()
    return _jobconf

class HappyJob(object):
    """
    Runner class for Hadoop Map/Reduce classes.
    Job classes can either inherit from this class or be passed in to the run() method.
    """

    def __init__(self):
        """
        Initializes fields for the job.  Be sure to call this initializer if you subclass.
        """
        self._initialized = True
        self.jobname = None
        self.inputpaths = None
        self.outputpath = None
        self.inputformat = "auto"
        self.outputformat = "text"
        self.compressoutput = False
        self.compressiontype = None
        self.sequencetype = "BLOCK"
        self.localjob = allLocalJobs
        self.localfs = allLocalFs
        self.includepaths = []
        self.maptasks = None
        self.reducetasks = None
        self.mapoutputkey = "text"
        self.mapoutputvalue = "text"
        self.outputkey = "text"
        self.outputvalue = "text"
        self.jobargs = {}

    def run(self, target=None):
        """
        Runs a job using the variables specified in this class.
        If a target isn't specified, self is used for the job.
        """
        # did the initializer get called?
        if not hasattr(self, "_initialized"): raise Exception("HappyJob.__init__() must be called before HappyJob.run()")

        # are we in a job?
        global job
        if job is not None: raise Exception('run() cannot be called inside of a running job.  Did you check if __name__=="__main__"?')

        # by default, we try to run ourselves if no target is given.
        if target is None: target = self

        # sanity checking:
        if not hasattr(target.__class__, "map"): raise Exception("Target is missing map function: " + str(target))
        if (self.reducetasks > 0 or self.reducetasks is None) and not hasattr(target.__class__, "reduce"): raise Exception("Target is missing reduce function: " + str(target))

        jobconf = JobConf()

        # set the filesystem:
        if self.localfs: jobconf.set("fs.default.name", "file:///")

        # set the script:
        if _scriptname is not None:
            localscriptname = File(_scriptname).getName()
            jobconf.set(HappyBase.SCRIPT_KEY, localscriptname)

        # check if the script asks for a local job:
        localjob = self.localjob
        if localjob: jobconf.set("mapred.job.tracker", "local")
        # check if that is how the tracker is configured:
        elif jobconf.get("mapred.job.tracker") == "local": localjob = True

        # set the name:
        jobname = self.jobname
        if jobname is None: jobname = localscriptname + " - " + self.__class__.__name__
        jobconf.setJobName(jobname)

        if self.maptasks is not None: jobconf.setNumMapTasks(self.maptasks)
        if self.reducetasks is not None: jobconf.setNumReduceTasks(self.reducetasks)

        # input and output paths:
        if self.inputpaths is None or len(self.inputpaths) == 0: raise Exception("No inputpaths specified")
        if not isinstance(self.inputpaths, list): inputpaths = [self.inputpaths]
        else: inputpaths = self.inputpaths
        for inputpath in inputpaths: FileInputFormat.addInputPath(jobconf, Path(inputpath))
        if self.outputpath is None: raise Exception("No outputpath specified")
        FileOutputFormat.setOutputPath(jobconf, Path(self.outputpath))

        # input formats:
        if self.inputformat == "text": jobconf.setInputFormat(TextInputFormat)
        elif self.inputformat == "keyvalue": jobconf.setInputFormat(KeyValueTextInputFormat)
        elif self.inputformat == "json": jobconf.setInputFormat(JSONInputFormat)
        elif self.inputformat == "sequence": jobconf.setInputFormat(SequenceFileInputFormat)
        elif self.inputformat == "auto": jobconf.setInputFormat(TextOrSequenceInputFormat)
        else: jobconf.setInputFormat(Class.forName(self.inputformat))

        # output formats:
        if self.outputformat == "text": jobconf.setOutputFormat(TextOutputFormat)
        elif self.outputformat == "sequence":
            jobconf.setOutputFormat(SequenceFileOutputFormat)
        elif self.outputformat == "mapdir": jobconf.setOutputFormat(Class.forName("org.apache.hadoop.mapred.MapFileOutputFormat"))
        else: jobconf.setOutputFormat(Class.forName(self.outputformat))

        # compression output:
        if self.compressoutput:
            jobconf.set("mapred.output.compress", "true")
            codec = None
            if self.compressiontype == "zlib": codec = "org.apache.hadoop.io.compress.DefaultCodec"
            elif self.compressiontype == "gzip": codec = "org.apache.hadoop.io.compress.GzipCodec"
            elif self.compressiontype == "lzo": codec = "org.apache.hadoop.io.compress.LzoCodec"
            if codec is not None: jobconf.set("mapred.output.compression.codec", codec)
        if self.sequencetype == "BLOCK" or self.sequencetype == "RECORD":
            jobconf.set("mapred.output.compression.type", self.sequencetype)

        # set the map and reduce runners:
        jobconf.setMapRunnerClass(HappyMap)
        if hasattr(target.__class__, "combine"): jobconf.setCombinerClass(HappyCombine)
        jobconf.setReducerClass(HappyReduce)

        # configure key, value types:
        def getOutputType(t):
            if t == None or t == "text": return Text
            elif t == "json": return JSONWritable
            else: return Class.forName(t)
        jobconf.setOutputKeyClass(getOutputType(self.outputkey))
        jobconf.setOutputValueClass(getOutputType(self.outputvalue))
        jobconf.setMapOutputKeyClass(getOutputType(self.mapoutputkey))
        jobconf.setMapOutputValueClass(getOutputType(self.mapoutputvalue))

        # speculative execution off for now:
        jobconf.setSpeculativeExecution(False)

        # serialize this object:
        scriptobject = PyObjectSerializer.serialize(target)
        scriptobject.deleteOnExit()
        _log.info("Job state serialized to " + scriptobject.getAbsolutePath())
        if localjob: scriptobjectPath = scriptobject.getAbsolutePath()
        else: scriptobjectPath = scriptobject.getName()
        jobconf.set(HappyBase.SCRIPT_OBJECT, scriptobjectPath)

        # set up the happy python path:
        global path
        if localjob:
            jobpythonpath = [includepath for includepath in path if not includepath.endswith(".jar")]
        else:
            jobpythonpath = [File(includepath).getName() for includepath in path if not includepath.endswith(".jar")]
        jobconf.set(HappyBase.PATH_KEY, ":".join(jobpythonpath))

        # set up other resources:
        resourcefiles = self.includepaths[:]
        resourcefiles.append(scriptobject.getAbsolutePath())
        if localjob:
            localIncludePaths = [includepath for includepath in resourcefiles if not includepath.endswith(".jar")]
        else:
            localIncludePaths = [File(includepath).getName() for includepath in resourcefiles if not includepath.endswith(".jar")]
        jobconf.set(HappyBase.RESOURCE_KEY, ":".join(localIncludePaths))

        # sort all of the paths into jars and files:
        allpaths = path + self.includepaths
        includeJars = [JarUtil.findContainingPath(HappyJobRunner)]
        includeFiles = [scriptobject.getAbsolutePath()]
        for includepath in allpaths:
            if includepath.endswith(".jar"): includeJars.append(includepath)
            else: includeFiles.append(includepath)

        # create a jar file to ship out if it isn't a local job:
        if not localjob:
            # create a temp job jar:
            jobJar = File.createTempFile("happy-", ".jar")
            jobJar.deleteOnExit()
            jobJarPath = jobJar.getAbsolutePath()
            # package it up:
            JarUtil.buildJar(includeJars, includeFiles, jobJar.getAbsolutePath())
            jobconf.setJar(jobJarPath)

        # add additional job arguments:
        for key, value in self.jobargs.iteritems(): jobconf.set(key, value)

        # run the job:
        finishedJob = HappyJobClient.runJob(jobconf)
        if not finishedJob.isSuccessful():
            raise Exception("Job " + jobname + " failed")

        # return results:
        return ResultSerializer.deserialize(jobconf)





