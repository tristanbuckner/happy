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

import sys, sets, threading, time, types
import happy, happy.dfs, happy.json
import java.lang
from java.io import File
from java.util import Collections, HashSet, ArrayList, Date
from java.text import SimpleDateFormat
from org.apache.hadoop.fs import Path
from com.freebase.happy.util import TextInt
from com.freebase.happy.cloud import FunctionChain, DataException

_log = happy.log.getLogger("HappyCloud")
_workdirDateFormat = SimpleDateFormat("yyMMdd-HHmmss")

# decorators:
def mapfn(fn):
    def op(pipe, sources=[]):
        return pipe.map(fn, sources=sources)
    addOperation(fn.__name__, op)
    return fn

def reducefn(fn):
    def op(pipe, sources=[]):
        return pipe.reduce(fn, sources=sources)
    addOperation(fn.__name__, op)
    return fn

# assert fn:
def assertTrue(value, message=None):
    if value != True:
        if message is None: message = "Data Assertion Failed"
        raise DataException(message)

# session operations:
_operations = {}

def addOperation(name, operation):
    """
    Adds a named operation that can be called on a Pipe object.
    """
    global _operations
    if name in _operations and _operations[name] != operation:
        raise Exception("Attempt to define operation " + name + " twice!")
    _operations[name] = operation

# figure out the script name:
if happy._scriptname is not None: _scriptname = File(happy._scriptname).getName()
else: _scriptname = "happy.cloud"

class Session:
    """
    Session is central class for specifying a series of Pipes.
    """
    def __init__(self, workpath=None):
        if workpath is None: self.workpath = self._getWorkPath()
        else: self.workpath = workpath
        self.sources = []
        self.pipes = []
        self.sinks = []
        self.pipecounter = 0
        self.jobcounter = 0

    def source(self, srcpaths, inputformat="auto", json=True, jobargs={}):
        if isinstance(srcpaths, basestring): srcpaths = [srcpaths]
        source = PipeSource(srcpaths, self, inputformat, json, jobargs=jobargs)
        self.sources.append(source)
        return source

    def run(self):
        # sanity check:
        if len(self.sources) == 0: raise Exception("No sources defined")
        if len(self.sinks) == 0: raise Exception("No sinks defined")

        # create a plan:
        specs = []
        pipemap = {}
        for sink in self.sinks:
            spec = JobSpec(self._jobid(), self.workpath)
            spec.outputpath = sink.sinkpath
            spec.outputformat = sink.outputformat
            spec.outputJson = sink.json
            spec.compressoutput = sink.compressoutput
            spec.compressiontype = sink.compressiontype
            specs.append(spec)
            if len(sink.sources) != 1: raise Exception("Sinks can only have one source: " + sink)
            self._walkPipe(spec, sink.sources[0], specs, pipemap)

        # sort out paths for jobs:
        self._configureJobs(specs)

        # run jobs:
        _log.info("Working directory is " + self.workpath)
        _log.info(str(len(specs)) + " job(s) found from " + str(len(self.pipes)) + " pipe action(s)")
        happy.dfs.delete(self.workpath)
        jobsDone = Collections.synchronizedSet(HashSet())
        jobResults = Collections.synchronizedList(ArrayList())
        jobsStarted = sets.Set()
        while jobsDone.size() < len(specs):
            # only keep 3 jobs in flight:
            for spec in specs:
                id = spec.id
                if id not in jobsStarted:
                    parentIds = [parent.id for parent in spec.parents]
                    if jobsDone.containsAll(parentIds):
                        thread = threading.Thread(name="Cloud Job " + str(id), target=self._runJob, args=(spec.getJob(), id, jobsDone, jobResults))
                        thread.setDaemon(True)
                        thread.start()
                        jobsStarted.add(id)
                if len(jobsStarted) - jobsDone.size() >= 3: break
            time.sleep(1)
        # compile results:
        results = {}
        for result in jobResults:
            for key, value in result.iteritems():
                results.setdefault(key, []).extend(value)
        # check for errors:
        if self.hasErrors():
            totalErrors = sum(results["happy.cloud.dataerrors"])
            _log.error("*** " + str(totalErrors) + " DataException errors were caught during this run, look in " + \
                self.workpath + "/errors to see details ***")
        return results


    def hasErrors(self):
        return happy.dfs.getFileSystem().exists(Path(self.workpath + "/errors"))

    def cleanup(self):
        if self.hasErrors():
            _log.error("*** Session.cleanup() failed because DataException errors were caught during this run, look in " + \
                self.workpath + "/errors to see details ***")
        else: happy.dfs.delete(self.workpath)

    def _runJob(self, job, jobId, jobsDone, jobResults):
        _log.info("Running job '" + job.jobname + "' with inputpaths " + str(job.inputpaths) + " and outputpath '" + str(job.outputpath) + "'")
        try:
            results = job.run()
        except java.lang.Exception, e:
            _log.error("Error running job", e)
            java.lang.System.exit(-1)
        except Exception, e:
            _log.error("Error running job: " + str(e))
            java.lang.System.exit(-1)
        if results is not None: jobResults.add(results)
        _log.info("Finished job " + job.jobname)
        jobsDone.add(jobId)

    def _getWorkPath(self):
        return _scriptname + "-" + _workdirDateFormat.format(Date())

    def _configureJobs(self, specs):
        for spec in specs:
            if spec.outputpath is None:
                spec.outputpath = self.workpath + "/job-" + str(spec.id)
            for child in spec.children:
                if spec.outputpath not in child.inputpaths:
                    child.inputpaths.append(spec.outputpath)

    def _walkPipe(self, spec, pipe, specs, pipemap):
        # there already is a job for this pipe:
        if pipe.id in pipemap:
            spec.parents.append(pipemap[pipe.id])
            pipemap[pipe.id].children.append(spec)
            return
        # this pipe is used by multiple streams, it requires a new job if it is not a no-op:
        if len(pipe.dests) > 1 and not isinstance(pipe, PipeSource) and not spec.isNoop():
            newspec = JobSpec(self._jobid(), self.workpath)
            spec.parents.append(newspec)
            newspec.children.append(spec)
            specs.append(newspec)
            spec = newspec
        # reducer:
        if isinstance(pipe, PipeReducer):
            if spec.reducer is None:
                spec.reducer = pipe.reducer
                spec.secondsort = pipe.secondsort
                # setting a reducer makes pre-maps post-maps:
                spec.postMappers = spec.preMappers
                spec.preMappers = []
            # if we already have a reducer, then start a new job:
            else:
                newspec = JobSpec(self._jobid(), self.workpath)
                newspec.reducer = pipe.reducer
                newspec.secondsort = pipe.secondsort
                spec.parents.append(newspec)
                newspec.children.append(spec)
                specs.append(newspec)
                spec = newspec
        # mapper:
        elif isinstance(pipe, PipeMapper):
            spec.preMappers.insert(0, pipe.mapper)
        # source:
        elif isinstance(pipe, PipeSource):
            spec.inputpaths.extend(pipe.srcpaths)
            spec.inputformat = pipe.inputformat
            spec.inputJson = pipe.json
            spec.jobargs = pipe.jobargs
        elif isinstance(pipe, PipeSink):
            raise Exception("Sink not expected " + str(pipe))
        else: raise Exception("Unknown Pipe " + str(pipe))
        # cache specs:
        if not isinstance(pipe, PipeSource): pipemap[pipe.id] = spec
        # if there is only one preceding source:
        if isinstance(pipe, PipeSource): pass
        elif len(pipe.sources) == 1:
            self._walkPipe(spec, pipe.sources[0], specs, pipemap)
        # multiple preceding sources:
        elif len(pipe.sources) > 1:
            for nextpipe in pipe.sources:
                if isinstance(nextpipe, PipeSource):
                    self._walkPipe(spec, nextpipe, specs, pipemap)
                elif isinstance(nextpipe, PipeMapper) or isinstance(nextpipe, PipeReducer):
                    newspec = JobSpec(self._jobid(), self.workpath)
                    spec.parents.append(newspec)
                    newspec.children.append(spec)
                    specs.append(newspec)
                    self._walkPipe(newspec, nextpipe, specs, pipemap)
                else: raise Exception("Unknown Pipe " + str(nextpipe))
        else:
            raise Exception("Pipe is missing source " + str(pipe))

    def _pipeid(self):
        ret = self.pipecounter
        self.pipecounter += 1
        return ret

    def _jobid(self):
        ret = self.jobcounter
        self.jobcounter += 1
        return ret

class Pipe:
    """
    Base class for pipes.
    """
    def map(self, mapper, sources=[]):
        if isinstance(self, PipeSink): raise Exception("Cannot call map() on a PipeSink")
        self._verifyfn(mapper)
        if not isinstance(sources, list): sources = [sources]
        pipe = PipeMapper([self] + sources, mapper, self.session)
        self.session.pipes.append(pipe)
        self.dests.append(pipe)
        return pipe

    def reduce(self, reducer, sources=[], secondsort=False):
        if isinstance(self, PipeSink): raise Exception("Cannot call reduce() on a PipeSink")
        self._verifyfn(reducer)
        if not isinstance(sources, list): sources = [sources]
        pipe = PipeReducer([self] + sources, reducer, self.session, secondsort)
        self.session.pipes.append(pipe)
        self.dests.append(pipe)
        return pipe

    def sink(self, sinkpath, outputformat="text", json=True, compressoutput=False, compressiontype=None):
        if isinstance(self, PipeSink): raise Exception("You cannot call sink() on a PipeSink")
        pipe = PipeSink([self], sinkpath, self.session, outputformat, json, compressoutput, compressiontype)
        self.session.sinks.append(pipe)
        self.dests.append(pipe)
        return pipe

    def run(self):
        self.session.run()

    def _verifyfn(self, fn):
        if isinstance(fn, types.FunctionType) or isinstance(fn, types.MethodType): return
        raise Exception("Not a valid function: " + fn)

    def __getattr__(self, name):
        global _operations
        operation = _operations.get(name)
        if operation is None: raise Exception("Invalid operation " + name)
        def function(**kwargs):
            return operation(self, **kwargs)
        return function

    def __str__(self):
        return "<Pipe" + str(self.__dict__) + ">"

    def __repr__(self):
        return "<Pipe" + str(self.__dict__) + ">"

class PipeSource(Pipe):
    def __init__(self, srcpaths, session, inputformat, json, jobargs):
        self.srcpaths = srcpaths
        self.session = session
        self.dests = []
        self.id = session._pipeid()
        self.inputformat = inputformat
        self.json = json
        self.jobargs = jobargs

class PipeSink(Pipe):
    def __init__(self, sources, sinkpath, session, outputformat, json, compressoutput, compressiontype):
        self.sources = sources
        self.sinkpath = sinkpath
        self.session = session
        self.dests = []
        self.id = session._pipeid()
        self.outputformat = outputformat
        self.json = json
        self.compressoutput = compressoutput
        self.compressiontype = compressiontype

class PipeMapper(Pipe):
    def __init__(self, sources, mapper, session):
        self.sources = sources
        self.mapper = mapper
        self.session = session
        self.dests = []
        self.id = session._pipeid()

class PipeReducer(Pipe):
    def __init__(self, sources, reducer, session, secondsort=False):
        self.sources = sources
        self.reducer = reducer
        self.secondsort = secondsort
        self.session = session
        self.dests = []
        self.id = session._pipeid()

class JobSpec:
    """
    A spec for a PipeJob.
    """
    def __init__(self, id, workpath):
        self.id = id
        self.workpath = workpath
        self.inputpaths = []
        self.inputformat = "auto"
        self.inputJson = True
        self.outputformat = "sequence"
        self.outputJson = True
        self.compressoutput = True
        self.compressiontype = None
        self.outputpath = None
        self.preMappers = []
        self.reducer = None
        self.secondsort = False
        self.postMappers = []
        self.children = []
        self.parents = []
        self.jobargs = {}

    def isNoop(self):
        return len(self.preMappers) == 0 and self.reducer is None and len(self.postMappers) == 0

    def getJob(self):
        return PipeJob(self)

    def __str__(self):
        return "<JobSpec" + str(self.__dict__) + ">"

    def __repr__(self):
        return "<JobSpec" + str(self.__dict__) + ">"

class PipeJob(happy.HappyJob):
    """
    The job that executes a series of pipes.
    """
    def __init__(self, spec):
        happy.HappyJob.__init__(self)
        self.id = spec.id
        self.inputpaths = spec.inputpaths
        self.inputformat = spec.inputformat
        self.inputJson = spec.inputJson
        self.outputpath = spec.outputpath
        self.outputformat = spec.outputformat
        self.compressoutput = spec.compressoutput
        if spec.compressiontype is not None: self.compressiontype = spec.compressiontype
        self.jobargs = spec.jobargs
        self.outputJson = spec.outputJson
        self.preMappers = spec.preMappers[:]
        self.reducer = spec.reducer
        if self.reducer is None: self.reducetasks = 0
        self.postMappers = spec.postMappers[:]
        self.errorpath = spec.workpath + "/errors"
        self.errorcollectors = {}
        # build a job name:
        prenames = [f.__name__ for f in self.preMappers]
        if self.reducer is not None: reducername = [self.reducer.__name__]
        else: reducername = []
        postnames = [f.__name__ for f in self.postMappers]
        self.jobname = _scriptname + " " + str(spec.id) + " " + "-".join(prenames + reducername + postnames)
        # config second sort:
        self.secondsort = spec.secondsort
        if self.secondsort:
            self.jobargs["mapred.output.value.groupfn.class"] = "com.freebase.happy.util.TextInt$TextComparator"
            self.jobargs["mapred.partitioner.class"] = "com.freebase.happy.util.TextInt$TextPartitioner"
            self.mapoutputkey = "com.freebase.happy.util.TextInt"
        # init function chains:
        self.mapFunctionChain = None
        self.reduceFunctionChain = None

    def mapconfig(self):
        self.jobstage = "map"

    def map(self, records, task):
        if len(self.preMappers) == 0 and not self.secondsort:
            for key, record in records: task.collect(key, record)
        else:
            if self.mapFunctionChain is None:
                # set up mapper input fn:
                if self.inputJson:
                    def mapperfn(_, records):
                        for key, value in records: yield key, happy.json.decode(value)
                else:
                    def mapperfn(_, records):
                        return records
                # emitting raw text:
                if self.reducetasks == 0 and not self.outputJson:
                    def collector(k, v):
                        task.collect(k, v)
                # secondary sort:
                elif self.secondsort:
                    textint = TextInt()
                    def collector(k, v):
                        if len(v) != 2 or not isinstance(v[0], int):
                            raise Exception("Invalid value " + str(v) + " for a secondary sort, (<int>, <obj>) tuple required")
                        textint.setString(k)
                        textint.setInt(v[0])
                        task.collect(textint, happy.json.encode(v))
                # json output:
                else:
                    def collector(k, v):
                        task.collect(k, happy.json.encode(v))
                self.mapFunctionChain = FunctionChain([mapperfn] + self.preMappers + [collector], self._recordError)
            # do the work:
            self.mapFunctionChain.callChain(None, records)

    def reduceconfig(self):
        self.jobstage = "reduce"

    def reduce(self, key, values, task):
        if self.reducer is None:
            for value in values: task.collect(key, value)
        else:
            if self.reduceFunctionChain is None:
                # emitting raw text:
                if not self.outputJson:
                    def collector(k, v):
                        task.collect(k, v)
                # json output:
                else:
                    def collector(k, v):
                        task.collect(k, happy.json.encode(v))
                self.reduceFunctionChain = FunctionChain([self.reducer] + self.postMappers + [collector], self._recordError)
            # second sort key:
            if self.secondsort: key = key.getString()
            # do the work:
            self.reduceFunctionChain.callChain(key, self._jsonReduceIterator(values))

    def _recordError(self, key, value, message, operation):
        """
        Records an error to the log and self.errorcollector.
        """
        errorcollector = self.errorcollectors.get(operation)
        if errorcollector is None:
            self.errorcollectors[operation] = errorcollector = \
                happy.dfs.createPartitionedCollector(self.errorpath + "/job-" + str(self.id) + "-" + self.jobstage + "-" + operation, type="text")
        errorcollector.collect(key, happy.json.encode({"key":key, "value":value, "operation": operation, "error": message}))
        currentErrors = happy.results.get("happy.cloud.dataerrors")
        if currentErrors is None: currentErrors = 1
        else: currentErrors += 1
        happy.results["happy.cloud.dataerrors"] = currentErrors

    def _jsonReduceIterator(self, records):
        for encodedRecord in records: yield happy.json.decode(encodedRecord)

    def run(self):
        happy.dfs.delete(self.outputpath)
        return happy.HappyJob.run(self)

