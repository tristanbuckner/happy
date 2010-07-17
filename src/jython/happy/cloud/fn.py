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

import happy, happy.cloud
from java.util import Random
from java.util.regex import Pattern
from java.lang import StringBuilder

def _addMapOp(name, function):
    """
    Adds a named function as a map operation.
    """
    def op(pipe, sources=[]):
        return pipe.map(function, sources=sources)
    happy.cloud.addOperation(name, op)

def _addReduceOp(name, function):
    """
    Adds a named function as a reduce operation.
    """
    def op(pipe, sources=[]):
        return pipe.reduce(function, sources=sources)
    happy.cloud.addOperation(name, op)

############ MAP OPERATIONS ############

# count:
class LogCountFn:
    def __init__(self, counter):
        self.counter = counter

    def logCount(self, key, value):
        happy.results[self.counter] = happy.results.get(self.counter, 0) + 1
        yield key, value

def logCountOp(pipe, counter, sources=[]):
    return pipe.map(LogCountFn(counter).logCount, sources=sources)
happy.cloud.addOperation("logCount", logCountOp)

# sample:
class SampleFn:
    def __init__(self, samplerate, seed):
        self.samplerate = samplerate
        self.seed = seed
        self.random = None

    def sample(self, key, value):
        if self.random is None: self.random = Random(self.seed)
        if self.random.nextInt % self.samplerate == 0: yield key, value

def sampleOp(pipe, samplerate, seed=0, sources=[]):
    return pipe.map(SampleFn(samplerate).sample, sources=sources)
happy.cloud.addOperation("sample", sampleOp)

# setKey
class SetKeyFn:
    def __init__(self, query):
        self.query = query

    def getFn(self):
        if isinstance(self.query, list): return self.setKeys
        else: return self.setKey

    def setKey(self, key, value):
        v = value.get(self.query)
        if v is not None: yield v, value

    def setKeys(self, key, value):
        for k in self.query:
            v = value.get(k)
            if v is not None: yield v, value

def setKeyOp(pipe, query, sources=[]):
    return pipe.map(SetKeyFn(query).getFn(), sources=sources)
happy.cloud.addOperation("setKey", setKeyOp)

# transform
class TransformFn:
    def __init__(self, fn):
        eval(fn)
        self.fn = fn
        self.evalfn = None

    def transform(self, key, value):
        if self.evalfn is None: self.evalfn = eval(self.fn)
        ret = self.evalfn(key, value)
        if ret is not None: yield ret

def transformOp(pipe, fn, sources=[]):
    return pipe.map(TransformFn(fn).transform, sources=sources)
happy.cloud.addOperation("transform", transformOp)

# filter
class FilterFn:
    def __init__(self, fn, re, dict):
        # fn:
        if fn is not None:
            eval(fn)
        self.fn = fn
        self.evalfn = None
        # dict:
        if dict is not None: self.items = dict.items()
        else: self.items = None
        # re:
        if re is not None:
           Pattern.compile(re)
        self.re = re
        self.compiledRe = None

    def filterRe(self, key, value):
        if self.compiledRe is None: self.compiledRe = Pattern.compile(self.re)
        if self.compiledRe.matcher(value).find(): yield key, value

    def filterFn(self, key, value):
        if self.evalfn is None: self.evalfn = eval(self.fn)
        if self.evalfn(key, value): yield key, value

    def filterDict(self, key, value):
        for fk, fv in self.items:
            qv = value.get(fk)
            if qv is None or (fv is not None and qv != fv): return
        yield key, value

def filterOp(pipe, fn=None, dict=None, re=None, sources=[]):
    filter = FilterFn(fn=fn, dict=dict, re=re)
    if fn is not None: f = filter.filterFn
    elif dict is not None: f = filter.filterDict
    elif re is not None: f = filter.filterRe
    else: raise Exception("Filter requires an argumant")
    return pipe.map(f, sources=sources)
happy.cloud.addOperation("filter", filterOp)

# assert
class AssertTrueFn:
    def __init__(self, fn, message):
        eval(fn)
        self.fn = fn
        self.evalfn = None
        if message is None: self.message = fn
        else: self.message = message

    def assertTrue(self, key, value):
        if self.evalfn is None: self.evalfn = eval(self.fn)
        if self.evalfn(key, value) != True:
            raise happy.cloud.DataException("Assertion failed: " + self.message)
        else: yield key, value

def assertOp(pipe, fn, message=None, sources=[]):
    return pipe.map(AssertTrueFn(fn, message).assertTrue, sources=sources)
happy.cloud.addOperation("assertTrue", assertOp)

############ REDUCE OPERATIONS ############

# count:
def aggregateCountFn(key, values):
    counter = 0
    for value in values: counter += 1
    yield key, counter
_addReduceOp("aggregateCount", aggregateCountFn)

# sum:
def sumFn(key, values):
    currentSum = 0.0
    for value in values: currentSum += value
    yield key, currentSum
_addReduceOp("sum", sumFn)

# dedupe:
def dedupeFn(key, values):
    deduped = set()
    for value in values:
        serialValue = happy.json.encode(value)
        if serialValue not in deduped:
            deduped.add(serialValue)
            yield key, value
_addReduceOp("dedupe", dedupeFn)

# cat:
class CatFn:
    def __init__(self, sep):
        self.sep = sep

    def cat(self, key, values):
        sb = StringBuilder()
        for value in values:
            if sb.length > 0: sb.append(self.sep)
            sb.append(str(value))
        yield key, str(sb.toString())
def catOp(pipe, sep="", sources=[]):
    return pipe.reduce(CatFn(sep).cat, sources=sources)
happy.cloud.addOperation("cat", catOp)

# aggregate:
def aggregateFn(key, values):
    yield key, list(values)
_addReduceOp("aggregate", aggregateFn)

# aggregateDicts
def aggregateDictsFn(key, values):
    ret = {}
    for d in values:
        for k, v in d.iteritems():
            l = ret.get(k)
            if l is None: ret[k] = [v]
            else: l.append(v)
    yield key, ret
_addReduceOp("aggregateDicts", aggregateDictsFn)

# aggregateTriples
def aggregateTriplesFn(key, values):
    ret = {}
    for t in values:
        s = t["s"]
        v = ret.get(s)
        if v is None: ret[s] = [t["p"]]
        else: v.append(t["p"])
    yield key, ret
_addReduceOp("aggregateTriples", aggregateTriplesFn)

class EnumerateSourceValueFn:
    def __init__(self, id):
        self.id = id
    def enumerateSource(self, key, value):
        yield key, (self.id, value)

# sortBySources
def sortBySourceOp(pipe, reducer, sources=[]):
    if not isinstance(sources, list): sources = [sources]
    pipe = pipe.map(EnumerateSourceValueFn(0).enumerateSource)
    sources = [source.map(EnumerateSourceValueFn(i + 1).enumerateSource) for i, source in enumerate(sources)]
    return pipe.reduce(reducer, sources=sources, secondsort=True)
happy.cloud.addOperation("sortBySource", sortBySourceOp)

# aggregateBySources:
class AggregateBySourceFn:
    def __init__(self, numSources, innerjoin):
        self.numSources = numSources
        self.innerjoin = innerjoin

    def aggregateBySource(self, key, values):
        d = {}
        for i, value in values:
            l = d.get(i)
            if l is None: d[i] = [value]
            else: l.append(value)
        ret = []
        for i in xrange(self.numSources):
            l = d.get(i)
            if l is None:
                if self.innerjoin: return
                else: l = []
            ret.append(l)
        yield key, ret
def aggregateBySourceOp(pipe, innerjoin=False, sources=[]):
    if not isinstance(sources, list): sources = [sources]
    pipe = pipe.map(EnumerateSourceValueFn(0).enumerateSource)
    sources = [source.map(EnumerateSourceValueFn(i + 1).enumerateSource) for i, source in enumerate(sources)]
    return pipe.reduce(AggregateBySourceFn(numSources=len(sources) + 1, innerjoin=innerjoin).aggregateBySource, sources=sources)
happy.cloud.addOperation("aggregateBySource", aggregateBySourceOp)











