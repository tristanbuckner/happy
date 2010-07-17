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
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULARmore flo    
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL METAWEB TECHNOLOGIES BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ========================================================================

import happy
import random
import re
from happy.log import logger

class IdentityJob(happy.HappyJob):
    """
    IdenityJobs pass the input through to the output unchanged.  
    
    Useful for collating several flows into a single file, as well as testing.
    """
    def __init__(self):
        happy.HappyJob.__init__(self)
        self.reducetasks = 0

    def map(self, records, task):
        for key, value in records:
            task.collect(key, value)


class SelfJoin(happy.secondsort.SecondSortJob):
    """
    Join a dataset to itself on the two passed keys.
    
    Useful for graph path enumerations in triple files:
        job=SelfJoin(joinkey1='o', joinkey2='s')
    """
    def __init__(self, joinkey1, joinkey2):
        """
        @param joinkey1: the first key in the JSON value to join with
        @param joinkey2: the second key in the JSON value to join against
        """
        happy.secondsort.SecondSortJob.__init__(self)
        self.joinkey1 = joinkey1
        self.joinkey2 = joinkey2

    def smap(self, records, task):
        """
        First, we pivot both files on their join keys, and set the
        joinorder for the secondsort such that all joinkey1 records
        come first.
        """
        for key, json in records:
            record = happy.json.decode(json)
            if record.has_key(self.joinkey1):
                record['__joinorder__'] = 1
                task.collect(record[self.joinkey1], 1, happy.json.encode(record))
            if record.has_key(self.joinkey2):
                record['__joinorder__'] = 2
                task.collect(record[self.joinkey2], 2, happy.json.encode(record))

    def sreduce(self, key, values, task):
        """
        Now calculate the cartesian product by iterating over all records
        grouped together on the join key.
        """
        list1 = []
        for json in values:
            record = happy.json.decode(json)
            order = record['__joinorder__']
            newrec = {}
            for key in record.keys():
                if (key != '__joinorder__'):
                    newkey = "%s%s" % (key, order)
                    newrec[newkey] = record[key]
            if (order==1):
                list1.append(newrec)
            else:
                try:
                    for i in xrange(len(list1)):
                        r = list1[i]
                        emitrec = {}
                        emitrec.update(newrec)
                        emitrec.update(r)
                        task.collect(key, happy.json.encode(emitrec))
                except:
                    logger.error("JOIN FAILED ON RECORD: (%s, %s)" % (key, json))


class InnerJoin(happy.secondsort.SecondSortJob):
    """
    Joins two datasets on the two passed keys.
    """

    def __init__(self, file1, file2, key1, key2, keymod1='1', keymod2='2', outer=None):
        """
        @param file1: the filename of the first file to join on
        @param file2: the filename of the second file to join on
        @param key1: the JSON key in the first file to join with
        @param key2: the JSON key in the second file to join with
        @param keymod1: append this value to each key in file1, for uniqueness
        @param keymod2: append this value to each key in file2, for uniqueness
        @param outer: [None | 'left' | 'right' | 'both'] 
                      If not None, do the appropriate outer join.
                      'both' will emit all unmatched left and all unmatched right keys.
        """
        happy.secondsort.SecondSortJob.__init__(self)
        self.file1 = file1
        self.file2 = file2
        self.key1 = key1
        self.key2 = key2
        self.keymod1 = keymod1
        self.keymod2 = keymod2
        self.outer = outer

    def _modkeys(self, dict, mod):
        """
        Creates a new dict which has the value in "mod" appended
        to all of the keys.
        """
        newdict = {}
        for (k, v) in dict.items():
            newk = k + mod
            newdict[newk] = v
        return newdict

    def smap(self, records, task):
        """
        First, we pivot both files on their join keys, and set the
        joinorder for the secondsort such that all joinkey1 records
        come first.
        """
        inpath = "/" + task.getInputPath() + "/"
        logger.warn("INPATH: %s" % inpath)
        for key, json in records:
            record = happy.json.decode(json)
            if ((inpath.find(self.file1)>=0) and record.has_key(self.key1)):
                newrec = self._modkeys(record, self.keymod1)
                record['__infile__'] = task.getInputPath()
                newrec['__joinorder__'] = 1
                k1 = record[self.key1]
                if happy.flow.isIterable(k1): k1=':|:'.join(k1)
                task.collect(k1, 1, happy.json.encode(newrec))
            if ((inpath.find(self.file2)>=0) and record.has_key(self.key2)):
                newrec = self._modkeys(record, self.keymod2)
                newrec['__joinorder__'] = 2
                k2 = record[self.key2]
                if happy.flow.isIterable(k2): k2=':|:'.join(k2)
                task.collect(k2, 2, happy.json.encode(newrec))

    def sreduce(self, key, values, task):
        """
        Now calculate the cartesian product by iterating over all records
        grouped together on the join key.
        """
        list1 = []
        found_file1 = False
        found_file2 = False
        outer_file1 = (self.outer=='left' or self.outer=='both')
        outer_file2 = (self.outer=='right' or self.outer=='both')
        for json in values:
            record = happy.json.decode(json)
            order = record['__joinorder__']
            newrec = {}
            for key in record.keys():
                newrec[key] = record[key]
            if (order==1):
                found_file1 = True
                list1.append(newrec)
            else:
                try:
                    found_file2 = True
                    for i in xrange(len(list1)):
                        r = list1[i]
                        emitrec = {}
                        emitrec.update(newrec)
                        emitrec.update(r)
                        emitrec['__jointype__'] = 'inner'
                        task.collect(key, happy.json.encode(emitrec))
                    if outer_file2 and not found_file1:
                        newrec['__jointype__'] = 'right'
                        task.collect(key, happy.json.encode(newrec))
                except:
                    logger.error("JOIN FAILED ON RECORD: (%s, %s)" % (key, json))
        if outer_file1 and not found_file2:
            for i in xrange(len(list1)):
                r = list1[i]
                r['__jointype__'] = 'left'
                task.collect(key, happy.json.encode(r))


class FilterExact(happy.HappyJob):
    """
    Filter records from JSON-encoded files
    who have key/values that match our criteria  
    """
    def __init__(self, filters):
        """
        @param filters: a dict, where each key specifies the JSON field to compare
                        against, and each value is the exact string match to be found
                        in that field
        """
        happy.HappyJob.__init__(self)
        self.filters = filters
        self.reducetasks = 0

    def map(self, records, task):
        for key, json in records:
            record = happy.json.decode(json)
            passed = reduce(lambda x,y: x and y, [self.filters[key] == record.get(key, None) for key in self.filters.keys()])
            if passed:
                task.collect(key,json)
                continue


class FilterRe(happy.HappyJob):
    """
    Filter out records from JSON-encoded files
    who have key/values that match our regex  
    """
    def __init__(self, filters):
        """
        @param filters: a dict, where each key specifies the JSON field to compare
                        against, and each value is the regex to be searched 
                        in that field.
        """
        happy.HappyJob.__init__(self)
        self.filters = filters
        self.reducetasks = 0

    def mapconfig(self):
        self.re = {}
        for (key, val) in self.filters.items():
            self.re[key] = re.compile(self.filters[key])        

    def map(self, records, task):
        for key, json in records:
            record = happy.json.decode(json)
            passed = reduce(lambda x,y: x and y, [self.re[key].search(record.get(key, None)) for key in self.re.keys()])
            if passed:
                task.collect(key,json)
                continue

class LabelLambda(happy.HappyJob):
    """
    Label records in JSON-encoded files
    who have key/values that match our criteria  
    """
    def __init__(self, filters):
        """
        @param filters: a list of {key, val, string-encoded lambda, **kwargs} records.
                    
        """
        happy.HappyJob.__init__(self)
        self.filters=filters
        self.reducetasks=0

    def mapconfig(self):
        for filter in self.filters:
            try:
                func = filter.get('func', 'lambda x: False')
                filter['f'] = eval(func)
            except SyntaxError:
                raise SyntaxError, "Could not eval(%s)." % func        

    def map(self, records, task):
        for key, json in records:
            record = happy.json.decode(json)
            for filter in self.filters:
                res = filter['f'](record)
                out = None
                if filter.has_key('val'):
                    if res:
                        out = filter['val']
                else:
                    out = res
                if out:
                    record[filter['key']] = out
                    if filter.get('replace', False):
                        record[filter['key']] = out
                    else:
                        if not record.has_key(filter['key']):
                           record[filter['key']] = out
                        elif happy.flow.isIterable(record[filter['key']]):
                            record[filter['key']].append(out)
                        else:
                            record[filter['key']] = [record[filter['key']], out]
            task.collect(key,happy.json.encode(record))


class FilterLambda(happy.HappyJob):
    """
    Filter out records from JSON-encoded files
    who have key/values that match our criteria  
    """
    def __init__(self, filters):
        """
        @param filters: a list of string-encoded lambda expressions,
                        each of hich will be eval'd against each record.
        """
        happy.HappyJob.__init__(self)
        self.filters=filters
        self.reducetasks=0

    def mapconfig(self):
        self.funcs = [ ]
        for x in self.filters:
            try:
                self.funcs.append(eval(x))
            except SyntaxError:
                raise SyntaxError, "Could not eval(%s)." % x        

    def map(self, records, task):
        for key, json in records:
            record = happy.json.decode(json)
            passed = reduce(lambda x,y: x and y, [f(record) for f in self.funcs])
            if passed:
                task.collect(key,json)
                continue


class Sample(happy.HappyJob):
    """
    Randomly samples the passed job, returning 
    1 in every samplerate records.
    """
    def __init__(self, samplerate):
        """
        @param samplerate: The samplerate.  To get a 1% sampling, enter 100.
        """
        happy.HappyJob.__init__(self)
        self.samplerate = int(samplerate)
        self.reducetasks = 0
        random.seed()
      
    def map(self, records, task):
        for key, value in records:
            r = random.randint(1, self.samplerate)
            if (r == 1):
                task.collect(key, value)


class Tablify(happy.HappyJob):
    """
    Turn JSON-formatted records into a table.
    """
    def __init__(self, cols, sep='\t', na=''):
        happy.HappyJob.__init__(self)
        self.cols = cols
        self.sep = sep
        self.na = na
        self.reducetasks = 0

    def map(self, records, task):
        for key, value in records:
            record = happy.json.decode(value)
            fields = [key]
            fields.extend([record.get(col, self.na) for col in self.cols])
            str_fields = []
            for x in fields:
                if happy.flow.isIterable(x):
                    x = ':'.join(x)
                try:
                    str_fields.append("%s" % x)
                except:
                    str_fields.append('unicode err')
            str = self.sep.join(str_fields)
            task.collect(str, '')


class Aggregate(happy.HappyJob):
    """
    Aggregate records on the values in the passed key,
    and count them
    """
    def __init__(self, aggkey, aggfunc, is_dict=False):
        """
        @param aggkey: the JSON key(s) to aggregate on.  May be a string or list.
        @param aggfunc: the string-encoded function to aggregate with.
                        often a function in the agg class.
        @param is_dict: if True, expects that the results of aggfunc() is a dict,
                        and places items directly in record.
                        if False, creates an item with key=aggfunc, val=aggfunc()
        """
        happy.HappyJob.__init__(self)
        self.aggkey=aggkey
        self.aggfunc=aggfunc
        self.is_dict=is_dict

    def map(self, records, task):
        """ 
        Pivot on the aggkey(s). 
        """
        for key, json in records:
            record = happy.json.decode(json)
            if happy.flow.isIterable(self.aggkey):
                outkey = ''
                for ak in self.aggkey:
                    if record.has_key(ak):
                        outkey = outkey + record[ak] + ":"
                task.collect(outkey, json)                
            elif record.has_key(self.aggkey):
                if (record[self.aggkey]):
                    task.collect(record[self.aggkey], json)

    def reduceconfig(self):
        self.func = eval(self.aggfunc)
        
    def reduce(self, key, values, task):
        """
        Apply the aggfunc over all record in the pivoted key.
        """
        agg = None
        for val in values:
            record = happy.json.decode(val)
            agg = self.func(agg, record)
        if self.is_dict:
            dict = {self.aggkey: key}
            dict.update(agg)
        else:
            dict = {self.aggkey: key, self.aggfunc: agg}
        task.collect(key, happy.json.encode(dict))
        
        
class agg:
    def count(cls):
        """
        Aggregator function to count the number of items in a list.
        We use this weird ternary-operator form to match the syntax
        of the other aggregators.
        """
        return lambda x,y: ((type(x)==int) and [x+1] or ((y==None) and [1] or [2]))[0]
    count=classmethod(count)

    def sum(cls, field):
        """
        Aggregator function to sum the items in a list
        """
        return lambda x,y: ((type(x)==int) and [x+1] or [2])[0]
    sum=classmethod(sum)
    
    def cat(cls, sep):
        """
        Aggregator function to group the items into a string
        """
        return lambda x,y: (len(x)<10000) and "%s%s%s" % (x, sep, y) or x
    cat=classmethod(cat)
     
    def json(cls):
        """
        Aggregator function to group the items into a JSON feature vector.
        
        Given [{'A':'a1', 'B':'b1'}, {'B':'b2', 'C':'c2'}], produces
              {'A':['a1'], 'B':['b1', 'b2'], 'C':['c2']}
        
        Note that all values are lists, even if only one item is available.    
        """
        def _json_impl(agg, record):
            if not agg: agg = {}
            for (k, v) in record.items():
                logger.info("k: " + str(k) + ", v: " + str(v))
                if agg.has_key(k):
                    if happy.flow.isIterable(v):
                        agg[k].extend(v)
                    else:
                        agg[k].append(v)
                else:
                    if happy.flow.isIterable(v):
                        agg[k] = v
                    else:
                        agg[k] = [v]
            return agg
        return _json_impl
    json=classmethod(json)
    
    def triple_features(cls):
        """
        Aggregator function to group triples into a JSON feature vector.
        
        Given: [{'s':'/id/1', 'p':'/foo', 'o':'F1'},
                {'s':'/id/1', 'p':'/foo', 'o':'F2'},
                {'s':'/id/1', 'p':'/bar', 'o':'B1'}]
        Produces: {'/foo':['F1', 'F2'], '/bar':['B1']}
         
        Note that all values are lists, even if only one item is available.    
        """
        def _feature_impl(agg, record):
            if not agg: agg = {}
            p = record['p']
            o = record['o']
            if not agg.has_key(p): agg[p] = [ ]
            agg[p].append(o)
            return agg
        return _feature_impl
    triple_features=classmethod(triple_features)
    
    def list(cls, field):
        """
        Aggregator function to group the items into a list
        """
        return lambda x,y: (type(x)==list) and x + [y.get(field, None)] or ((y==None) and [x.get(field, None)] or [x.get(field, None), y.get(field,None)]) 
        # return lambda x,y: (type(x)==list) and x + [y] or ((y==None) and [x] or [x, y]) 
        # return lambda x,y: (type(x)==list) and x + [y.get(field, None)] or [x.get(field, None)]
    list=classmethod(list)
