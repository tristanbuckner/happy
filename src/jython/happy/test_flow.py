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

# following are for normal operation in happy
import happy
from happy.log import logger, setLevel
import happy.dfs as dfs
import happy.json as json

# following are only for unitest in pydev: cannot call happy
# import logging as logger
# logger.basicConfig(level=logger.INFO)

# following are required in both
import unittest
from happy.flow import *
from happy.functions import *
from sets import Set as set


class TestFlow(unittest.TestCase):    

    def _testEmptyRun(self):
        logger.info("In TestFlow.testRun() ...")
        dict = {1: {'children': [2, 3, 5], 'kwargs': {'job': 'NullJob()', 'name': 'P1', 'inputpaths':['small']}}, 
                2: {'children': [4],       'kwargs': {'job': 'NullJob()', 'name': 'C1'}}, 
                3: {'children': [4],       'kwargs': {'job': 'NullJob()', 'name': 'C2'}},
                5: {'children': [4],       'kwargs': {'job': 'NullJob()', 'name': 'C3'}},
                4: {'children': [],        'kwargs': {'job': 'NullJob()', 'name': 'G1', 'outputpath':'crap'}}}
        dag = HappyJobNode.dedictize(dict)
        dag.run(force=True)

    def _testHappyRun(self):
        logger.info("In TestFlow.testSingleRun() ...")
        h = IdentityJob()
        h.inputpaths = "small"
        h.outputpath = "crap"
        dfs.delete('crap')
        h.run()
        dfs.delete('crap')

    def _testSingleRun(self):
        logger.info("In TestFlow.testSingleRun() ...")
        node = HappyJobNode(name="P1", job=IdentityJob(),
                          inputpaths=['small'], outputpath='crap')
        node.run(force=True)

    def _testDagRun(self):
        logger.info("In TestFlow.testDagRun() ...")
        p1 = HappyJobNode(name="P1", job=IdentityJob(),inputpaths=['small'])
        c1 = HappyJobNode(name="C1", job=IdentityJob())
        c2 = HappyJobNode(name="C2", job=IdentityJob())
        g1 = HappyJobNode(name="G1", job=IdentityJob(), outputpath='crap')
        p1.addChild(c1)
        p1.addChild(c2)
        c1.addChild(g1)
        c2.addChild(g1)
        p1.run(force=True)

    def _testFlowRun(self):
        logger.info("In TestFlow.testFlowRun() ...")
        f = Flow(IdentityJob(),inputpaths=['small'])
        f1 = f.chain(IdentityJob())
        f2 = f.chain(IdentityJob()).chain(IdentityJob(), join=f1, outputpath='crap')
        f2.run(force=True)

    def _testFilter(self):
        logger.info("In TestFlow.testFilter() ...")
        node = HappyJobNode(name='filter_graph', 
                            job=FilterJson(filterkey='propname', 
                                          filtervalues=['/type/object/type'],
                                          returnkeys=['target', 'creator']),
                            inputpaths=['/data/graph/latest/crawl'], 
                            outputpath='typecount')
        node.run(force=True)

    def _testFilterLambda(self):
        logger.info("In TestFlow.testFilterLambda() ...")
        node = HappyJobNode(name='filter_graph_lambda', 
                            job=FilterLambda(filters=["lambda x: x.get('propname', None) in ['/type/object/name', '/common/topic/alias']",
                                                      "lambda y: type(y.get('value', ' '))==str and y.get('value', ' ').startswith('c')"],
                                          returnkeys=['value', '__keys__']),
                            inputpaths=['/data/graph/latest/crawl'], 
                            outputpath='cnames')
        node.run(force=True)

    def _testCountTypes(self):
        logger.info("In TestFlow.testCountTypes() ...")
        filter = HappyJobNode(name='filter_graph', 
                            job=FilterExact(filterkey='propname', 
                                          filtervalues=['/type/object/type'],
                                          returnkeys=['target']),
                            inputpaths=['/data/graph/latest/crawl'])
        agg = HappyJobNode(name='agg_types', 
                            job=AggregateJson(aggkey='target', aggfunc='agg.count()'),
                            outputpath='typecount')
        filter.addChild(agg)
        filter.run(force=True)

    def _testGraphNames(self):
        logger.info("In TestFlow.testGraphNames() ...")
        names = HappyJobNode(name='filter_graph_names', 
                            job=FilterExact(filterkey='propname', 
                                           filtervalues=['/type/object/name', '/common/topic/alias'],
                                           keyfield='guid', mapfields={'value':'name'}),
                            inputpaths=['/data/graph/latest/crawl'])
        agg = HappyJobNode(name='invert_names', 
                            job=AggregateJson(aggkey='value', aggfunc='agg.list("guid")'),
                            outputpath='namelist')
        names.addChild(agg)
        names.run(force=True)

    def _testJoin(self):
        logger.info("In TestFlow.testGraphNames() ...")
        names = HappyJobNode(name='get_names', 
                            job=FilterExact(filterkey='propname', 
                                           filtervalues=['/type/object/name', '/common/topic/alias'],
                                           keyfield='a:guid', mapfields={'value':'name'}),
                            inputpaths=['/data/graph/latest/crawl'])
        types = HappyJobNode(name='get_types', 
                              job=FilterExact(filterkey='propname', 
                                              filtervalues=['/type/object/type'],
                                              keyfield='b:guid', mapfields={'target':'type'}),
                              inputpaths=['/data/graph/latest/crawl'])
        join = HappyJobNode(name='join_name_types',
                            job=InnerJoin(joinkeys=['a:guid', 'b:guid'], outkey='guid'))
        people = HappyJobNode(name='filter_people',
                              job=FilterExact(filterkey='type', 
                                              filtervalues=['/people/person'],
                                              keyfield='guid', mapfields={'type':'type', 'name':'name'}))
        agg = HappyJobNode(name='invert_names', 
                            job=AggregateJson(aggkey='name', aggfunc='agg.list("guid")'),
                            outputpath='namelist')
        names.addChild(join)
        types.addChild(join)
        join.addChild(people)
        people.addChild(agg)
        names.run(force=True)

    def testFlow2(self):
        logger.info("In TestFlow.testFlow2() ...")
        test_flow = Flow2(inputpaths=['/data/graph/latest/crawl'], outputpath='namelist')
        (names, types) = test_flow.split()
        names.chain(HappyJobNode(name='get_names', 
                                 job=FilterExact(filterkey='propname', 
                                                 filtervalues=['/type/object/name', '/common/topic/alias'],
                                                 keyfield='a:guid', mapfields={'value':'name'})))
        types.chain(HappyJobNode(name='get_types', 
                                 job=FilterExact(filterkey='propname', 
                                                 filtervalues=['/type/object/type'],
                                                 keyfield='b:guid', mapfields={'target':'type'})))
        names.chain(HappyJobNode(name='join_name_types',
                                 job=InnerJoin(joinkeys=['a:guid', 'b:guid'], outkey='guid'),
                                 force=True),
                    join=types)
        names.chain(HappyJobNode(name='filter_people',
                                 job=FilterExact(filterkey='type', 
                                                 filtervalues=['/people/person'],
                                                 keyfield='guid', mapfields={'type':'type', 'name':'name'})))
        names.chain(HappyJobNode(name='invert_names', 
                                 job=AggregateJson(aggkey='name', aggfunc='agg.list("guid")')))
        logger.debug("DAG: \n%s\n" % names.startNode.dictize())
        names.run(force=False)

if __name__ == '__main__':
    setLevel("debug")
    unittest.main()
