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


import unittest
import logging as logger
logger.basicConfig(level=logger.INFO)
from sets import Set as set
from flow import DAG, CycleException

class TestDag(unittest.TestCase):    

    def testNull(self):
        """ Test that unittest harness is working """
        logger.info("In TestFlow.testNull ...")
        self.assertEqual(1, 1)
        logger.info("DONE.")

    def testSort(self):
        """
        Make sure sorting is working in Python 2.2 like I expect
        """
        a = [2,4,3,1]
        a.sort()
        self.assertEqual(a,[1,2,3,4])
        
    def testBidirectional(self):
        """ Test that parent / child links are bidirectional """
        logger.info("In TestFlow.testBidirectional ...")
        p1 = DAG()
        c1 = DAG()
        p1.addChild(c1)
        self.assertEquals(c1.parents(), [p1])
        self.assertEquals(p1.children(), [c1])
        p2 = DAG()
        c2 = DAG()
        c2.addParent(p2)
        self.assertEquals(c2.parents(), [p2])
        self.assertEquals(p2.children(), [c2])
        logger.info("DONE.")

    def testCreateWithParent(self):
        """ Test instanciation with parent specified """
        logger.info("In TestFlow.testCreateWithParent ...")
        p = DAG()
        c = DAG(parents=p)
        self.assertEqual(c.parents(), [p])
        self.assertEqual(p.children(), [c])
        logger.info("DONE.")

    def testMultiParent(self):
        """ Set up with many parents """
        logger.info("In TestFlow.testMultiParent ...")
        p1 = DAG()
        p2 = DAG()
        c1 = DAG()
        p1.addChild(c1)
        c1.addParent(p2)
        self.assertEqual(c1.parents(), [p1, p2])
        self.assertEqual(p1.children(), [c1])
        self.assertEqual(p2.children(), [c1])
        logger.info("DONE.")

    def testMultiChild(self):
        """ Set up with many children """
        logger.info("In TestFlow.testMultiChild ...")
        p1 = DAG(name="P1")
        c1 = DAG(name="C1")
        c2 = DAG(name="C2")
        p1.addChild(c1)
        p1.addChild(c2)
        self.assertEqual(c1.parents(), [p1])
        self.assertEqual(c2.parents(), [p1])
        self.assertEqual(p1.children(), [c1, c2])
        logger.info("DONE.")

    def testDictize(self):
        logger.info("In TestFlow.testDictize ...")
        name="name", 
        p1 = DAG(name="P1")
        c1 = DAG(name="C1")
        c2 = DAG(name="C2")
        p1.addChild(c1)
        p1.addChild(c2)
        dict = p1.dictize()
        d1 = DAG.dedictize(dict)
        self.assertEqual(p1.name, d1.name)
        self.assertEqual(p1.children()[0].name, d1.children()[0].name)
        self.assertEqual(p1.children()[1].name, d1.children()[1].name)
        logger.info("DONE.")

    def testDAG(self):
        """ Set up with many relationships """
        logger.info("In TestFlow.testDAG ...")
        logger.debug("Setting up DAG ...")
        a = DAG()
        b = DAG()
        c = DAG()
        d = DAG()
        e = DAG()
        f = DAG()
        g = DAG()
        h = DAG()
        i = DAG()
        a.addChild(b)
        a.addChild(c)
        b.addChild(c)
        d.addChild(f)
        e.addChild(f)
        c.addChild(g)
        f.addChild(g)
        h.addChild(g)
        g.addChild(i)        
        logger.debug("Testing parent/child relationships ...")
        self.assertEqual(a.parents(), [])
        self.assertEqual(a.children(), [b, c])
        self.assertEqual(c.parents(), [a, b])
        self.assertEqual(f.parents(), [d, e])
        self.assertEqual(g.parents(), [c, f, h])
        self.assertEqual(g.children(), [i])
        logger.debug("Testing node retrieval ...")
        nodes0 = set([a,b,c,d,e,f,g,h,i])
        nodes1 = a.nodes()
        nodes2 = e.nodes()
        self.assertEqual(nodes0, nodes1)
        self.assertEqual(nodes0, nodes2)
        logger.debug("Testing sinks and sources ...")
        sinks = a.sinks()
        self.assertEqual(sinks, [i])
        sources = a.sources()
        self.assertEqual(sources, [a, d, e, h])
        logger.debug("Testing isAncestorOf() and isDecendentOf() ...")
        self.assert_(a.isAncestorOf(b))
        self.assert_(a.isAncestorOf(g))
        self.assert_(a.isAncestorOf(i))
        self.assert_(not a.isAncestorOf(d))
        self.assert_(i.isDecendentOf(g))
        self.assert_(i.isDecendentOf(a))
        self.assert_(i.isDecendentOf(b))
        self.assert_(i.isDecendentOf(e))
        self.assert_(not f.isDecendentOf(a))
        logger.info("DONE.")

    def testTopoSort(self):
        """  Topological sort of the DAG """
        logger.info("In TestFlow.testSort ...")
        logger.debug("Setting up DAG ...")
        a = DAG()
        b = DAG()
        c = DAG()
        d = DAG()
        e = DAG()
        f = DAG()
        g = DAG()
        h = DAG()
        i = DAG()
        j = DAG()
        a.addChild(b)
        a.addChild(c)
        b.addChild(d)
        b.addChild(e)
        c.addChild(f)
        c.addChild(g)
        d.addChild(h)
        g.addChild(i)        
        i.addChild(j)   
        e.addChild(j)     
        b.addChild(i)
        a.addChild(j)
        logger.debug("Testing topological sort ...")
        sort = a.sort()
        self.assertEqual(sort, [a, b, c, d, e, f, g, h, i, j])        
        logger.info("DONE.")

    def testCircle(self):
        logger.info("In TestFlow.testCircle ...")
        p1 = DAG(name="P1")
        c1 = DAG(name="C1")
        c2 = DAG(name="C2")
        g1 = DAG(name="G1")
        p1.addChild(c1)
        p1.addChild(c2)
        c1.addChild(g1)
        self.assertRaises(CycleException, c1.addChild, p1)
        self.assertRaises(CycleException, p1.addParent, c1)
        self.assertRaises(CycleException, c2.addChild, p1)
        self.assertRaises(CycleException, p1.addParent, c2)
        self.assertRaises(CycleException, g1.addChild, p1)
        self.assertRaises(CycleException, p1.addParent, g1)


if __name__ == '__main__':
    setLevel("info")
    unittest.main()
