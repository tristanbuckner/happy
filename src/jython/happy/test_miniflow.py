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
from sets import Set as set


class TestFlow(unittest.TestCase):    

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
        
    def testCreateSingle(self):
        """ Test that we can create a HappyJobNode """
        logger.info("In TestFlow.testCreateSingle ...")
        node = HappyJobNode(name="name", inFiles=['in'], outFiles=['out'], 
                            status='status', job='job')
        self.assert_(node != None)
        self.assertEqual(node.name, 'name')
        self.assertEqual(node.inFiles, ['in'])
        self.assertEqual(node.outFiles, ['out'])
        self.assertEqual(node.status, 'status')
        self.assertEqual(node.job, 'job')
        logger.info("DONE.")
        

if __name__ == '__main__':
    setLevel("info")
    unittest.main()
