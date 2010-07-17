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

import unittest
import happy
import happy.dfs as dfs
import happy.json as json

class TestDfs(unittest.TestCase):    
    def testFilesystem(self):
        self.assertNotEqual(dfs.getFileSystem(), None, "No filesystem found")

    def testReadWrite(self):
        # prep:
        filename = "testcase.txt"
        filename2 = "testcase2.txt"        
        dfs.delete(filename)
        dfs.delete(filename2)
        # write:
        file = dfs.write(filename)
        self.assertNotEqual(file, None, "No file found")
        file.write("test1\n")
        file.write("test2\n")
        file.write("test3\n")
        file.close()
        # rename:
        dfs.rename(filename, filename2)
        # read:
        file = dfs.read(filename2)
        self.assertNotEqual(file, None, "No file found")
        lines = file.readlines()
        file.close()
        self.assertEqual(len(lines), 3, "Wrong number of lines was read")
        for i, line in enumerate(lines):
            self.assertEqual(line, "test" + str(i + 1) + "\n", "Wrong line value")
        # grep:
        grepresult = list(dfs.grep(filename2, "t2"))
        self.assertEqual(grepresult, ["test2"], "grep failed")
        # cleanup:
        dfs.delete(filename2)

    def testSequenceFiles(self):
        # prep:
        dfs.delete("testcase")
        # write:
        for i, compressiontype in enumerate(["lzo", "gzip", "zlib"]):
            filename = "testcase/testcase" + str(i) + ".seq"
            collector = dfs.createCollector(filename, type="sequence", compressiontype=compressiontype)
            for _ in range(1000):
                collector.collect("key", "value")
            collector.close()
        # read:
        sequence = dfs.readSequenceFile("testcase")
        counter = 0
        for key, value in sequence:
            counter += 1
            self.assertEqual(key, "key", "Wrong key")
            self.assertEqual(value, "value", "Wrong value")
        sequence.close()
        self.assertEqual(counter, 3000, "Wrong number of values")
        # cleanup:
        dfs.delete("testcase")

class TestJSON(unittest.TestCase):
    def testEncodeDecode(self):
        t = [1, 2, 3.5, 4.6, {"abc":["foo"], "bar":"bar"}, None]
        self.assertEqual(json.decode(json.encode(t)), t, "Decode/encode failed")
        
if __name__ == '__main__':
    unittest.main()

    