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

from org.apache.hadoop.fs import FileSystem, Path
from org.apache.hadoop.io import SequenceFile
from org.apache.hadoop.io.compress import DefaultCodec, GzipCodec, LzoCodec
from com.freebase.happy.util import DatasetPath, SequenceFileIterator, StringIterator, PyMapDir, TextFileCollector, TextSequenceFileCollector
from com.freebase.happy.bjson import BJSONCollector
import java.lang
import happy, time, random, jarray

def getFileSystem(fs="dfs"):
    """
    Returns a Hadoop FileSystem object, either "dfs" (default) or "local".
    """
    if fs == "dfs": return FileSystem.get(happy.getJobConf())
    elif fs == "local": return FileSystem.getLocal(happy.getJobConf())
    else: raise Exception("Unknown filesystem " + fs)

def read(path):
    """
    Returns a Python file-like object for a specified DFS file or directory.
    Merges files in a specified directory.
    """
    # this is a hack because PyFile doesn't support Readers:
    return ReaderFile(DatasetPath(happy.getJobConf(), path).getReader())

def write(path, compressiontype=None):
    """
    Returns a Python file-like object for a specified DFS file.  Uses a specified compression codec.
    """
    return WriterFile(DatasetPath(happy.getJobConf(), path).getWriter(_getCodec(compressiontype)))

def grep(path, regex):
    """
    Returns an iterator over lines in a path that contain a given regular expression.
    Uses the Java regex syntax.
    """
    return StringIterator.getIterator(DatasetPath(happy.getJobConf(), path).grepLines(regex))

def delete(path):
    """
    Deletes a specified DFS path.
    """
    DatasetPath(happy.getJobConf(), path).deletePath()

def copyToLocal(path, localpath):
    """
    Copies a DFS path to a local file.  Merges files in a specified directory.
    """
    DatasetPath(happy.getJobConf(), path).copyToLocal(localpath)

def copyFromLocal(localpath, path):
    """
    Copies a local path to a DFS file.
    """
    DatasetPath(happy.getJobConf(), path).copyFromLocal(localpath)

def rename(src, dst):
    """
    Renames a DFS path.
    """
    DatasetPath(happy.getJobConf(), src).rename(dst)

def merge(path, dst):
    """
    Merges files in a specified directory to a specified file.
    """
    input = DatasetPath(happy.getJobConf(), path)
    output = DatasetPath(happy.getJobConf(), dst)
    input.copyTo(output)

def createCollector(path, fs="dfs", type="text", compressiontype="lzo", sequencetype="BLOCK"):
    """
    Creates a type "text" (default) or "sequence" file collector at the specified path.
    Collectors are automatically closed at the end of the job.
    """
    filesystem = getFileSystem(fs)
    datasetPath = DatasetPath(filesystem, path)
    datasetPath.deletePath()
    if type == "sequence":
        collector = TextSequenceFileCollector(filesystem, happy.getJobConf(), Path(path),
                                              _getSequenceFileType(sequencetype), _getCodecInstance(compressiontype))
    elif type == "text":
        collector = TextFileCollector(filesystem, happy.getJobConf(), Path(path))
    elif type == "bjson":
        collector = BJSONCollector(filesystem, happy.getJobConf(), Path(path),
                                   _getSequenceFileType(sequencetype), _getCodecInstance(compressiontype))
    else: raise Exception("Unknown collector type " + type)
    # add as a closeable so that it is closed correctly:
    if happy.job is not None: happy.job.addCloseable(collector)
    return collector

def createPartitionedCollector(path, fs="dfs", type="text", compressiontype="lzo", sequencetype="BLOCK"):
    """
    Creates a partitioned collector of type "text" (default) or "sequence" at the specified path.
    Collectors are automatically closed at the end of the job.
    """
    if happy.job is not None: partition = happy.job.getTaskPartition()
    else: raise Exception("Cannot create a partitioned collector outside of a task partition")
    filename = path + "/part-%05d"%(partition,)
    collector = createCollector(filename, fs=fs, type=type, compressiontype=compressiontype, sequencetype=sequencetype)
    return collector

def readSequenceFile(path, fs="dfs"):
    """
    Returns an iterator over a SequenceFile's key, value pairs.
    Merges files in a specified directory.
    """
    return SequenceFileIterator.getIterator(Path(path), getFileSystem(fs))

def _getCodec(codec):
    if codec == "zlib": return DefaultCodec
    elif codec == "gzip": return GzipCodec
    elif codec == "lzo": return LzoCodec
    else: return None

def _getCodecInstance(codec):
    codecclazz = _getCodec(codec)
    if codecclazz is not None: return codecclazz()
    else: return None

def _getSequenceFileType(sequencetype):
    if sequencetype == "RECORD": return SequenceFile.CompressionType.RECORD
    elif sequencetype == "BLOCK": return SequenceFile.CompressionType.BLOCK
    else: return SequenceFile.CompressionType.NONE

def mktemp(name=None):
    """
    Generate a directory path safe to use for temporary data.
    An optional name will be used to prefix the path for easier debugging.
    The path will be generated within the current hadoop.tmp.dir and will sort
    chronologically.
    """
    path = happy.getJobConf().get("hadoop.tmp.dir") + "/"
    if name:
        path += str(name) + "-"
    path += "%.0f%i" % (time.time(), random.randint(0, 1E5))
    return path

def mkdir(dir):
    """
    Create an HDFS directory.
    """
    # path = DatasetPath(happy.getJobConf(), dir)
    path = Path(dir)
    return getFileSystem().mkdirs(path)

def fileStatus(path):
    """
    Returns the org.apache.hadoop.fs.FileStatus object for this path
    """
    return DatasetPath(happy.getJobConf(), path).getFileStatus()

def modtime(path):
    """
    Returns the modification time for this path, in ms from the epoch.
    """
    return fileStatus(path).getModificationTime()

def exists(path):
    """
    Returns True if this path exists
    """
    return DatasetPath(happy.getJobConf(), path).exists()

def openMapDir(path):
    """
    Opens a MapDir map over a directory of MapFiles.
    """
    return PyMapDir.openMapDir(getFileSystem(), path, happy.getJobConf())

_separator = java.lang.System.getProperty("line.separator")

class WriterFile:
    def __init__(self, output):
        self._output = output

    def close(self):
        self._output.close()

    def flush(self):
        self._output.flush()

    def write(self, s):
        self._output.write(s)

    def writelines(self, seq):
        for s in seq: self.write(s)

class ReaderFile:
    def __init__(self, input):
        self._input = input

    def __iter__(self):
        return self

    def close(self):
        self._input.close()

    def next(self):
        line = self._input.readLine()
        if line is None: raise StopIteration()
        return unicode(line + _separator)

    def read(self, size=None):
        if size is not None:
            buffer = jarray.zeros(size, 'c')
            r = self._input.read(buffer, 0, size)
            if r == -1: return None
            else: return unicode(java.lang.String(buffer, 0, r))
        else:
            buffer = jarray.zeros(64, 'c')
            ret = java.lang.StringBuilder()
            while True:
                r = self._input.read(buffer, 0, size)
                if r == -1: return unicode(ret.toString())
                else: ret.append(buffer, 0, r)

    def readline(self, size=None):
        line = self._input.readLine()
        if line is None: return ""
        else: return line + _separator

    def readlines(self, sizehint=None):
        return list(self.next())
