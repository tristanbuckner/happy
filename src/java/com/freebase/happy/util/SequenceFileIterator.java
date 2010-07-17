/**
 * ========================================================================
 * Copyright (c) 2008, Metaweb Technologies, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY METAWEB TECHNOLOGIES ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL METAWEB TECHNOLOGIES BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
 * IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * ========================================================================
 *
 */

package com.freebase.happy.util;

import com.freebase.happy.HappyBase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.python.core.PyIterator;
import org.python.core.PyObject;
import org.python.core.PyTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a SequenceFile.Reader in a Python iterator.
 */
public class SequenceFileIterator {

    public static PyIterator getIterator(Path path, FileSystem fileSystem) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        if(fileSystem.isFile(path)) paths.add(path);
        else
        {
            FileStatus[] files = fileSystem.listStatus(path);
            for(FileStatus file: files)
            {
                if(!file.isDir()) paths.add(file.getPath());
            }
        }
        return new PySequenceFileIterator(paths, fileSystem);
    }

    public static class PySequenceFileIterator extends PyIterator
    {
        private FileSystem fileSystem;
        private List<Path> paths;
        private Writable key;
        private Writable value;
        private SequenceFile.Reader currentReader;

        public PySequenceFileIterator(List<Path> paths, FileSystem fileSystem) {
            this.paths = paths;
            this.fileSystem = fileSystem;
        }

        public PyObject __iternext__() {
            try
            {
                // try to read:
                if(currentReader != null)
                {
                    if(currentReader.next(key, value))
                    {
                        return new PyTuple(new PyObject[]{HappyBase.getPyValue(key), HappyBase.getPyValue(value)});
                    }
                }

                // check if we failed, get the next reader:
                while(currentReader != null || paths.size() > 0)
                {
                    nextReader();
                    if(currentReader != null)
                    {
                        if(currentReader.next(key, value))
                        {
                            return new PyTuple(new PyObject[]{HappyBase.getPyValue(key), HappyBase.getPyValue(value)});
                        }
                    }
                }

                // nothing found:
                return null;
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        private void nextReader() throws IOException, IllegalAccessException, InstantiationException {
            if(currentReader != null) currentReader.close();
            if(paths.size() > 0)
            {
                currentReader = new SequenceFile.Reader(fileSystem, paths.remove(0), fileSystem.getConf());
                key = (Writable) currentReader.getKeyClass().newInstance();
                value = (Writable) currentReader.getValueClass().newInstance();
            }
            else currentReader = null;
        }

        public void close() throws IOException {
            if(currentReader != null) currentReader.close();
            currentReader = null;
            paths.clear();
        }
    }
}
