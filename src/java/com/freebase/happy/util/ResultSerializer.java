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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serialized and deserialized Happy results for a job.
 */
public class ResultSerializer {

    /**
     * Saves happy.results to dfs if any results are set.
     *
     * @param jobConf
     * @param interpreter
     */
    public static void serialize(JobConf jobConf, PythonInterpreter interpreter) throws IOException {
        // store results:
        PyDictionary results = (PyDictionary) interpreter.get("happy").__getattr__("results");
        if(results != null && !results.equals(Py.None) && results.__len__() > 0)
        {
            FileSystem fs = FileSystem.get(jobConf);
            Path workPath = FileOutputFormat.getWorkOutputPath(jobConf);
            Path resultPath = new Path(workPath,
                    "_hresult/result-" + jobConf.get("mapred.task.partition") + "-" + System.currentTimeMillis() + ".bin");
            OutputStream resultOut = fs.create(resultPath);
            PyObjectSerializer.serialize(results, resultOut);
            resultOut.close();
        }
    }

    /**
     * Deserializes results from happy.results and combines all of them into a single dictionary.
     *
     * @param jobConf
     * @return
     */
    public static PyDictionary deserialize(JobConf jobConf) throws IOException, ClassNotFoundException {
        PyDictionary results = new PyDictionary();
        FileSystem fs = FileSystem.get(jobConf);
        Path outputPath = FileOutputFormat.getOutputPath(jobConf);
        // look for a result dir:
        Path resultPath = new Path(outputPath, "_hresult");
        if(fs.exists(resultPath))
        {
            FileStatus[] files = fs.listStatus(resultPath);
            for(FileStatus file: files)
            {
                // load result files into a dictionary:
                if(!file.isDir() && file.getPath().getName().endsWith(".bin"))
                {
                    InputStream in = fs.open(file.getPath());
                    PyDictionary result = (PyDictionary) PyObjectSerializer.deserialize(in);
                    in.close();
                    PyList keys = result.keys();
                    for(int i = 0; i < keys.size(); i++)
                    {
                        PyObject key = keys.pyget(i);
                        PyObject value = result.get(key);
                        PyList list = (PyList) results.__finditem__(key);
                        if(list == null)
                        {
                            list = new PyList();
                            results.__setitem__(key, list);
                        }
                        list.pyadd(value);
                    }
                }
            }
            fs.delete(resultPath, true);
        }
        return results;
    }
}
