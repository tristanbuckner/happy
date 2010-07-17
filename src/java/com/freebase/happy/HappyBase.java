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

package com.freebase.happy;

import com.freebase.happy.bjson.BJSON;
import com.freebase.happy.json.JSONWritable;
import com.freebase.happy.util.JarUtil;
import com.freebase.happy.util.PyObjectSerializer;
import com.freebase.happy.util.ResultSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.python.core.*;
import org.python.util.PythonInterpreter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

/**
 * Base class for happy jobs.
 */
public class HappyBase {
    public static final String RESOURCE_KEY = "happy.resources";
    public static final String PATH_KEY = "happy.path";
    public static final String SCRIPT_KEY = "happy.script";
    public static final String SCRIPT_OBJECT = "happy.object";

    protected final Log log = LogFactory.getLog(super.getClass());
    protected long jobStart;
    protected PythonInterpreter pythonInterpreter;
    protected PyObject jobObject;
    protected PyObject workFunction;
    protected PyObject pytask;
    protected Job job;
    protected JobWrapper jobWrapper;

    /**
     * Converts a key or value for a map or reduce into a PyObject.
     *
     * @param o
     * @return
     */
    public static PyObject getPyValue(Object o)
    {
        if(o instanceof Text) return new PyUnicode(o.toString());
        else if(o instanceof JSONWritable) return ((JSONWritable)o).get();
        else if(o instanceof BJSON) return (PyObject) ((BJSON)o).getObject();
        else return PyJavaType.wrapJavaObject(o);
    }

    public void configure(Job job) {
        this.job = job;
        jobStart = System.currentTimeMillis();
        log.info("Starting config for job " + job.getJobName());

        // set up the local resources:
        if(!"local".equals(job.getConfiguration().get("mapred.job.tracker")))
        {
            try
            {
                symlinkResources(job);
                log.info("Symlinked local resources");
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }

        // make sure that this jar file is in the Jython classpath and we're using the right classloader:
        PySystemState.initialize(PySystemState.getBaseProperties(), null, null);
        Py.getSystemState().setClassLoader(HappyBase.class.getClassLoader());
        String thisJar = JarUtil.findContainingPath(HappyJobRunner.class);

        // add our jar file to the pythonpath:
        PyList pathList = Py.getSystemState().path;
        pathList.insert(0, new PyString(thisJar));
        for(String path: job.getConfiguration().get(PATH_KEY, "").split(":"))
        {
            if(path.length() > 0) pathList.insert(0, new PyString(path));
        }
        log.info("pythonpath: " + pathList.toString());

        // start the interpreter:
        pythonInterpreter = new PythonInterpreter();

        // default imports:
        imp.load("site");

        log.info("Jython intepreter created");

        // set up the happy environment:
        jobWrapper = new JobWrapper(job);
        pythonInterpreter.exec("import happy");
        pythonInterpreter.get("happy").__setattr__(new PyString("job"), PyJavaType.wrapJavaObject(jobWrapper));

        // import script:
        String importFile = job.getConfiguration().get(SCRIPT_KEY);
        if(importFile != null)
        {
            String importPackage = importFile.substring(0, importFile.lastIndexOf("."));
            pythonInterpreter.exec("from " + importPackage + " import *");
        }

        // get our script object:
        File scriptObjectFile = new File(job.getConfiguration().get(SCRIPT_OBJECT));

        log.info("Deserializing " + scriptObjectFile.getAbsolutePath() + ", size: " + scriptObjectFile.length());
        try {
            jobObject = PyObjectSerializer.deserialize(scriptObjectFile);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing " + scriptObjectFile, e);
        }

        // configure:
        log.info("Configuring job");
        String configFunctionName;
        String workFunctionName;
        if(this instanceof HappyMap)
        {
            configFunctionName = "mapconfig";
            workFunctionName = "map";
        }
        else if(this instanceof HappyCombine)
        {
            configFunctionName = "combineconfig";
            workFunctionName = "combine";
        }
        else
        {
            configFunctionName = "reduceconfig";
            workFunctionName = "reduce";
        }

        PyObject configFunction = jobObject.__findattr__(configFunctionName.intern());
        if(configFunction != null) configFunction.__call__();
        workFunction = jobObject.__findattr__(workFunctionName.intern());

        log.info("Configuration took " + (System.currentTimeMillis() - jobStart) + "ms");
    }

    public void close() throws IOException {
        // call the job close method:
        if(jobObject != null)
        {
            // close:
            String closeFunctionName;
            if(this instanceof HappyMap) closeFunctionName = "mapclose";
            else if(this instanceof HappyCombine) closeFunctionName = "combineclose";
            else closeFunctionName = "reduceclose";
            PyObject closeFunction = jobObject.__findattr__(closeFunctionName.intern());
            if(closeFunction != null) closeFunction.__call__();
        }
        // close closeables:
        jobWrapper.close();
        // store any results:
        ResultSerializer.serialize(job, pythonInterpreter);
        // unset the job variable:
        pythonInterpreter.get("happy").__setattr__(new PyString("job"), Py.None);

        // log job duration:
        long now = System.currentTimeMillis();
        log.info("Job finished, duration: " + (now - jobStart) + "ms");
    }

    protected String getStringValue(Object o)
    {
        try
        {
            return String.valueOf(o);
        }
        catch(Exception e)
        {
            return "";
        }
    }

    /**
     * Looks for resource files in the classpath and symlinks then to the working directory.
     * @param jobConf
     * @throws java.io.IOException
     */
    private void symlinkResources(Job job) throws IOException {
        for(String resource: job.getConfiguration().get(RESOURCE_KEY, "").split(":"))
        {
            if(resource.length() > 0)
            {
                URL url = HappyBase.class.getClassLoader().getResource(resource);
                if(url != null)
                {
                    String uri = url.getPath();
                    org.apache.hadoop.fs.FileUtil.symLink(uri, "./" + resource);
                }
                else
                {
                    log.debug("Resource " + resource + " not found");
                }
            }
        }
        for(String path: job.getConfiguration().get(PATH_KEY, "").split(":"))
        {
            if(path.length() > 0)
            {
                URL url = HappyBase.class.getClassLoader().getResource(path);
                if(url != null)
                {
                    String uri = url.getPath();
                    org.apache.hadoop.fs.FileUtil.symLink(uri, "./" + path);
                }
                else
                {
                    log.debug("Path " + path + " not found");
                }
            }
        }
    }

    /**
     * A Python iterator for map records.
     */
    public static class RecordIterator extends PyIterator
    {
        private final RecordReader recordReader;
        private final WritableComparable key;
        private final Writable value;

        public RecordIterator(RecordReader recordReader) {
            this.recordReader = recordReader;
            key = (WritableComparable) recordReader.createKey();
            value = (Writable) recordReader.createValue();
        }

        public PyObject __iternext__() {
            try {
                if(recordReader.next(key, value))
                {
                    return new PyTuple(new PyObject[]{getPyValue(key), getPyValue(value)});
                }
                else return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public WritableComparable getKey() {
            return key;
        }

        public Writable getValue() {
            return value;
        }
    }

    /**
     * A python iterator for combiners and reducers.
     */
    public static class ReduceIterator extends PyIterator
    {
        private final Iterator iterator;
        private PyObject currentValue;

        public ReduceIterator(Iterator iterator) {
            this.iterator = iterator;
        }

        public PyObject __iternext__() {
            if(iterator.hasNext())
            {
                currentValue = getPyValue(iterator.next());
                return currentValue;
            }
            else return null;
        }

        public PyObject getCurrentValue() {
            return currentValue;
        }
    }
}

