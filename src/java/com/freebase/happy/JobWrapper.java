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

import org.apache.hadoop.mapred.JobConf;
import org.python.core.PyObject;

import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper for the current JobConf in a job.  Holds task-wide configuration and context.
 */
public class JobWrapper {
    private JobConf jobConf;
    private List<PyObject> closeables = new ArrayList<PyObject>();

    public JobWrapper(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public JobConf getJobConf() {
        return jobConf;
    }

    /**
     * Returns the task partition id of the current task.
     *
     * @return
     */
    public int getTaskPartition()
    {
        return jobConf.getInt("mapred.task.partition", -1);
    }

    /**
     * Adds a PyObject that needs to have close() called when the task is complete.
     *
     * @param closeable
     */
    public void addCloseable(PyObject closeable)
    {
        closeables.add(closeable);
    }

    /**
     * Calls close() on all closeables.
     */
    public void close()
    {
        for(PyObject closeable: closeables)
        {
            PyObject close = closeable.__getattr__("close");
            if(close == null) throw new RuntimeException("Closeable object " + closeable + " has no close method");
            close.__call__();
        }
    }
}
