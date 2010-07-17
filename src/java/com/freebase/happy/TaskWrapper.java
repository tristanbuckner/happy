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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.python.core.PyObject;

import java.io.IOException;

/**
 * A wrapper for the output collector.
 */
public class TaskWrapper
{
    private Serializer keySerializer;
    private Serializer valueSerializer;
    private OutputCollector collector;
    private Reporter reporter;
    private String inputPath;

    public TaskWrapper(OutputCollector collector, Reporter reporter, Class keyclass, Class valueclass) {
        this.collector = collector;
        this.reporter = reporter;
        keySerializer = getSerializer(keyclass);
        valueSerializer = getSerializer(valueclass);
    }

    public void collect(Object key, Object value) throws IOException
    {
        if(key == null) throw new NullPointerException("Null key specified in collect");
        if(value == null) throw new NullPointerException("Null value specified in collect");
        collector.collect(keySerializer.serialize(key), valueSerializer.serialize(value));
    }

    public void progress()
    {
        reporter.progress();
    }

    public void setStatus(String status)
    {
        reporter.setStatus(status);
    }

    public String getInputPath()
    {
        if(inputPath != null) return inputPath;
        InputSplit split = reporter.getInputSplit();
        if(!(split instanceof FileSplit)) throw new RuntimeException("Split " + split + " doesn't have a path");
        inputPath = ((FileSplit)split).getPath().toString();
        return inputPath;
    }

    private Serializer getSerializer(Class clazz)
    {
        if(clazz.equals(Text.class)) return new TextSerializer();
        else if(clazz.equals(JSONWritable.class)) return new JSONSerializer();
        else if(clazz.equals(BJSON.class)) return new BJSONSerializer();
        else return new DefaultSerializer();
    }

    /////////////////////////////////////////////////////

    private interface Serializer
    {
        Object serialize(Object o);
    }

    private static class BJSONSerializer implements Serializer
    {
        private BJSON bjson = new BJSON();

        public Object serialize(Object o) {
            bjson.setObject(o);
            return bjson;
        }
    }

    private static class TextSerializer implements Serializer
    {
        private Text text = new Text();

        public Object serialize(Object o) {
            text.set(o.toString());
            return text;
        }
    }

    private static class JSONSerializer implements Serializer
    {
        private JSONWritable json = new JSONWritable();

        public Object serialize(Object o) {
            json.set((PyObject) o);
            return json;
        }
    }

    private static class DefaultSerializer implements Serializer
    {
        public Object serialize(Object o) {
            return o;
        }
    }
}

