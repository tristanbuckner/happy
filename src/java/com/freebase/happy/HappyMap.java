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


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.python.core.PyJavaType;

import java.io.IOException;

/**
 * Happy Mapper.
 */
public class HappyMap extends HappyBase extends Mapper <Object, Object, Object, Object> {
    public void run(RecordReader<Object, Object> recordReader,
                    OutputCollector<Object, Object> outputCollector, Reporter reporter) throws IOException {
        RecordIterator recordIterator = null;
        try
        {
            if(pytask == null) pytask = PyJavaType.wrapJavaObject(
                    new TaskWrapper(outputCollector, reporter,
                            jobConf.getMapOutputKeyClass(), jobConf.getMapOutputValueClass()));
            recordIterator = new RecordIterator(recordReader);
            workFunction.__call__(recordIterator, pytask);
            super.close();
        }
        catch(Throwable e)
        {
            RuntimeException re;
            if(recordIterator != null)
            {
                String key = getStringValue(recordIterator.getKey());
                if(key.length() > 200) key = key.substring(0, 200) + "...";
                String value = getStringValue(recordIterator.getValue());
                if(value.length() > 200) value = value.substring(0, 200) + "...";
                String message = "Error caught on map record key: '" + key + "', value: '" + value + "'";
                re = new RuntimeException(message, e);
            }
            else
            {
                re = new RuntimeException(e);
            }
            log.error("Exception rethrown", re);
            throw re;
        }
    }
}