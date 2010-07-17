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

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.python.core.PyJavaType;

import java.io.IOException;
import java.util.Iterator;

/**
 * Happy Reducer
 */
public class HappyReduce
        extends HappyBase implements Reducer<Object, Object, Object, Object> {

    public void reduce(Object key, Iterator<Object> iterator,
                       OutputCollector<Object, Object> outputCollector, Reporter reporter)
            throws IOException {
        ReduceIterator reduceIterator = null;
        try
        {
            if(pytask == null) pytask = PyJavaType.wrapJavaObject(
                    new TaskWrapper(outputCollector, reporter,
                            jobConf.getOutputKeyClass(), jobConf.getOutputValueClass()));
            reduceIterator = new ReduceIterator(iterator);
            workFunction.__call__(getPyValue(key), reduceIterator, pytask);
        }
        catch(Throwable e)
        {
            RuntimeException re;
            if(reduceIterator != null)
            {
                String keyString = getStringValue(key);
                if(keyString.length() > 200) keyString = keyString.substring(0, 200) + "...";
                String value = getStringValue(reduceIterator.getCurrentValue());
                if(value.length() > 200) value = value.substring(0, 200) + "...";
                String message = "Error caught on reduce record key: '" + keyString +
                        "', value: '" + value + "'";
                log.error(message, e);
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