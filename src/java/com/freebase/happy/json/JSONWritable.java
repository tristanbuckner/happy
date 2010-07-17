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

package com.freebase.happy.json;

import org.apache.hadoop.io.WritableComparable;
import org.python.core.PyObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A thin wrapper for JSON keys and values.
 */
public class JSONWritable implements WritableComparable {
    private PyObject value;
    private String serializedValue;

    public void set(PyObject o)
    {
        value = o;
        serializedValue = null;
    }

    public PyObject get()
    {
        if(value == null)
        {
            if(serializedValue == null) throw new NullPointerException("No serialized value found");
            try
            {
                value = (PyObject) JSON.decode(serializedValue);
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }
        return value;
    }

    public String getSerialized()
    {
        if(serializedValue == null)
        {
            if(value == null) throw new NullPointerException("No value found");
            try
            {
                serializedValue = JSON.encode(value);
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }
        return serializedValue;
    }

    public void setSerialized(String s)
    {
        serializedValue = s;
        value = null;
    }

    public String toString()
    {
        return getSerialized();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(getSerialized());
    }

    public void readFields(DataInput in) throws IOException {
        setSerialized(in.readUTF());
    }

    public int compareTo(Object o) {
        if(o == this) return 0;
        JSONWritable w = (JSONWritable) o;
        return getSerialized().compareTo(w.getSerialized());
    }
}
