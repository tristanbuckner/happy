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

package com.freebase.happy.bjson;

import com.freebase.happy.util.TextSerializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.python.core.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 *
 */
public class BJSON implements Writable, Serializable {
    private static final byte TOKEN_NULL = 0;
    private static final byte TOKEN_STRING = 1;
    private static final byte TOKEN_LIST = 2;
    private static final byte TOKEN_DICT = 3;
    private static final byte TOKEN_INT = 4;
    private static final byte TOKEN_FLOAT = 5;
    private static final byte TOKEN_BOOL_TRUE = 6;
    private static final byte TOKEN_BOOL_FALSE = 7;

    private static StringInternMap internMap = new StringInternMap();

    private TextSerializer text = new TextSerializer();
    private Object object;

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public void write(DataOutput out) throws IOException {
        write(object, out);
    }

    public void readFields(DataInput in) throws IOException {
        this.object = read(in);
    }

    //////////////////////////////////////////////////////////////

    private PyObject read(DataInput in) throws IOException {
        byte tokenType = in.readByte();
        switch(tokenType)
        {
            case TOKEN_LIST: return decodeArray(in);
            case TOKEN_DICT: return decodeObject(in);
            case TOKEN_STRING: return decodeString(in);
            case TOKEN_FLOAT: return new PyFloat(in.readDouble());
            case TOKEN_INT: return new PyInteger(WritableUtils.readVInt(in));
            case TOKEN_BOOL_TRUE: return Py.True;
            case TOKEN_BOOL_FALSE: return Py.False;
            case TOKEN_NULL: return Py.None;
            default: throw new IOException("invalid token value" + tokenType);
        }
    }

    private PyObject decodeString(DataInput in) throws IOException {
        text.readFields(in);
        String s = text.getString();
        if(s.length() == 0) return Py.EmptyString;
        else return internMap.intern(s);
    }

    private PyObject decodeObject(DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        PyDictionary pyDictionary = new PyDictionary();
        for(int i = 0; i < size; i++)
        {
            pyDictionary.__setitem__(read(in), read(in));
        }
        return pyDictionary;
    }

    private PyObject decodeArray(DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        PyList objects = new PyList();
        for(int i = 0; i < size; i++)
        {
            objects.pyadd(read(in));
        }
        return objects;
    }

    //////////////////////////////////////////////////////////////

    private void write(Object o, DataOutput out) throws IOException {
        if(o == null) out.writeByte(TOKEN_NULL);
        else if(o instanceof PyDictionary) encodePyDictionary((PyDictionary)o, out);
        else if(o instanceof PyBaseString || o instanceof String)         {
            out.writeByte(TOKEN_STRING);
            text.setString(o.toString());
            text.write(out);
        }
        else if(o instanceof Boolean) {
            if(o.equals(Boolean.TRUE)) out.writeByte(TOKEN_BOOL_TRUE);
            else out.writeByte(TOKEN_BOOL_FALSE);
         }
         else if(o instanceof PyBoolean)
         {
             if(((PyBoolean)o).getValue() != 0) out.writeByte(TOKEN_BOOL_TRUE);
             else out.writeByte(TOKEN_BOOL_FALSE);
         }
        else if(o instanceof Float)
        {
            out.writeByte(TOKEN_FLOAT);
            out.writeDouble((Float) o);
        }
        else if(o instanceof PyFloat) {
            out.writeByte(TOKEN_FLOAT);
            out.writeDouble(((PyFloat)o).getValue());
        }
        else if(o instanceof PyInteger)
        {
            out.writeByte(TOKEN_INT);
            WritableUtils.writeVInt(out, ((PyInteger)o).getValue());
        }
        else if(o instanceof Integer)
        {
            out.writeByte(TOKEN_INT);
            WritableUtils.writeVInt(out, (Integer)o);
        }
        else if(o instanceof List) encodeList((List)o, out);
        else if(o instanceof Map) encodeMap((Map)o, out);
        else if(o.getClass().isArray()) encodeList(Arrays.asList(o), out);
        else if(Py.None.equals(o)) out.writeByte(TOKEN_NULL);
        else
        {
            throw new IOException("Unknown object " + o.toString());
        }
    }

    private void encodeList(List l, DataOutput out) throws IOException {
        out.writeByte(TOKEN_LIST);
        WritableUtils.writeVInt(out, l.size());
        for(Object o: l) write(o, out);
    }

    private void encodeMap(Map m, DataOutput out) throws IOException {
        out.writeByte(TOKEN_DICT);
        WritableUtils.writeVInt(out, m.size());
        for (Object o : m.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            write(entry.getKey(), out);
            write(entry.getValue(), out);
        }
    }

    private void encodePyDictionary(PyDictionary m, DataOutput out) throws IOException {
        out.writeByte(TOKEN_DICT);
        WritableUtils.writeVInt(out, m.size());
        PyObject iterator = m.iteritems();
        PyTuple tuple;
        while((tuple = (PyTuple) iterator.__iternext__()) != null)
        {
            write(tuple.pyget(0), out);
            write(tuple.pyget(1), out);
        }
    }

    private static class StringInternMap
    {
        private WeakHashMap map = new WeakHashMap();

        public synchronized PyUnicode intern(String s)
        {
            WeakReference reference = (WeakReference) map.get(s);
            PyUnicode ret;
            if(reference != null)
            {
                ret = (PyUnicode) reference.get();
                if(ret != null) return ret;
            }
            ret = new PyUnicode(s);
            map.put(s, new WeakReference(ret));
            return ret;
        }
    }
}
