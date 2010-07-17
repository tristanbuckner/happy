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

import org.python.core.*;

import java.io.IOException;
import java.util.*;
import java.lang.ref.WeakReference;

/**
 * JavaScript encoder and decoder that works with Jython native objects.
 * The .encode() method also accepts Java List and Map classes.
 */
public class JSON {
    private static final StringInternMap internMap = new StringInternMap();
    private enum TokenType { object, array, stringValue, numberValue, booleanValue, nullValue }

    /**
     * Encodes an object to a JSON String.
     *
     * @param o
     * @return
     * @throws java.io.IOException
     */
    public static String encode(Object o) throws IOException {
        StringBuilder sb = new StringBuilder(512);
        encode(o, sb);
        return sb.toString();
    }

    /**
     * Decodes a JSON String to Jython native objects.
     *
     * @param s
     * @return
     * @throws java.io.IOException
     */
    public static Object decode(String s) throws IOException {
        if(s == null) throw new IOException("Cannot decode null");
        if(s.length() == 0) throw new IOException("Cannot decode an empty string");
        return decodeReader(new Input(s));
    }

    ////////////////////////// Encoder Methods //////////////////////////////

    private static void encode(Object o, StringBuilder sb) throws IOException {
        if(o == null) sb.append("null");
        else if(o instanceof PyDictionary) encodePyDictionary((PyDictionary)o, sb);
        else if(o instanceof PyBaseString) encodeString(o.toString(), sb);
        else if(o instanceof Number) sb.append(o.toString());
        else if(o instanceof Boolean) sb.append(o.toString().toLowerCase());
        else if(o instanceof String) encodeString((String)o, sb);
        else if(o instanceof PyBoolean) sb.append(o.toString().toLowerCase());
        else if(o instanceof PyFloat) sb.append(Double.toString(((PyFloat)o).getValue()));
        else if(o instanceof PyInteger) sb.append(Long.toString(((PyInteger)o).getValue()));
        else if(o instanceof PyLong) sb.append(Long.toString(((PyLong)o).getValue().longValue()));
        else if(o instanceof List) encodeList((List)o, sb);
        else if(o instanceof Map) encodeMap((Map)o, sb);
        else if(o.getClass().isArray()) encodeList(Arrays.asList(o), sb);
        else if(Py.None.equals(o)) sb.append("null");
        else
        {
            throw new IOException("Unknown object " + o.toString());
        }
    }

    private static void encodeList(List l, StringBuilder sb) throws IOException {
        sb.append('[');
        for(Iterator iterator = l.iterator(); iterator.hasNext();)
        {
            encode(iterator.next(), sb);
            if(iterator.hasNext()) sb.append(", ");
        }
        sb.append(']');
    }

    private static void encodeMap(Map m, StringBuilder sb) throws IOException {
        sb.append('{');
        for(Iterator iterator = m.entrySet().iterator(); iterator.hasNext();)
        {
            Map.Entry entry = (Map.Entry) iterator.next();
            encode(entry.getKey().toString(), sb);
            sb.append(':');
            encode(entry.getValue(), sb);
            if(iterator.hasNext()) sb.append(", ");
        }

        sb.append('}');
    }

    private static void encodePyDictionary(PyDictionary m, StringBuilder sb) throws IOException {
        sb.append('{');
        PyObject iterator = m.iteritems();
        PyTuple tuple;
        int counter = 0;
        while((tuple = (PyTuple) iterator.__iternext__()) != null)
        {
            PyObject key = (PyObject) tuple.pyget(0);
            PyObject value = (PyObject) tuple.pyget(1);
            encode(key, sb);
            sb.append(':');
            encode(value, sb);
            if(counter < m.size() - 1) sb.append(", ");
            counter++;
        }
        sb.append('}');
    }

    private static void encodeString(String s, StringBuilder sb)
    {
        sb.append('"');
        if(s.length() > 0)
        {
            int index;
            boolean clean = true;
            for(index = 0; index < s.length() && clean; index++)
            {
                char c = s.charAt(index);
                switch(c)
                {
                    case '"':
                    case '\\':
                    case '\b':
                    case '\f':
                    case '\n':
                    case '\r':
                    case '\t':
                        clean = false;
                    default:
                        if(c < ' ' || c > '~')
                        {
                            clean = false;
                        }
                }
            }
            if(clean) sb.append(s);
            else
            {
                index--;
                sb.append(s, 0, index);
                for(; index < s.length(); index++)
                {
                    char c = s.charAt(index);
                    switch(c)
                    {
                        case '"': sb.append("\\\""); break;
                        case '\\': sb.append("\\\\"); break;
                        case '\b': sb.append("\\b"); break;
                        case '\f': sb.append("\\f"); break;
                        case '\n': sb.append("\\n"); break;
                        case '\r': sb.append("\\r"); break;
                        case '\t': sb.append("\\t"); break;
                        default:
                            if(c < ' ' || c > '~')
                            {
                                sb.append("\\u");
                                sb.append(encodeHex((c >> 12) & 0xF));
                                sb.append(encodeHex((c >> 8) & 0xF));
                                sb.append(encodeHex((c >> 4) & 0xF));
                                sb.append(encodeHex(c & 0xF));
                            }
                            else sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static char encodeHex(int i) {
        switch(i)
        {
            case 0: return '0';
            case 1: return '1';
            case 2: return '2';
            case 3: return '3';
            case 4: return '4';
            case 5: return '5';
            case 6: return '6';
            case 7: return '7';
            case 8: return '8';
            case 9: return '9';
            case 10: return 'a';
            case 11: return 'b';
            case 12: return 'c';
            case 13: return 'd';
            case 14: return 'e';
            case 15: return 'f';
            default: throw new RuntimeException("Illegal hex digit " + i);
        }
    }

    ////////////////////////// Decoder Methods //////////////////////////////

    private static PyObject decodeReader(Input in) throws IOException {
        in.consumeWhitespace();
        TokenType type = getTokenType((char)in.peek());
        if(type == null) throw new IOException(in.generateError("invalid value"));
        switch(type)
        {
            case array: return decodeArray(in);
            case object: return decodeObject(in);
            case stringValue: return decodeString(in);
            case numberValue: return decodeNumber(in);
            case booleanValue: return decodeBoolean(in);
            case nullValue: return decodeNull(in);
            default: throw new IOException(in.generateError("invalid value"));
        }
    }

    private static PyObject decodeNull(Input in) throws IOException {
        int n = in.read();
        int u = in.read();
        int l1 = in.read();
        int l2 = in.read();

        if((n == 'N' || n == 'n') &&
           (u == 'U' || u == 'u' || u == 'o' || u == 'O') &&
           (l1 == 'L' || l1 == 'l' || l1 == 'n' || l1 == 'N') &&
           (l2 == 'L' || l2 == 'l' || l2 == 'e' || l2 == 'E')) return Py.None;
        else throw new IOException(in.generateError("invalid null value"));
    }

    private static PyObject decodeBoolean(Input in) throws IOException {
        int i = in.read();
        switch(i)
        {
            case 't':
            case 'T':
                if(in.read() == 'r' && in.read() == 'u' && in.read() == 'e') return Py.True;
                else break;
            case 'f':
            case 'F':
                if(in.read() == 'a' && in.read() == 'l' && in.read() == 's' && in.read() == 'e') return Py.False;
                else break;
        }
        throw new IOException(in.generateError("invalid boolean value"));
    }

    private static PyObject decodeNumber(Input in) throws IOException {
        boolean integer = true;
        boolean inNumber = true;
        StringBuilder number = in.getBuffer();
        int i;
        while(inNumber)
        {
            i = in.peek();
            switch(i)
            {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    number.append((char)i);
                    in.skip();
                    break;
                case '+':
                case '-':
                    if(number.length() != 0) integer = false;
                    number.append((char)i);
                    in.skip();
                    break;
                case '.':
                case 'e':
                case 'E':
                    integer = false;
                    number.append((char)i);
                    in.skip();
                    break;
                default:
                    inNumber = false;
            }
        }
        if(integer)
        {
            return Py.newInteger(Long.parseLong(number.toString()));
        }
        else return Py.newFloat(Double.parseDouble(number.toString()));
    }

    private static PyObject decodeString(Input in) throws IOException {
        int start = in.read();
        if(start != '\'' && start != '"') throw new IOException(in.generateError("invalid string value"));
        boolean inString = true;
        int startIndex = in.index();
        StringBuilder sb = null;
        while(inString)
        {
            int i = in.read();
            if(i < 0) throw new IOException(in.generateError("invalid string value"));
            switch(i)
            {
                case '\\':
                    if(sb == null)
                    {
                        sb = in.getBuffer();
                        sb.append(in.string(), startIndex, in.index() - 1);
                    }
                    int esc = in.read();
                    switch(esc)
                    {
                        case '\'':
                            sb.append('\'');
                            break;
                        case '\"':
                            sb.append('\"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u':
                            int code = (decodeDigit(in.read()) << 12) +
                                    (decodeDigit(in.read()) << 8) +
                                    (decodeDigit(in.read()) << 4) +
                                    decodeDigit(in.read());
                            sb.append((char)code);
                            break;
                        default:
                            throw new IOException(in.generateError("invalid escape sequence"));
                    }
                    break;
                case '\'':
                case '"':
                    if(i == start) inString = false;
                    else if(sb != null) sb.append((char)i);
                    break;
                default:
                    if(sb != null) sb.append((char)i);
            }
        }
        if(sb != null)
        {
            return internMap.intern(sb.toString(), false);
        }
        else
        {
            if(in.index() - startIndex <= 1) return Py.EmptyString;
            else return internMap.intern(in.string().substring(startIndex, in.index() - 1), true);
        }
    }

    /**
     * Decode a hex digit.
     *
     * @param digit
     * @return
     */
    private static int decodeDigit(int digit) {
        if(digit == -1) throw new RuntimeException("Invalid end of string");
        switch((char)digit)
        {
            case '0': return 0;
            case '1': return 1;
            case '2': return 2;
            case '3': return 3;
            case '4': return 4;
            case '5': return 5;
            case '6': return 6;
            case '7': return 7;
            case '8': return 8;
            case '9': return 9;
            case 'a': return 10;
            case 'A': return 10;
            case 'b': return 11;
            case 'B': return 11;
            case 'c': return 12;
            case 'C': return 12;
            case 'd': return 13;
            case 'D': return 13;
            case 'e': return 14;
            case 'E': return 14;
            case 'f': return 15;
            case 'F': return 15;
            default: throw new RuntimeException("Illegal hex digit " + (char)digit);
        }
    }

    private static PyObject decodeObject(Input in) throws IOException {
        int start = in.read();
        if(start != '{') throw new IOException(in.generateError("invalid object"));
        PyDictionary pyDictionary = new PyDictionary();

        // catch empty objects:
        in.consumeWhitespace();
        if(in.peek() == '}')
        {
            in.skip();
            return pyDictionary;
        }

        boolean inObject = true;
        while(inObject)
        {
            // get the key:
            in.consumeWhitespace();
            Object keyValue = decodeReader(in);
            if(!(keyValue instanceof PyString)) throw new IOException(in.generateError("key must be string"));

            // get the value:
            in.consumeWhitespace();
            if(in.read() != ':') throw new IOException(in.generateError("key missing value"));
            pyDictionary.__setitem__((PyString)keyValue, (PyObject) decodeReader(in));

            // move to the next key or the end of the object:
            in.consumeWhitespace();
            int i = in.read();
            if(i == '}') inObject = false;
            else if(i != ',') throw new IOException(in.generateError("invalid object"));
        }
        return pyDictionary;
    }

    private static PyObject decodeArray(Input in) throws IOException {
        int start = in.read();
        if(start != '[') throw new IOException(in.generateError("invalid array"));

        // catch empty arrays:
        in.consumeWhitespace();
        if(in.peek() == ']')
        {
            in.skip();
            return new PyList();
        }

        PyList objects = new PyList();
        boolean inArray = true;
        while(inArray)
        {
            // get the value:
            in.consumeWhitespace();
            objects.pyadd((PyObject) decodeReader(in));

            // move to the next value or the end of the array:
            in.consumeWhitespace();
            int i = in.read();
            if(i == ']') inArray = false;
            else if(i != ',') throw new IOException(in.generateError("invalid array"));
        }
        return objects;
    }

    private static TokenType getTokenType(char c)
    {
        switch(c)
        {
            case '[':
                return TokenType.array;
            case '{':
                return TokenType.object;
            case '"':
            case '\'':
                return TokenType.stringValue;
            case '-':
            case '+':
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '.':
                return TokenType.numberValue;
            case 't':
            case 'T':
            case 'f':
            case 'F':
                return TokenType.booleanValue;
            case 'n':
            case 'N':
                return TokenType.nullValue;
            default:
                return null;
        }
    }

    private static class StringInternMap
    {
        private WeakHashMap map = new WeakHashMap();

        public synchronized PyUnicode intern(String s, boolean recreate)
        {
            WeakReference reference = (WeakReference) map.get(s);
            PyUnicode ret;
            if(reference != null)
            {
                ret = (PyUnicode) reference.get();
                if(ret != null) return ret;
            }
            // this recreates the string to prevent memory leaks by String.substring():
            if(recreate) s = new String(s);
            ret = new PyUnicode(s);
            map.put(s, new WeakReference(ret));
            return ret;
        }
    }

    private static class Input
    {
        private String s;
        private int i = 0;
        private int length;
        private StringBuilder buffer = null;

        private Input(String s) {
            this.s = s;
            this.length = s.length();
        }

        public StringBuilder getBuffer()
        {
            if(buffer == null) buffer = new StringBuilder(256);
            else buffer.setLength(0);
            return buffer;
        }

        public int read()
        {
            if(i < length) return s.charAt(i++);
            else return -1;
        }

        public void skip()
        {
            i++;
        }

        public void skip(int s)
        {
            i += s;
        }

        public int index()
        {
            return i;
        }

        public String string()
        {
            return s;
        }

        public int peek()
        {
            if(i < length) return s.charAt(i);
            else return -1;
        }

        public String generateError(String error)
        {
            int start = i - 5;
            if(start < 0) start = 0;
            int end = start + 10;
            if(end > length) end = length;
            return "Error " + error + " at character " + i + ": ..." + s.substring(start, end) + "...";
        }

        public void consumeWhitespace()
        {
            while(i < length)
            {
                int c = s.charAt(i);
                switch((char)c)
                {
                    case ' ':
                    case '\t':
                    case '\n':
                    case '\r':
                    case '\f': break;
                    default: return;
                }
                i++;
            }
        }
    }
}