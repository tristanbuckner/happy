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

import org.python.core.*;
import org.python.util.PythonObjectInputStream;

import java.io.*;
import java.util.Iterator;

/**
 * Handles serialization of Jython objects.
 */
public class PyObjectSerializer {

    /**
     * Serializes a Jythom object to a file.
     *
     * @param o
     * @return
     * @throws IOException
     */
    public static File serialize(PyObject o) throws IOException {
        File tempfile = File.createTempFile("happy-", ".bin");
        FileOutputStream out = new FileOutputStream(tempfile);
        serialize(o, out);
        out.close();
        return tempfile;
    }

    /**
     * Deserializes a Jython object from a file.
     *
     * @param file
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static PyObject deserialize(File file) throws IOException, ClassNotFoundException {
        InputStream in = new FileInputStream(file);
        PyObject ret = deserialize(in);
        in.close();
        return ret;
    }

    /**
     * Serialized a Python object to an OutputStream.
     *
     * @param o
     * @param out
     * @throws IOException
     */
    public static void serialize(PyObject o, OutputStream out) throws IOException {
        JythonObjectOutputStream jout = new JythonObjectOutputStream(out);
        jout.writeObject(o);
        jout.flush();
    }

    /**
     * Deserializes a Jython object from an InputStream.
     *
     * @param in
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static PyObject deserialize(InputStream in) throws IOException, ClassNotFoundException {
        JythonObjectInputStream jin = new JythonObjectInputStream(in);
        return (PyObject) jin.readObject();
    }

    /**
     * A hack to work around a bug in serialization of PyStringMap.
     */
    public static class JythonObjectInputStream extends PythonObjectInputStream {
        public JythonObjectInputStream(InputStream istr) throws IOException {
            super(istr);
            super.enableResolveObject(true);
        }

        protected Object resolveObject(Object o) throws IOException {
            if (o instanceof PyFunctionPlaceholder)
            {
                return ((PyFunctionPlaceholder)o).getFunction();
            }
            else if (o instanceof PyMethodPlaceholder)
            {
                return ((PyMethodPlaceholder)o).getMethod();
            }
            else return o;
        }
    }

    /**
     * A hack to work around a bug in serialization of PyStringMap.
     */
    public static class JythonObjectOutputStream extends ObjectOutputStream {
        public JythonObjectOutputStream(OutputStream outputStream) throws IOException {
            super(outputStream);
            super.enableReplaceObject(true);
        }

        protected Object replaceObject(Object o) throws IOException {
            if (o instanceof PyMethod) return new PyMethodPlaceholder((PyMethod) o);
            else if (o instanceof PyFunction) return new PyFunctionPlaceholder((PyFunction) o);
            else return o;
        }
    }

    public static class PyMethodPlaceholder implements Serializable {
        private PyObject self;
        private String pyClass;
        private String pyClassModule;
        private String methodName;

        public PyMethodPlaceholder(PyMethod pyMethod) throws NotSerializableException {
            self = pyMethod.im_self;
            methodName = ((PyFunction)pyMethod.im_func).__name__;

            PyClass clazz = (PyClass) pyMethod.im_class;
            PyObject moduleName = clazz.__findattr__("__module__");
            if (moduleName == null || !(moduleName instanceof PyString) || moduleName == Py.None) {
                throw new NotSerializableException("Can't find module for class: " + clazz.__name__);
            }
            pyClassModule = moduleName.toString();

            PyObject className = clazz.__findattr__("__name__");
            if (className == null || !(className instanceof PyString) || className == Py.None) {
                throw new NotSerializableException("Can't find module for class with no name: " + clazz);
            }
            pyClass = className.toString();
        }

        public PyMethod getMethod() throws InvalidObjectException {
            PyObject mod = imp.importName(pyClassModule.intern(), false);
            PyClass pyc = (PyClass)mod.__getattr__(pyClass.intern());
            PyFunction func = (PyFunction) ((PyMethod) pyc.__getattr__(methodName.intern())).im_func;
            return new PyMethod(func, self, pyc);
        }
    }

    public static class PyFunctionPlaceholder implements Serializable {
        private String importName;
        private String functionName;

        public PyFunctionPlaceholder(PyFunction pyFunction) throws NotSerializableException {
            PyCode pyCode = pyFunction.func_code;
            if (!(pyCode instanceof PyTableCode)) throw new NotSerializableException("Cannot serialize " + pyFunction);
            PyTableCode pyTableCode = (PyTableCode) pyCode;
            this.functionName = pyTableCode.co_name;
            // process import name:
            String path = pyTableCode.co_filename;
            if (!path.endsWith(".py")) throw new NotSerializableException("Cannot serialize " + pyFunction);
            // try to canonicalize the filename:
            File file = new File(path);
            if(file.exists()) path = file.getAbsolutePath();
            // try to find the import path name:
            this.importName = getImport(path);
            if(this.importName == null)
            {
                throw new NotSerializableException("Cannot find import path for " + path + " for " + pyFunction);
            }
        }

        private String getImport(String filename) throws NotSerializableException {
            Iterator<String> iterator = Py.getSystemState().path.iterator();
            String longestPath = null;
            while(iterator.hasNext())
            {
                String path = iterator.next();
                if(filename.startsWith(path))
                {
                    if(longestPath == null || path.length() > longestPath.length()) longestPath = path;
                }
            }
            // we found a match:
            if(longestPath != null)
            {
                String suffix = filename.substring(longestPath.length());
                if(suffix.startsWith("/") || suffix.startsWith("\\")) suffix = suffix.substring(1);
                if(suffix.endsWith(".py")) suffix = suffix.substring(0, suffix.length() - 3);
                if(suffix.endsWith("__init__")) suffix = suffix.substring(0, suffix.length() - 9);
                suffix = suffix.replace("/", ".");
                suffix = suffix.replace("\\", ".");
                return suffix;
            }
            // no match, we'll error out:
            else
            {
                return null;
            }
        }

        public PyFunction getFunction() throws InvalidObjectException {
            PyObject module = imp.importName(importName.intern(), false);
            if(module == null)
                throw new InvalidObjectException("Cannot deserialize " + functionName + " from " + importName);
            PyObject pyfunc = module.__findattr__(functionName.intern());
            if (pyfunc == null || !(pyfunc instanceof PyFunction))
                throw new InvalidObjectException("Cannot deserialize " + functionName + " from " + importName);
            return (PyFunction) pyfunc;
        }
    }
}
