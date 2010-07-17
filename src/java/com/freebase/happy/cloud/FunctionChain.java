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

package com.freebase.happy.cloud;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.python.core.*;

import java.util.List;

/**
 *
 */
public class FunctionChain {
    private static final Log log = LogFactory.getLog(FunctionChain.class);
    private final PyObject[] chainedFunctions;
    private final PyObject errorHandler;
    private boolean exceptionThrown = false;

    public FunctionChain(List functionList, PyObject errorHandler)
    {
        log.info("FunctionChain: " + functionList.toString());
        chainedFunctions = (PyObject[]) functionList.toArray(new PyObject[functionList.size()]);
        this.errorHandler = errorHandler;
    }

    public void callChain(PyObject key, PyObject value) throws Throwable {
        callFunction(0, key, value);
    }

    private void callFunction(int functionIndex, PyObject key, PyObject value) throws Throwable {
        PyObject function = chainedFunctions[functionIndex];
        // call the function:
        try
        {
            PyObject iterator = function.__call__(key, value);
            // pass the values forward:
            if(functionIndex < chainedFunctions.length - 1 && iterator != null)
            {
                PyObject ret;
                while((ret = iterator.__iternext__()) != null)
                {
                    if(ret instanceof PySequenceList)
                    {
                        PySequenceList pyseq = (PySequenceList) ret;
                        if(pyseq.size() != 2)
                            throw new RuntimeException("Function must return a tuple containing a key and value, not " + ret.toString());
                        callFunction(functionIndex + 1, pyseq.pyget(0), pyseq.pyget(1));
                    }
                    else
                    {
                        throw new RuntimeException("Function must return a tuple containing a key and value, not " + ret.toString());
                    }
                }
            }
        }
        catch(Throwable t)
        {
            // pass through already thrown exceptions:
            if(exceptionThrown) throw t;

            // log and re-throw exceptions:
            DataException de = getDataException(t);
            String keyMsg = toString(key);
            String valueMsg = toString(value);
            String operation = function.__findattr__("__name__").toString() + "-" + functionIndex;
            if(de != null)
            {
                log.error("Exception caught in " + operation +
                        ", key:'" + keyMsg + "', value: '" + valueMsg + "'", de);
                errorHandler.__call__(new PyUnicode(keyMsg), new PyUnicode(valueMsg),
                        new PyUnicode(toString(de.getMessage())), new PyUnicode(operation));
            }
            else
            {
                exceptionThrown = true;
                throw new RuntimeException("Exception in " + operation +
                    ", key:'" + keyMsg + "', value: '" + valueMsg + "'", t);
            }
        }
    }

    private String toString(Object o)
    {
        if(o == null) return "null";
        else return String.valueOf(o);
    }

    private DataException getDataException(Throwable t)
    {
        // don't rethrow DataExceptions:
        if(t instanceof DataException) return (DataException)t;
        // weed thru pythons obfuscated exception handling for DataExceptions:
        if(t instanceof PyException)
        {
            PyException pye = (PyException) t;
            if(pye.value != null)
            {
                Object de = pye.value.__tojava__(DataException.class);
                if(de != null && de instanceof DataException) return (DataException) de;
            }
        }
        return null;
    }
}
