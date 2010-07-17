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

import com.freebase.happy.util.JarUtil;
import org.python.core.*;
import org.python.util.InteractiveConsole;
import org.python.util.JLineConsole;
import org.python.util.PythonInterpreter;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * The job runner and interactive console.
 */
public class HappyJobRunner {
    public static boolean RUNNING = false;

    public static void main(String[] args) {
        try
        {
            // set up the init state:
            PySystemState.initialize(PySystemState.getBaseProperties(), null, args);

            // this turns on jython debugging:
            // Options.verbose = Py.DEBUG;

            if(args.length > 0)
            {
                // add the path of the script:
                String path = new java.io.File(args[0]).getParent();
                if (path == null) path = "";
                Py.getSystemState().path.insert(0, new PyString(path));
            }

            // make sure that this jar file is in the Jython classpath and we're using the right classloader:
            //Py.getSystemState().setClassLoader(HappyJobRunner.class.getClassLoader());
            String thisJar = JarUtil.findContainingPath(HappyJobRunner.class);
            if(thisJar != null && thisJar.endsWith(".jar"))
            {
                PySystemState.packageManager.addJar(thisJar, false);
            }

            // grab happy include paths:
            String happypath = System.getProperty("happy.path", "");
            String[] envIncludePaths = happypath.split(":");

            // set up the pythonpath:
            PyList pythonpath = Py.getSystemState().path;
            if(thisJar != null) pythonpath.insert(0, new PyString(thisJar));
            for(String envIncludePath: envIncludePaths)
                if(envIncludePath.length() > 0) pythonpath.insert(0, new PyString(envIncludePath));
            pythonpath.insert(0, new PyString("."));

            // create the right interpreter:
            PythonInterpreter pythonInterpreter;
            if(args.length > 0)
            {
                pythonInterpreter = new PythonInterpreter();
            }
            else
            {
                pythonInterpreter = new JLineConsole();
            }

            // default imports:
            imp.load("site");

            // exec the job code if there is a script:
            HappyJobRunner.RUNNING = true;
            if(args.length > 0)
            {
                pythonInterpreter.getLocals().__setitem__(new PyString("__file__"), new PyString(args[0]));
                pythonInterpreter.execfile(args[0]);
            }
            // otherwise run the interpreter:
            else
            {
                InteractiveConsole console = (InteractiveConsole) pythonInterpreter;
                console.interact();
            }
        }
        catch(Throwable e)
        {
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static String getAll(BufferedReader in) throws IOException {
        StringBuffer sb = new StringBuffer();
        String line;
        while((line = in.readLine()) != null) sb.append(line).append("\n");
        return sb.toString();
    }


}
