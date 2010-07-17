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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;

/**
 * Simple wrapper around Hadoops Apache logging / Log4J log infrastructure.
 */
public class HappyLogger {

    public static Log getLogger(String name)
    {
        return LogFactory.getLog("happy." + name);
    }

    public static Log getLogger()
    {
        return LogFactory.getLog("happy.task");
    }

    public static void setLevel(String level)
    {
        Log log = LogFactory.getLog("happy");
        if(log instanceof Log4JLogger)
        {
            org.apache.log4j.Level lvl = null;
            if(level.equals("trace")) lvl = org.apache.log4j.Level.TRACE;
            else if(level.equals("debug")) lvl = org.apache.log4j.Level.DEBUG;
            else if(level.equals("info")) lvl = org.apache.log4j.Level.INFO;
            else if(level.equals("warn")) lvl = org.apache.log4j.Level.WARN;
            else if(level.equals("error")) lvl = org.apache.log4j.Level.ERROR;
            else if(level.equals("fatal")) lvl = org.apache.log4j.Level.FATAL;
            if(lvl == null) throw new RuntimeException("Unknown log level " + level);
            ((Log4JLogger)log).getLogger().setLevel(lvl);
        }
        else if(log instanceof Jdk14Logger)
        {
            java.util.logging.Level lvl = null;
            if(level.equals("trace")) lvl = java.util.logging.Level.FINEST;
            else if(level.equals("debug")) lvl = java.util.logging.Level.FINE;
            else if(level.equals("info")) lvl = java.util.logging.Level.INFO;
            else if(level.equals("warn")) lvl = java.util.logging.Level.WARNING;
            else if(level.equals("error")) lvl = java.util.logging.Level.SEVERE;
            else if(level.equals("fatal")) lvl = java.util.logging.Level.SEVERE;
            if(lvl == null) throw new RuntimeException("Unknown log level " + level);
            ((Jdk14Logger)log).getLogger().setLevel(lvl);
        }
        else throw new RuntimeException("Unknown logger type " + log);
    }
}
