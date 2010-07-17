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

import org.python.core.imp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

/**
 */
public class JarUtil {

    private static final Log log = LogFactory.getLog(JarUtil.class);

    /**
     * Finds the path that contains a jar file containing the specified class.
     * Compiles python sources found along the way.
     *
     * @param my_class the class to find.
     * @return a jar file or path that contains the class, or null.
     * @throws IOException
     */
    public static String findContainingPath(Class my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration itr = loader.getResources(class_file);
                itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                if(url.getProtocol().equals("jar"))
                {
                    toReturn = toReturn.replaceAll("!.*$", "");
                }
                else if(url.getProtocol().equals("file"))
                {
                    toReturn = toReturn.substring(0, toReturn.length() - class_file.length());
                }
                return toReturn;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Builds a new jar from a set of existing jars and files.
     * Compiles Python sources found along the way.
     *
     * @param inputJars
     * @param inputFiles
     * @param outputJar
     * @throws IOException
     */
    public static void buildJar(List<String> inputJars, List<String> inputFiles, String outputJar) throws IOException {
        JarOutputStream out = new JarOutputStream(new FileOutputStream(outputJar));
        out.setLevel(9);
        byte[] buffer = new byte[4096];
        Set loadedEntries = new HashSet();

        // copy over the jars:
        for(String jarFile: inputJars)
        {
            JarInputStream in = new JarInputStream(new FileInputStream(jarFile));
            JarEntry nextEntry;
            while((nextEntry = in.getNextJarEntry()) != null)
            {
                JarEntry newEntry = new JarEntry(nextEntry);
                newEntry.setCompressedSize(-1);
                String name = newEntry.getName();
                // compile python source:
                if(isPython(name))
                {
                    // buffer the src:
                    ByteArrayOutputStream srcbuffer = new ByteArrayOutputStream();
                    int read;
                    while((read = in.read(buffer)) != -1) srcbuffer.write(buffer, 0, read);
                    byte[] src = srcbuffer.toByteArray();
                    // save the src:
                    // # don't include .py files to work around Hadoop compilation race condition:
                    //out.putNextEntry(newEntry);
                    //out.write(src);
                    loadedEntries.add(name);
                    // save the compiled file:
                    log.debug("Compiling " + name);
                    compile(name, src, out);
                }
                else if(!loadedEntries.contains(name) && !isCompiledPython(name))
                {
                    out.putNextEntry(newEntry);
                    int read;
                    while((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
                    loadedEntries.add(name);
                }
            }
            in.close();
        }

        // copy over files and directories:
        for(String fileName: inputFiles)
        {
            File file = new File(fileName);
            if(!file.exists()) throw new IOException("Path " + file.getAbsolutePath() + " doesn't exist!");
            buildJar("", file, out, buffer, loadedEntries);
        }
        out.close();
    }

    /**
     * Recurses over a directory tree to load a jar file.
     * Compiles Python sources found along the way.
     *
     * @param entryPath
     * @param file
     * @param out
     * @param buffer
     * @throws IOException
     */
    private static void buildJar(String entryPath, File file, JarOutputStream out, byte[] buffer, Set loadedEntries) throws IOException {
        if(file.isFile())
        {
            String name = entryPath + file.getName();
            if(isPython(name))
            {
                // buffer the src:
                InputStream in = new FileInputStream(file);
                ByteArrayOutputStream srcbuffer = new ByteArrayOutputStream();
                int read;
                while((read = in.read(buffer)) != -1) srcbuffer.write(buffer, 0, read);
                in.close();
                byte[] src = srcbuffer.toByteArray();
                // save the src:
                // # don't include .py files to work around Hadoop compilation race condition:
                //JarEntry nextEntry = new JarEntry(name);
                //nextEntry.setTime(file.lastModified());
                //out.putNextEntry(nextEntry);
                //out.write(src);
                loadedEntries.add(name);
                // save the compiled file:
                log.debug("Compiling " + name);
                compile(name, src, out);
            }
            else if(!loadedEntries.contains(name) && !isCompiledPython(name))
            {
                InputStream in = new FileInputStream(file);
                JarEntry nextEntry = new JarEntry(name);
                nextEntry.setTime(file.lastModified());
                out.putNextEntry(nextEntry);
                int read;
                while((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
                loadedEntries.add(name);
            }
        }
        else if(file.isDirectory())
        {
            String newEntryPath = entryPath + file.getName() + "/";
            File[] children = file.listFiles();
            for(File child: children)
            {
                buildJar(newEntryPath, child, out, buffer, loadedEntries);
            }
        }
    }

    private static boolean isPython(String name)
    {
        return name.endsWith(".py");
    }

    private static boolean isCompiledPython(String name)
    {
        return name.endsWith("$py.class");
    }

    private static void compile(String filename, byte[] source, JarOutputStream out) throws IOException {
        try
        {
            File tempfile = File.createTempFile("source", ".py");
            tempfile.deleteOnExit();
            FileOutputStream tempOut = new FileOutputStream(tempfile);
            tempOut.write(source);
            tempOut.close();
            String name = filename.substring(filename.lastIndexOf("/") + 1, filename.lastIndexOf("."));
            byte[] compiled = imp.compileSource(name, tempfile, filename, null);
            String newFileName = filename.substring(0, filename.lastIndexOf("/") + 1) + name + "$py.class";
            JarEntry entry = new JarEntry(newFileName);
            entry.setTime(System.currentTimeMillis());
            out.putNextEntry(entry);
            out.write(compiled);
            tempfile.delete();
        }
        catch(Exception e)
        {
            log.error("Error compiling " + filename + ", skipping compilation", e);
        }
    }
}
