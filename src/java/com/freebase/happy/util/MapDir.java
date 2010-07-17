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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MapDir {
    private Path rootDir;
    private FileSystem fileSystem;
    private Configuration jobConf;
    private MapFile.Reader[] readers;
    private HashPartitioner hashPartitioner = new HashPartitioner();
    private Class keyClass;
    private Class valueClass;

    public MapDir(FileSystem fileSystem, String path, Configuration jobConf) throws IOException, IllegalAccessException, InstantiationException {
        this.fileSystem = fileSystem;
        rootDir = new Path(path);
        this.jobConf = jobConf;
        List<Path> mapfiles = getMapFiles();
        if(mapfiles.size() == 0) throw new IOException("No valid map directories found in " + path);
        readers = new MapFile.Reader[mapfiles.size()];
        for(int i = 0; i < mapfiles.size(); i++)
        {
            readers[i] = new MapFile.Reader(fileSystem, mapfiles.get(i).toString(), jobConf);
            if(keyClass == null)
            {
                keyClass = readers[i].getKeyClass();
                valueClass = readers[i].getValueClass();
            }
            else
            {
                if(!keyClass.equals(readers[i].getKeyClass()))
                {
                    throw new IOException("Key classes " + keyClass.getClass() + " and " + readers[i].getKeyClass() + " don't match");
                }
                if(!valueClass.equals(readers[i].getValueClass()))
                {
                    throw new IOException("Value classes " + valueClass.getClass() + " and " + readers[i].getValueClass() + " don't match");
                }
            }
        }
    }

    public Writable get(WritableComparable key, Writable val) throws IOException {
        int index = hashPartitioner.getPartition(key, val, readers.length);
        return readers[index].get(key, val);
    }

    public Class getKeyClass()
    {
        return keyClass;
    }

    public Class getValueClass()
    {
        return valueClass;
    }

    public void close() throws IOException {
        for(MapFile.Reader reader: readers) reader.close();
    }

    private List<Path> getMapFiles() throws IOException {
        List<Path> ret = new ArrayList<Path>();
        NumberFormat format = new DecimalFormat("00000");
        int index = 0;
        while(true)
        {
            Path currentDir = new Path(rootDir, "part-" + format.format(index));
            if(fileSystem.exists(currentDir) && fileSystem.getFileStatus(currentDir).isDir())
            {
                ret.add(currentDir);
                index++;
            }
            else
            {
                return ret;
            }
        }
    }

}
