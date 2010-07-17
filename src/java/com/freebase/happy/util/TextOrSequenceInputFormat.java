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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * An InputFormat that handles both Text and KeyValue files.
 */
public class TextOrSequenceInputFormat extends FileInputFormat implements JobConfigurable {
    public static final Log LOG =
      LogFactory.getLog(TextOrSequenceInputFormat.class);

    private CompressionCodecFactory compressionCodecs = null;

    public TextOrSequenceInputFormat() {
      setMinSplitSize(SequenceFile.SYNC_INTERVAL);
    }

    public void configure(JobConf conf) {
      compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
      return compressionCodecs.getCodec(file) == null;
    }

    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        FileSystem fs = fileSplit.getPath().getFileSystem(job);
        reporter.setStatus(split.toString());
        if(isSequenceFile(fs, fileSplit.getPath()))
        {
            LOG.info("Opening " + fileSplit.getPath() + " as SequenceFile");
            return new SequenceFileRecordReader(job, fileSplit);
        }
        else
        {
            LOG.info("Opening " + fileSplit.getPath() + " as text file");
            return new KeyValueLineRecordReader(job, fileSplit);
        }
    }

    private boolean isSequenceFile(FileSystem fs, Path file) throws IOException {
        InputStream in = fs.open(file);
        try
        {
            if(in.read() == (byte)'S' && in.read() == (byte)'E' && in.read() == (byte)'Q') return true;
        }
        finally
        {
            in.close();
        }
        return false;
    }
}
