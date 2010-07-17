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

import com.freebase.happy.util.TextOrSequenceInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * An InputFormat that handles Text keys and JSON values, seperated by a tab.
 */
public class JSONInputFormat extends TextOrSequenceInputFormat {


    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new JSONRecordReader(super.getRecordReader(split, job, reporter));
    }

    public static class JSONRecordReader implements RecordReader<Text, JSONWritable> {
        private RecordReader reader;
        private Text innervalue = new Text();

        public JSONRecordReader(RecordReader reader) throws IOException {
            this.reader = reader;
            if(!(reader.createKey() instanceof Text) || !(reader.createValue() instanceof Text))
            {
                throw new IllegalArgumentException("Input reader must have text keys and values: " + reader);
            }
        }

        public boolean next(Text key, JSONWritable value) throws IOException {
            if (reader.next(key, innervalue)) {
                value.setSerialized(innervalue.toString());
                return true;
            } else return false;
        }

        public Text createKey() {
            return new Text();
        }

        public JSONWritable createValue() {
            return new JSONWritable();
        }

        public long getPos() throws IOException {
            return reader.getPos();
        }

        public void close() throws IOException {
            reader.close();
        }

        public float getProgress() throws IOException {
            return reader.getProgress();
        }
    }
}
