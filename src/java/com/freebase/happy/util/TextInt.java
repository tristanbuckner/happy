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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  Stores a text and int together - useful for secondary sorts.
 */
public class TextInt implements WritableComparable {
    private Text text = new Text();
    private IntWritable integer = new IntWritable();

    public void setString(String s) {
        text.set(s);
    }

    public void setInt(int i) {
        integer.set(i);
    }

    public String getString() {
        return text.toString();
    }

    public int getInt() {
        return integer.get();
    }

    public void write(DataOutput dataOutput) throws IOException {
        text.write(dataOutput);
        integer.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        text.readFields(dataInput);
        integer.readFields(dataInput);
    }

    public int compareTo(Object o) {
        TextInt that = (TextInt) o;
        if (this == that) {
            return 0;
        } else {
            int ret = text.compareTo(that.text);
            if (ret != 0) return ret;
            return integer.compareTo(that.integer);
        }
    }

    public String toString()
    {
        return text.toString() + ":" + integer.get();
    }

    public int hashCode()
    {
        return text.hashCode() ^ integer.hashCode();
    }

    public boolean equals(Object o)
    {
        if(!(o instanceof TextInt)) return false;
        return compareTo(o) == 0;
    }

    ////////////////////////////////////////////////////////

    /**
     * Does a full compare of the text and then the int.
     */
    public static class FullComparator extends WritableComparator {
        protected FullComparator() {
            super(TextInt.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try
            {
                // get the size of the length vint:
                int n1 = WritableUtils.decodeVIntSize(b1[s1]);
                int n2 = WritableUtils.decodeVIntSize(b2[s2]);
                // get the length of the remaining string:
                int tlen1 = WritableComparator.readVInt(b1, s1);
                int tlen2 = WritableComparator.readVInt(b2, s2);

                // compare the text:
                int ret = WritableComparator.compareBytes(b1, s1+n1, tlen1, b2, s2+n2, tlen2);
                if(ret != 0) return ret;

                // compare the integer:
                int thisValue = WritableComparator.readInt(b1, s1 + n1 + tlen1);
                int thatValue = WritableComparator.readInt(b2, s2 + n2 + tlen2);
                return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }

        public int compare(Object o1, Object o2) {
            return ((TextInt)o1).compareTo(((TextInt)o2));
        }
    }

    /**
     * Compares just the text.
     */
    public static class TextComparator implements RawComparator {
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try
            {
                // get the size of the length vint:
                int n1 = WritableUtils.decodeVIntSize(b1[s1]);
                int n2 = WritableUtils.decodeVIntSize(b2[s2]);
                // get the length of the remaining string:
                int tlen1 = WritableComparator.readVInt(b1, s1);
                int tlen2 = WritableComparator.readVInt(b2, s2);

                // compare the text:
                return WritableComparator.compareBytes(b1, s1+n1, tlen1, b2, s2+n2, tlen2);
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }

        public int compare(Object o1, Object o2) {
            return ((TextInt)o1).text.compareTo(((TextInt)o2).text);
        }
    }

    static {
        // register the full comparator
        WritableComparator.define(TextInt.class, new FullComparator());
    }

    public static class TextPartitioner implements Partitioner
    {

        public void configure(JobConf jobConf) {
        }

        public int getPartition(Object key, Object value, int numReduceTasks) {
            return (((TextInt)key).text.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
}
