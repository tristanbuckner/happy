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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

/**
 *
 */
public class TextSerializer implements Writable {
    private CharsetEncoder encoder;
    private CharsetDecoder decoder;
    private ByteBuffer byteBuffer;
    private CharBuffer charBuffer;

    public TextSerializer() {
    }

    public String getString() throws CharacterCodingException {
        if(decoder == null) decoder = Charset.forName("UTF-8").newDecoder();
        ensureCharCapacity(byteBuffer.limit());
        charBuffer.clear();
        decoder.reset();
        CoderResult result = decoder.decode(byteBuffer, charBuffer, true);
        if(result.isError()) result.throwException();
        return new String(charBuffer.array(), 0, charBuffer.position());
    }

    public void setString(String string) throws CharacterCodingException {
        if(encoder == null) encoder = Charset.forName("UTF-8").newEncoder();
        ensureByteCapacity((int) (string.length() * encoder.maxBytesPerChar()));
        byteBuffer.clear();
        encoder.reset();
        CoderResult result = encoder.encode(CharBuffer.wrap(string), byteBuffer, true);
        if(result.isError()) result.throwException();
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, byteBuffer.position());
        out.write(byteBuffer.array(), 0, byteBuffer.position());
    }

    public void readFields(DataInput in) throws IOException {
        int textLen = WritableUtils.readVInt(in);
        ensureByteCapacity(textLen);
        byteBuffer.clear();
        in.readFully(byteBuffer.array(), 0, textLen);
        byteBuffer.limit(textLen);
    }

    private void ensureByteCapacity(int capacity)
    {
        if(byteBuffer == null || byteBuffer.capacity() < capacity) byteBuffer = ByteBuffer.allocate(getPowerSize(capacity));
    }

    private void ensureCharCapacity(int capacity)
    {
        if(charBuffer == null || charBuffer.capacity() < capacity) charBuffer = CharBuffer.allocate(getPowerSize(capacity));
    }

    private int getPowerSize(int size)
    {
        int ret = 1024;
        while(ret < size) ret = 2 * ret;
        return ret;
    }
}
