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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A wrapper for a dataset path that allows some common actions.
 */
public class DatasetPath {
    private Path path;
    private FileSystem fileSystem;

    public DatasetPath(String path) throws IOException {
        this(new JobConf(), path);
    }

    public DatasetPath(JobConf jobConf, String path) throws IOException {
        this.path = new Path(path);
        fileSystem = FileSystem.get(jobConf);
    }

    public DatasetPath(FileSystem fileSystem, String path) throws IOException {
        this.fileSystem = fileSystem;
        this.path = new Path(path);
    }

    /**
     * Return the path.
     *
     * @return
     */
    public String getPath() {
        return path.getName();
    }

    /**
     * Returns an inputstream for this file or all the files in this path.
     *
     * @return
     * @throws IOException
     */
    public InputStream getInputStream() throws IOException {
        List<Path> paths = new LinkedList<Path>();
        if(fileSystem.exists(path))
        {
            if(fileSystem.getFileStatus(path).isDir())
            {
                FileStatus[] children = fileSystem.listStatus(path);
                for(FileStatus child: children)
                {
                    if(!child.isDir()) paths.add(child.getPath());
                }
            }
            else paths.add(path);
        }
        else throw new IOException("Path " + path + " doesn't exist");
        return new MultiFileInputStream(paths);
    }

    /**
     * Returns an open BufferedReader for reading from this file.
     *
     * @return
     * @throws IOException
     */
    public BufferedReader getReader() throws IOException
    {
        return new BufferedReader(new InputStreamReader(getInputStream(), "utf-8"));
    }

    /**
     * Returns an open OutputStream for writing to this file.
     * Throws an exception if the file already exists.
     *
     * @return
     * @throws IOException
     */
    public OutputStream getOutputStream() throws IOException
    {
        return fileSystem.create(path, false);
    }

    /**
     * Returns an open OutputStream for writing to this file using the specified compression codec.
     * Throws an exception if the file already exists.
     *
     * @return
     * @throws IOException
     */
    public OutputStream getOutputStream(CompressionCodec codec) throws IOException
    {
        if(codec == null) return getOutputStream();
        else return codec.createOutputStream(fileSystem.create(path, false));
    }

    /**
     * Returns an open PrintWriter for writing to this file.
     * Throws an exception if the file already exists.
     *
     * @return
     * @throws IOException
     */
    public PrintWriter getWriter() throws IOException
    {
        return new PrintWriter(new OutputStreamWriter(getOutputStream(), "utf-8"));
    }

    /**
     * Returns an open PrintWriter for writing to this file using the specified compression codec.
     * Throws an exception if the file already exists.
     *
     * @return
     * @throws IOException
     */
    public PrintWriter getWriter(CompressionCodec codec) throws IOException
    {
        if(codec == null) return getWriter();
        else return new PrintWriter(new OutputStreamWriter(getOutputStream(codec), "utf-8"));
    }

    /**
     * Returns a reader for a SequenceFile.
     *
     * @return
     * @throws IOException
     */
    public SequenceFile.Reader getSequenceFileReader() throws IOException {
        return new SequenceFile.Reader(fileSystem, path, fileSystem.getConf());
    }

    /**
     * Iterate through the lines in the file or files in this path.
     *
     * @return
     * @throws IOException
     */
    public Iterator<String> iterateLines() throws IOException {
        return new DirectoryLineIterator(getInputStream());
    }

    /**
     * Iterate through the lines in the file or files in this path that contain
     * a match with the given regex.
     *
     * @param regex
     * @return
     * @throws IOException
     */
    public Iterator<String> grepLines(String regex) throws IOException {
        return new RegexLineIterator(getInputStream(), regex);
    }

    /**
     * Load all of the lines into a List.  Be careful, this may use a lot of memory.
     *
     * @return
     * @throws java.io.IOException
     */
    public List<String> loadAllLines() throws IOException {
        Iterator<String> iterator = this.iterateLines();
        List<String> ret = new ArrayList<String>();
        while(iterator.hasNext()) ret.add(iterator.next());
        return ret;
    }

    /**
     * Loads all of the data into a String.  Be careful, this may use a lot of memory.
     *
     * @return
     * @throws IOException
     */
    public String loadAll() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        IOUtils.copyBytes(getInputStream(), buffer, 1024, true);
        return new String(buffer.toByteArray(), "utf-8");
    }

    /**
     * Returns true if this path exists.
     *
     * @return
     */
    public boolean exists() throws IOException {
        return fileSystem.exists(path);
    }

    /**
     * @return the org.apache.hadoop.fs.FileStatus for this path.
     */
    public FileStatus getFileStatus() throws IOException {
        return fileSystem.getFileStatus(path);
    }

    /**
     * Delete this path.
     *
     * @throws IOException
     */
    public void deletePath() throws IOException {
        if(fileSystem.exists(path))
        {
            FileUtil.fullyDelete(fileSystem, path);
        }
    }

    /**
     * Copies a path to another path.
     *
     * @param path
     * @throws IOException
     */
    public void copyTo(DatasetPath path) throws IOException {
        InputStream in = null;
        OutputStream out = null;
        try
        {
            in = getInputStream();
            out = path.getOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
        }
        finally
        {
            if(out != null) out.close();
            if(in != null) in.close();
        }
    }

    /**
     * Copies a path to a local path.
     *
     * @param path
     * @throws IOException
     */
    public void copyToLocal(String path) throws IOException {
        InputStream in = null;
        OutputStream out = null;
        try
        {
            in = getInputStream();
            out = new FileOutputStream(path, false);
            byte[] buffer = new byte[4096];
            int read;
            while((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
        }
        finally
        {
            if(out != null) out.close();
            if(in != null) in.close();
        }
    }

    /**
     * Copies from a local path.
     *
     * @param path
     * @throws IOException
     */
    public void copyFromLocal(String path) throws IOException {
        InputStream in = null;
        OutputStream out = null;
        try
        {
            in = new FileInputStream(path);
            out = getOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
        }
        finally
        {
            if(out != null) out.close();
            if(in != null) in.close();
        }
    }

    /**
     * Renames this path to a new path name.  This path now refers to the new name.
     *
     * @param name
     * @throws IOException
     */
    public void rename(String name) throws IOException {
        Path newPath = new Path(name);
        fileSystem.rename(path, newPath);
        path = newPath;
    }

    private class MultiFileInputStream extends InputStream
    {
        private List<Path> paths;
        private InputStream currentStream;

        private MultiFileInputStream(List<Path> paths) {
            this.paths = paths;
        }

        public int read(byte[] bytes) throws IOException {
            // try to read:
            if(currentStream != null)
            {
                int ret = currentStream.read(bytes);
                if(ret > -1) return ret;
            }

            // if we failed, get the next stream:
            while(currentStream != null || paths.size() > 0)
            {
                nextStream();
                if(currentStream != null)
                {
                    int ret = currentStream.read(bytes);
                    if(ret > -1) return ret;
                }
            }

            // we are done:
            return -1;
        }

        public int read(byte[] bytes, int off, int len) throws IOException {
            // try to read:
            if(currentStream != null)
            {
                int ret = currentStream.read(bytes, off, len);
                if(ret > -1) return ret;
            }

            // if we failed, get the next stream:
            while(currentStream != null || paths.size() > 0)
            {
                nextStream();
                if(currentStream != null)
                {
                    int ret = currentStream.read(bytes, off, len);
                    if(ret > -1) return ret;
                }
            }

            // we are done:
            return -1;
        }

        public int read() throws IOException {
            // try to read:
            if(currentStream != null)
            {
                int ret = currentStream.read();
                if(ret > -1) return ret;
            }

            // if we failed, get the next stream:
            while(currentStream != null || paths.size() > 0)
            {
                nextStream();
                if(currentStream != null)
                {
                    int ret = currentStream.read();
                    if(ret > -1) return ret;
                }
            }

            // we are done:
            return -1;
        }

        private void nextStream() throws IOException {
            if(currentStream != null) currentStream.close();
            if(paths.size() > 0)
            {
                currentStream = fileSystem.open(paths.remove(0));
            }
            else currentStream = null;
        }

        public void close() throws IOException {
            if(currentStream != null) currentStream.close();
            currentStream = null;
            paths.clear();
        }
    }


    private class DirectoryLineIterator implements Iterator<String>
    {
        protected String currentLine;
        private BufferedReader reader;

        private DirectoryLineIterator(InputStream in) throws UnsupportedEncodingException {
            reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        }

        public boolean hasNext() {
            try {
                // if we have a line, true:
                if(currentLine != null) return true;
                // no reader, we must be done:
                if(reader == null) return false;
                // get a line:
                currentLine = reader.readLine();
                // got a line?  true:
                if(currentLine != null) return true;
                // otherwise, we are done, clean up:
                reader.close();
                reader = null;
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public String next() {
            if(hasNext())
            {
                String ret = currentLine;
                currentLine = null;
                return ret;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class RegexLineIterator extends DirectoryLineIterator
    {
        private Pattern pattern;
        private boolean done = false;

        public RegexLineIterator(InputStream in, String regex) throws UnsupportedEncodingException {
            super(in);
            pattern = Pattern.compile(regex);
        }

        public boolean hasNext() {
            if(done) return false;
            while(true)
            {
                // check if we are done:
                if(!super.hasNext())
                {
                    done = true;
                    return false;
                }
                // a match:
                if(pattern.matcher(super.currentLine).find()) return true;
                // next line:
                super.currentLine = null;
            }
        }

        public String next() {
            return super.next();
        }
    }
}