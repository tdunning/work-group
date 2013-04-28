/*
 * Copyright MapR Technologies, 2013
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.workGroup;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class MessageOutput<T> {
    private FileChannel channel;
    private DataOutputStream stream;

    public MessageOutput(Path p) throws IOException {
        channel = FileChannel.open(p, StandardOpenOption.WRITE);
        stream = new DataOutputStream(Channels.newOutputStream(channel));
    }

    public void write(T v) throws IOException {
        long offset = channel.position();
        writeDelimitedTo(stream, v);
        stream.writeInt((int) (channel.position() - offset));
    }

    public abstract void writeDelimitedTo(OutputStream stream, T v) throws IOException;

    public void flush() throws IOException {
        stream.flush();
    }

    public void close() throws IOException {
        stream.close();
    }
}
