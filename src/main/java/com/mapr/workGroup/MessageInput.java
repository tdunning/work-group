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

import com.google.protobuf.GeneratedMessage;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Reads protobuf messages from a file carefully checking to make sure that messages are
 * complete and not perturbed by EOF effects.
 */
public abstract class MessageInput<T extends GeneratedMessage> {
    private long offset = 0;
    private FileChannel channel;
    private DataInputStream stream;

    public MessageInput(Path p) throws IOException {
        channel = FileChannel.open(p, StandardOpenOption.READ);
        stream = new DataInputStream(Channels.newInputStream(channel));
    }

    public T read() throws IOException {
        T r;
        try {
            r = parse(stream);
            long after = channel.position();
            int n = stream.readInt();
            if (after - offset != n) {
                channel.position(offset);
                return null;
            }
        } catch (EOFException e) {
            channel.position(offset);
            return null;
        }
        offset = channel.position();
        return r;
    }

    public abstract T parse(InputStream in) throws IOException;
}
