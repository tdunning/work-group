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
