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
