package com.mapr.workGroup;

import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessageInputTest {
    @Test
    public void testRead() throws IOException {
        Path tmp = Files.createTempFile("foo", "data");
        MessageInput<ProgressNote.Update> in = new MessageInput<ProgressNote.Update>(tmp) {
            @Override
            public ProgressNote.Update parse(InputStream in) throws IOException {
                return ProgressNote.Update.parseDelimitedFrom(in);
            }
        };

        assertNull(in.read());

        MessageOutput<ProgressNote.Update> out = new MessageOutput<ProgressNote.Update>(tmp) {
            @Override
            public void writeDelimitedTo(OutputStream stream, ProgressNote.Update v) throws IOException {
                v.writeDelimitedTo(stream);
            }
        };
        for (int i = 0; i < 20; i++) {
            ProgressNote.Update.Builder x = ProgressNote.Update.newBuilder();
            x.getNoteBuilder().setId(i).build();
            out.write(x.build());
        }
        out.flush();

        for (int i = 0; i < 20; i++) {
            ProgressNote.Update x = in.read();
            assertEquals(i, x.getNote().getId());
        }

        assertNull(in.read());

        for (int i = 0; i < 3; i++) {
            ProgressNote.Update.Builder x = ProgressNote.Update.newBuilder();
            x.getNoteBuilder().setId(i).build();
            out.write(x.build());
        }
        out.close();

        for (int i = 0; i < 3; i++) {
            ProgressNote.Update x = in.read();
            assertEquals(i, x.getNote().getId());
        }
    }

    @Test
    public void testPartial() throws IOException {
        Path tmp = Files.createTempFile("foo", "data");
        MessageInput<ProgressNote.Update> in = new MessageInput<ProgressNote.Update>(tmp) {
            @Override
            public ProgressNote.Update parse(InputStream in) throws IOException {
                return ProgressNote.Update.parseDelimitedFrom(in);
            }
        };

        assertNull(in.read());

        DataOutputStream out = new DataOutputStream(Files.newOutputStream(tmp));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ProgressNote.Update.Builder x = ProgressNote.Update.newBuilder();
        x.getNoteBuilder().setId(35).setNote("this is a test").build();
        x.build().writeDelimitedTo(baos);
        byte[] buf = baos.toByteArray();
        assertTrue(buf.length > 3);

        out.write(buf, 0, 3);
        out.flush();
        assertEquals(3, tmp.toFile().length());
        ProgressNote.Update z = in.read();
        assertNull(z);

        out.write(buf, 3, buf.length - 3);
        out.writeInt(buf.length);
        out.flush();
        ProgressNote.Update zz = in.read();
        assertEquals(35, zz.getNote().getId());
    }
}
