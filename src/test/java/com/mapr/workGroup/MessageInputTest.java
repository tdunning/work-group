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
            x.getNoteBuilder().setId(String.format("%08x", i)).build();
            out.write(x.build());
        }
        out.flush();

        for (int i = 0; i < 20; i++) {
            ProgressNote.Update x = in.read();
            assertEquals(String.format("%08x", i), x.getNote().getId());
        }

        assertNull(in.read());

        for (int i = 0; i < 3; i++) {
            ProgressNote.Update.Builder x = ProgressNote.Update.newBuilder();
            x.getNoteBuilder().setId(String.format("%08x", i)).build();
            out.write(x.build());
        }
        out.close();

        for (int i = 0; i < 3; i++) {
            ProgressNote.Update x = in.read();
            assertEquals(String.format("%08x", i), x.getNote().getId());
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
        x.getNoteBuilder().setId(String.format("%08x", 35)).setNote("this is a test").build();
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
        assertEquals(String.format("%08x", 35), zz.getNote().getId());
    }
}
