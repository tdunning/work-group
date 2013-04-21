package com.mapr.workGroup;

import com.google.common.collect.Maps;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Random;

/**
 * Record progress.
 */
public class ProgressLog {
    private FileOutputStream stream;
    private final long id;
    private static boolean returnStatus = false;
    private static String backTrace = null;
    private static String message;
    private static boolean completed;

    private ProgressLog(String name, FileOutputStream stream) throws IOException {
        this.stream = stream;
        this.id = new Random().nextLong();
        ProgressNote.Update.Builder out = ProgressNote.Update.newBuilder();
        out.getStartBuilder()
                .setId(id)
                .setTime(System.currentTimeMillis())
                .setName(name)
                .build();
        out.build().writeDelimitedTo(stream);
    }

    public static ProgressLog open(String ourName, File f) throws IOException {
        return new ProgressLog(ourName, new FileOutputStream(f, true));
    }

    public void increment(String name, int delta) throws IOException {
        ProgressNote.Update.Builder out = ProgressNote.Update.newBuilder();
        ProgressNote.Progress.Builder note = out.getProgressBuilder()
                .setId(id)
                .setTime(System.currentTimeMillis());
        note.addCountBuilder().setKey(name).setIntegerIncrement(delta).build();
        out.build().writeDelimitedTo(stream);
    }

    public void note(String note) throws IOException {
        ProgressNote.Update.Builder out = ProgressNote.Update.newBuilder();
        out.getNoteBuilder()
                .setId(id)
                .setTime(System.currentTimeMillis())
                .setNote(note)
                .build();
        out.build().writeDelimitedTo(stream);
    }

    public void finish() throws IOException {
        ProgressNote.Update.Builder out = ProgressNote.Update.newBuilder();
        out.getCompleteBuilder()
                .setId(id)
                .setTime(System.currentTimeMillis())
                .setExitStatus(0)
                .build();
        out.build().writeDelimitedTo(stream);
        stream.close();
        stream = null;
    }

    public void fail(String message, Throwable e) throws IOException {
        StringWriter stack = new StringWriter();
        PrintWriter pw = new PrintWriter(stack);
        e.printStackTrace(pw);
        pw.close();

        ProgressNote.Update.Builder out = ProgressNote.Update.newBuilder();
        out.getCompleteBuilder()
                .setId(id)
                .setTime(System.currentTimeMillis())
                .setExitMessage(message)
                .setStackTrace(stack.toString())
                .setExitStatus(1)
                .build();
        out.build().writeDelimitedTo(stream);

        stream.close();
        stream = null;
    }

    public static Map<String, Double> readTotals(File log) throws IOException {
        FileInputStream input = new FileInputStream(log);

        Map<String, Double> r = Maps.newHashMap();
        ProgressNote.Update update = ProgressNote.Update.parseDelimitedFrom(input);
        while (update != null) {
            if (update.hasProgress()) {
                for (ProgressNote.KeyValue kv : update.getProgress().getCountList()) {
                    Double v = r.get(kv.getKey());
                    if (v == null) {
                        v = 0.0;
                        r.put(kv.getKey(), v);
                    }
                    if (kv.hasIvalue()) {
                        v = (double) kv.getIvalue();
                    } else if (kv.hasIntegerIncrement()) {
                        v += kv.getIntegerIncrement();
                    } else {
                        // missing value = 0
                    }
                    r.put(kv.getKey(), v);
                }
            } else if (update.hasComplete()) {
                completed = true;
                returnStatus = update.getComplete().getExitStatus() == 0;
                backTrace = update.getComplete().getStackTrace();
                message = update.getComplete().getExitMessage();
            }
            update = ProgressNote.Update.parseDelimitedFrom(input);
        }

        return r;
    }

    public boolean finished() {
        return completed;
    }

    public String exitMessage() {
        return message;
    }

    public boolean success() {
        return returnStatus;
    }

    public String getStackTrace() {
        return backTrace;
    }
}
