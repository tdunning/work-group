package com.mapr.workGroup;

import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A worker is a process which shares state in a file system in order to coordinate a fixed number
 * of processes each with different roles.  The following roles can be taken on by a worker:
 * <p/>
 * <ul>
 * <li>coordinator - the coordinator designates which roles the other servers will take on</li>
 * <li>worker - a worker does useful work and takes on tasks as assigned by the coordinator</li>
 * <li>drone - a drone is a process that has been designated as unnecessary by the coordinator.  Typically drones
 * will exit immediately upon being designated as drones.  Such an exit is consider a success.</li>
 * </ul>
 * <p/>
 * Which process becomes coordinator is handled by using a lock-file.  The process which creates the lock-file
 * is the coordinator.  All other processes will create a lock-file of their own with a unique name and then
 * will monitor the coordinator's lock-file for assignments.  If the coordinator does not provide an assignment
 * within a short time, these other processes will exit with an error.
 * <p/>
 * Processes designated as workers should create a log file using the ProgressLog class and should update that
 * file relatively frequently (every 30 seconds or so).  If a worker's progress log is not updated for a configurable
 * long time, the entire computation will be considered as having failed and the coordinator will instruct all
 * workers to exit immediately.
 * <p/>
 * As processes exit, they should log successful completion in their log file write a completion flag to their lock-file.
 * The coordinator should be the last process to exit and it should remove its own lock file at that time.  The
 * coordinator should have a separate log file for logging coordination activities.
 * <p/>
 * The state machine for processes is
 * <ul>
 * <li>STARTING - can go to COORDINATOR or WORKER depending on whether the coordinator lock-file can be created</li>
 * <li>COORDINATOR - can go to COORDINATOR_FAIL or COORDINATOR_SUCCESS.  A task taking on this role should monitor
 * the worker lock file directory for new workers.  When a worker is detected, it should be assigned a task or
 * instruct it to become a drone.  The worker log files should also be monitored.  If a log file is not updated
 * for a coordinator.task.timeout.ms milliseconds or a works logs an error exit, then the process should
 * transition to COORDINATOR_FAIL.  Once all configured workers have started, assigned a role and logged a
 * successful completion in their logs, the process should transition to COORDINATOR_SUCCESS</li>
 * <li>WORKER - can go to WORKER_FAIL or WORKER_KILL or WORKER_SUCCESS.  Process should monitor the coordinator
 * lock file for changes.  The coordinator lock file will be empty or will contain a list of process assignments.
 * If a worker sees a change in this file, it should read the file and note the assignment for itself.  This
 * assignment can direct the worker to take on a role of worker or drone or it can tell the process to exit
 * immediately.  If given a drone role, the process should transition immediately to WORKER_SUCCESS.  If told
 * to exit, the process should transition to WORKER_KILL.  If told to be a worker, the process should read
 * its assignment and run that task.  While running the task, the process should write progress notes fairly often
 * and should check for exit instructions as well.  Upon completion of the task, the process can transition to
 * WORKER_SUCCESS or WORKER_FAIL as appropriate.</li>
 * <li>COORDINATOR_SUCCESS - can go to EXIT.  Precondition: all processes have exited.  Log completion, remove
 * all lock files and transition</li>
 * <li>COORDINATOR_FAIL - can go to EXIT.  Precondition: a fatal error has been detected.  Write failure command to
 * coordinator lock file.  Wait until as many worker lock files have been created as configured or until
 * coordinator.exit.wait.ms milliseconds have passed.  Log failure with notes about whether processes have all run.
 * remove all lock files and transition.</li>
 * <li>WORKER_SUCCESS - can go to EXIT.  Write success to log file.  Transition.</li>
 * <li>WORKER_KILL - can go to EXIT.  Write failure to log file.  Transition.</li>
 * <li>WORKER_FAIL - can go to EXIT.  Write failure to log file.  Transition.</li>
 * <li>EXIT - process exits with 0 exit code.</li>
 * </ul>
 */
public class Worker extends GroupThread {
    private Logger log = LoggerFactory.getLogger(Coordinator.class);

    public static Future start(File baseDirectory) {
        return Executors.newSingleThreadScheduledExecutor().submit(new Worker(baseDirectory));
    }

    public Worker(File baseDirectory) {
        super(baseDirectory);
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public Object call() throws Exception {
        setState(log, ThreadState.WORKER);

        final BlockingQueue<ClusterState.Assignment> toDo = getAssignments();

        // creating this lock-file should trigger the coordinator to tell us what to do
        Path logFile = new File(new File(getBaseDirectory(), Constants.getWorkerLockDir()), getId().toStringUtf8()).toPath();
        Files.write(logFile, new byte[0], StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        MessageOutput<ProgressNote.Update> out = new MessageOutput<ProgressNote.Update>(logFile) {
            @Override
            public void writeDelimitedTo(OutputStream stream, ProgressNote.Update v) throws IOException {
                v.writeDelimitedTo(stream);
            }
        };

        final File updateLogFile = null;  // TODO make this real

        ClusterState.Assignment assignment = toDo.poll(Constants.getWorkerStartupTimeout(), TimeUnit.MILLISECONDS);
        ChildProcess child = null;
        loop:
        while (assignment != null) {
            switch (assignment.getType()) {
                case DRONE:
                    // TODO kill our child here if still running
                    // TODO record success
                    setState(log, ThreadState.WORKER_SUCCESS);
                    break loop;

                case WORKER_EXIT:
                case ALL_EXIT:
                    // TODO kill our child here if still running
                    // TODO record success
                    setState(log, ThreadState.WORKER_KILL);
                    break loop;

                case WORKER_TIMEOUT:
                    // TODO kill our child here if still running
                    // TODO record fail
                    setState(log, ThreadState.WORKER_FAIL);
                    break loop;

                case WORKER:
                    // TODO start child cooking here
                    child = ChildProcess.builder(assignment.getCommandList()).run();
                    break;

                default:
                    throw new RuntimeException("Can't happen, got " + assignment.toString());
            }
            assignment = toDo.poll(100, TimeUnit.MILLISECONDS);

            if (child != null) {
                Integer r = child.waitFor(10, TimeUnit.MILLISECONDS);
                if (r != null) {
                    // child finished
                    if (r==0) {
                        setState(log, ThreadState.WORKER_SUCCESS);
                        ProgressNote.Update.Builder note = ProgressNote.Update.newBuilder();
                        note.getCompleteBuilder().setExitStatus(r).setId(getId().toStringUtf8()).build();
                        out.write(note.build());
                    } else {
                        setState(log, ThreadState.WORKER_FAIL);
                        ProgressNote.Update.Builder note = ProgressNote.Update.newBuilder();
                        note.getCompleteBuilder()
                                .setExitStatus(r)
                                .setId(getId().toStringUtf8())
                                .setStackTrace(child.errorOutput())
                                .build();
                        out.write(note.build());
                    }
                    getWatcher().close();
                    return r;
                }
            }
        }


        // now lay about looking for status updates from the process log
        // ultimately we should receive a completion flag or time out waiting
        ProgressNote.Update.Builder timeoutUpdate = ProgressNote.Update.newBuilder();
        timeoutUpdate.getCompleteBuilder().setExitStatus(1).setExitMessage("Process timed out").build();
//        final BlockingQueue<ProgressNote.Update> updateQueue = MessageInput.getMessages(getWatcher(), updateLogFile,
//                timeoutUpdate.build(), Constants.getWorkerProgressTimeout());

//        ProgressNote.Update update = updateQueue.poll(Constants.getWorkerProgressTimeout(), TimeUnit.MILLISECONDS);
        return null;
    }

    private BlockingQueue<ProgressNote.Update> getUpdates(final File updateLogFile) throws IOException {
        final BlockingQueue<ProgressNote.Update> updateQueue = Queues.newLinkedBlockingQueue();
        getWatcher().watch(updateLogFile, new Watcher.Watch() {
            private long updateOffset = 0;
            private FileChannel updateLockChannel;
            private InputStream updateLockStream;

            {
                updateLockChannel = FileChannel.open(
                        updateLogFile.toPath(),
                        StandardOpenOption.READ);
                updateLockStream = Channels.newInputStream(updateLockChannel);
            }

            @Override
            public void changeNotify(Watcher watcher, File f) {
                try {
                    updateLockChannel.position(updateOffset);
                    ProgressNote.Update update = ProgressNote.Update.parseDelimitedFrom(updateLockStream);
                    updateOffset = updateLockChannel.position();
                    updateQueue.add(update);
                } catch (EOFException e) {
                    // ignore EOF ... more data will come soon
                } catch (IOException e) {
                    throw new RuntimeException("Can't read update log", e);
                }
            }

            @Override
            public void timeoutNotify(Watcher watcher, File f) {
                throw new UnsupportedOperationException("Default operation");
            }
        }, Constants.getWorkerProgressTimeout());
        return updateQueue;
    }

    /**
     * Monitors the coordinators output file looking for assignments for us.
     * @return
     * @throws IOException
     */
    private BlockingQueue<ClusterState.Assignment> getAssignments() throws IOException {
        final BlockingQueue<ClusterState.Assignment> toDo = Queues.newLinkedBlockingQueue();
        final File coordinatorLockFile = new File(getBaseDirectory(), Constants.getCoordinatorLock());

        // start scanning for directions.  This watcher filters the messages that come through
        // so that only the important ones come through the assignment queue
        getWatcher().watch(coordinatorLockFile, new Watcher.Watch() {
            boolean assigned = false;
            private long coordinatorOffset = 0;
            private MessageInput<ClusterState.Assignment> coordinatorLockStream;

            {
                Path path = new File(getBaseDirectory(), Constants.getCoordinatorLock()).toPath();
                coordinatorLockStream = new MessageInput<ClusterState.Assignment>(path) {
                    @Override
                    public ClusterState.Assignment parse(InputStream in) throws IOException {
                        return ClusterState.Assignment.parseDelimitedFrom(in);
                    }
                };
            }

            @Override
            public void changeNotify(Watcher watcher, File f) {
                try {
                    ClusterState.Assignment assignment = coordinatorLockStream.read();
                    switch (assignment.getType()) {
                        case WORKER_EXIT:
                        case DRONE:
                            if (assignment.getId().equals(getId().toStringUtf8())) {
                                toDo.add(assignment);
                            }
                            break;

                        case ALL_EXIT:
                            toDo.add(assignment);
                            break;

                        case WORKER:
                            if (assignment.getId().equals(getId().toStringUtf8())) {
                                toDo.add(assignment);
                                assigned = true;
                            }
                            break;

                        default:
                            // TODO shut everything down somehow
                            throw new RuntimeException("Can't happen, got " + assignment.toString());
                    }
                } catch (EOFException e1) {
                    // message not ready yet
                } catch (IOException e) {
                    throw new RuntimeException("Error reading coordinator lock file", e);
                }
            }

            @Override
            public void timeoutNotify(Watcher watcher, File f) {
                if (!assigned) {
                    toDo.add(ClusterState.Assignment.newBuilder()
                            .setType(ClusterState.WorkerType.WORKER_TIMEOUT)
                            .build());
                }
            }
        }, Constants.getWorkerStartupTimeout());
        return toDo;
    }
}
