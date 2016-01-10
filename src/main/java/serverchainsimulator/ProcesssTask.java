package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ProcesssTask implements Runnable, Stopable {

    private static final Logger logger = LoggerFactory.getLogger(ProcesssTask.class);
    final private CyclicBarrier batchStart;
    final private CyclicBarrier batchEnd;
    final private String[] jvmArguments;
    final private String[] programArguments;
    final private String classpath;
    final private String mainClass;
    private volatile boolean process = true;
    final private int turns;

    public ProcesssTask(String classpath, String[] jvmArguments, String mainClass, String[] programArguments, CyclicBarrier batchStart, CyclicBarrier batchEnd, int turns) {
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.classpath = classpath;
        this.jvmArguments = jvmArguments;
        this.programArguments = programArguments;
        this.mainClass = mainClass;
        this.turns = turns;
    }

    public int exec(String classCanonicalName, String classpath, String[] jvmArguments, String[] programArguments, CyclicBarrier batchStart, CyclicBarrier batchEnd) throws IOException,
            InterruptedException {


        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        List<String> allArguments = new ArrayList<>(3);
        allArguments.add(javaBin);
        allArguments.add("-cp");
        allArguments.add(classpath);
        if (null != jvmArguments && jvmArguments.length > 0) {
            allArguments.addAll(Arrays.asList(jvmArguments));
        }
        allArguments.add(classCanonicalName);
        if (null != programArguments && programArguments.length > 0) {
            allArguments.addAll(Arrays.asList(programArguments));
        }
        ProcessBuilder builder = new ProcessBuilder(allArguments);
        if (logger.isTraceEnabled()) {
            for (String elem : builder.command()) {
                logger.trace(elem);
            }
        }
        Process process = builder.inheritIO().start();

        new Thread(() -> {
             {
                Scanner sc = new Scanner(process.getInputStream());
                while (sc.hasNextLine()) {
                    //dest.println(sc.nextLine());
                    sc.nextLine();
                }
            }
        }).start();

        //logger.debug("Bootstrap running.!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        Thread.sleep(1000*20);
        //logger.debug("batchStart.await()");
        try {
            batchStart.await();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        //logger.debug("batchStart.await() -done ");
        //logger.debug("batchEnd.await()!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        try {
            batchEnd.await();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        Thread.sleep(1000);
        logger.debug("Bootstrap shouting down.<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        process.destroy();

        process.waitFor();
        Thread.sleep(1000);
        //logger.debug("Bootstrap shout down.!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        return process.exitValue();
    }

    public void run() {
        //int i=0;
        //while(i < turns) {
        //    i++;
            //long startTime = 0;
            try {
                //logger.debug("start "+ i +"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                //startTime = System.nanoTime();
                int status = exec(mainClass, classpath, jvmArguments, programArguments, batchStart, batchEnd);
                logger.debug("end");
                logger.info("Bootstrap returned status " + status);
                //long endTime = System.nanoTime();
                //long duration = endTime - startTime;
                //double seconds = (duration / 1000000000.0);
            } catch (IOException ex) {
                //long endTime = System.nanoTime();
                //long duration = endTime - startTime;
                //double seconds = (duration / 1000000000.0);
                logger.error(ex.getMessage(), ex);
            } catch (InterruptedException ex) {
                //long endTime = System.nanoTime();
                //long duration = endTime - startTime;
                //double seconds = (duration / 1000000000.0);
                logger.error(ex.getMessage(), ex);
            }
        //}

        //logger.debug("done !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    }

    public void signalBatchEnd() {
        process = false;
        logger.info("process set to " + process);
    }
}