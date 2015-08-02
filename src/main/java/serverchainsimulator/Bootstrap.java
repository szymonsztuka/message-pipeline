package serverchainsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Bootstrap implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    final private CountDownLatch done;
    final private String[] jvmArguments;
    final private String[] programArguments;
    final private String classpath;
    final private String mainClass;

    public Bootstrap(CountDownLatch latch, String classpath, String[] jvmArguments, String mainClass, String[] programArguments) {
        done = latch;
        this.classpath = classpath;
        this.jvmArguments = jvmArguments;
        this.programArguments = programArguments;
        this.mainClass = mainClass;
    }

    public int exec(String classCanonicalName, String classpath, String[] jvmArguments, String[] programArguments, CountDownLatch done) throws IOException,
            InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        List<String> allArguments = new ArrayList<String>(3);
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

        logger.info("Bootstrap running.");
        done.await();
        Thread.sleep(1000 * 2);
        logger.info("Bootstrap shouting down.");

        process.destroy();

        process.waitFor();
        logger.info("Bootstrap shout down.");
        return process.exitValue();
    }

    public void run() {
        //long startTime = 0;
        try {
            //startTime = System.nanoTime();
            int status = exec(mainClass, classpath, jvmArguments, programArguments, done);
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
    }
}
