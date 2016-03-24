package messagepipeline.pipeline.node;

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

public class UniversalJvmProcessRunner extends UniversalNode implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(UniversalJvmProcessRunner.class);
    final private String[] jvmArguments;
    final private String[] programArguments;
    final private String classpath;
    final private String mainClass;
    //private volatile boolean process = true;
    private String processLogFile;
    Process jvmProcess;
    int run = 1;

    public UniversalJvmProcessRunner(String name, String classpath, String[] jvmArguments, String mainClass, String[] programArguments, String processLogFile, CyclicBarrier batchStart, CyclicBarrier batchEnd) {
        super(name,".",batchStart, batchEnd);
        this.classpath = classpath;
        this.jvmArguments = jvmArguments;
        this.programArguments = programArguments;
        this.mainClass = mainClass;
        this.processLogFile = processLogFile;
    }

    private void start(String classCanonicalName, String classpath, String[] jvmArguments, String[] programArguments, CyclicBarrier batchStart, CyclicBarrier batchEnd) throws IOException,
            InterruptedException {

        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        List<String> allArguments = new ArrayList<>(3);
        allArguments.add(javaBin);
        allArguments.add("-cp");
        allArguments.add(classpath);
        if (null != jvmArguments && jvmArguments.length > 0 && !"".equals(jvmArguments[0])) {
            allArguments.addAll(Arrays.asList(jvmArguments));
        }
        allArguments.add(classCanonicalName);
        if (null != programArguments && programArguments.length > 0) {
            allArguments.addAll(Arrays.asList(programArguments));
        }logger.debug(allArguments.toString());
        ProcessBuilder builder = new ProcessBuilder(allArguments);
        if (logger.isTraceEnabled()) {
            builder.command().stream().forEach(logger::trace);
        }
        builder.redirectErrorStream(true);
        File output = new File(processLogFile+run+".log");
        run ++;
        builder.redirectOutput(output);
        jvmProcess = builder.start();
        new Thread(() -> {
            {
                Scanner sc = new Scanner(jvmProcess.getInputStream());
                    while (sc.hasNextLine()) {
                        //dest.println(sc.nextLine());
                        sc.nextLine();
                    }
                }
            }).start();
    }

    public int stop(){
        jvmProcess.destroy();
        try {
            jvmProcess.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error( e.getMessage(), e);
        }
        return  jvmProcess.exitValue();
    }

    public void run() {
        try {
            start(mainClass, classpath, jvmArguments, programArguments, batchStart, batchEnd);
        } catch (IOException | InterruptedException ex ) {
            logger.error(ex.getMessage(), ex);
            ex.printStackTrace();
        }
        while(process) {

            try {logger.debug("s "+ batchStart.getParties()+" "+batchStart.getNumberWaiting());
                batchStart.await();

            } catch (BrokenBarrierException | InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
            try {logger.debug("e "+ batchEnd.getParties()+" "+batchEnd.getNumberWaiting());
                batchEnd.await();

            } catch (BrokenBarrierException | InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
        }
        logger.info("returned status " + stop());
        stop();
    }

    @Override
    public void signalStepEnd() {
        //process = false;
    }

}