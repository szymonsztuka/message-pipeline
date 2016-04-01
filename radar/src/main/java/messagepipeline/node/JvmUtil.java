package messagepipeline.node;

import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class JvmUtil {

    private final Logger logger;

    private final String[] jvmArguments;
    private final String[] programArguments;
    private final String classpath;
    private final String mainClass;
    private Process jvmProcess;

    public JvmUtil(String classpath, String[] jvmArguments, String mainClass, String[] programArguments, Logger logger) {
        this.logger = logger;
        this.classpath = classpath;
        this.jvmArguments = jvmArguments;
        this.programArguments = programArguments;
        this.mainClass = mainClass;
    }

    public void start(String processLogFile) throws IOException {

        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        List<String> allArguments = new ArrayList<>(3);
        allArguments.add(javaBin);
        allArguments.add("-cp");
        allArguments.add(classpath);
        if (null != jvmArguments && jvmArguments.length > 0 && !"".equals(jvmArguments[0])) {
            allArguments.addAll(Arrays.asList(jvmArguments));
        }
        allArguments.add(mainClass);
        if (null != programArguments && programArguments.length > 0) {
            allArguments.addAll(Arrays.asList(programArguments));
        }
        logger.debug(allArguments.toString());
        ProcessBuilder builder = new ProcessBuilder(allArguments);
        if (logger.isTraceEnabled()) {
            builder.command().stream().forEach(logger::trace);
        }
        logger.debug("JVM process dir "  +builder.directory());
        builder.redirectErrorStream(true);
        File output = new File(processLogFile);
        builder.redirectOutput(output);
        jvmProcess = builder.start();
        new Thread(() -> {
            Scanner sc = new Scanner(jvmProcess.getInputStream());
            while (sc.hasNextLine()) {
                //dest.println(sc.nextLine());
                sc.nextLine();
            }
        }).start();
    }

    public int end() {
        if (jvmProcess != null) {
            jvmProcess.destroy();
            try {
                jvmProcess.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
            return jvmProcess.exitValue();
        } else {
            return 0;
        }
    }
}
