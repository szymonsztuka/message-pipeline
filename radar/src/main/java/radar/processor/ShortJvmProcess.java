package radar.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Short living JVM process self terminating
 */
public class ShortJvmProcess implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(ShortJvmProcess.class);

    private final String[] jvmArguments;
    private final String[] programArguments;
    private final String classpath;
    private final String mainClass;
    private final String processLogFile;
    private int stepNb;

    public ShortJvmProcess(String classpath, String[] jvmArguments, String mainClass, String[] programArguments, String processLogFile) {
        this.classpath = classpath;
        this.jvmArguments = jvmArguments;
        this.programArguments = programArguments;
        this.mainClass = mainClass;
        this.processLogFile = processLogFile;
    }

    @Override
    public void start() {
    }

    @Override
    public void step(Path step) {
        stepNb++;
        String [] args;
        if (step != null) {
            args = new String[programArguments.length + 1];
            for (int i = 0; i < programArguments.length; i++) {
                args[i] = programArguments[i];
            }
            args[args.length - 1] = step.toString();
        } else {
            args = programArguments;
        }
        JvmUtil jvm = new JvmUtil(classpath, jvmArguments, mainClass, args, logger);
        try {
            jvm.start(processLogFile + "-" + stepNb + ".log");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void end() {
    }

    @Override
    public void signalStepEnd() {
    }
}