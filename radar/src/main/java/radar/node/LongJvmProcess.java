package radar.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/** Long running JVM process which doesn't terminate itself */
public class LongJvmProcess implements Node {

    private static final Logger logger = LoggerFactory.getLogger(LongJvmProcess.class);

    private final String processLogFile;
    private final JvmUtil jvm;

    public LongJvmProcess(String classpath, String[] jvmArguments, String mainClass, String[] programArguments, String processLogFile) {
        this.processLogFile = processLogFile;
        this.jvm = new JvmUtil(classpath, jvmArguments, mainClass, programArguments, logger);
    }

    @Override
    public void start() {
        try {
            jvm.start(processLogFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void step(Path data) {
    }

    @Override
    public void signalStepEnd() {
    }

    @Override
    public void end() {
        jvm.end();
    }
}