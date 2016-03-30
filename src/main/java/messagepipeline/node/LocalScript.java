package messagepipeline.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Path;

public class LocalScript implements Node {

    private static final Logger logger = LoggerFactory.getLogger(LocalScript.class);

    private final String script;

    public LocalScript(String script) {
        this.script = script;
    }

    @Override
    public void start() {
        try {
            if (script != null) {
                String info = "Running " + script;
                final Process process = Runtime.getRuntime().exec(script);
                // exhaust input stream  http://dhruba.name/2012/10/16/java-pitfall-how-to-prevent-runtime-getruntime-exec-from-hanging/
                final BufferedInputStream in = new BufferedInputStream(process.getInputStream());
                final byte[] bytes = new byte[4096];
                while (in.read(bytes) != -1) {
                }// wait for completion
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (int i = 0; i < info.length(); i++) {
                    logger.trace("\b");
                }
                logger.trace("Done    " + script);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void step(Path data) {
    }

    @Override
    public void end() {
    }

    @Override
    public void signalStepEnd() {
    }
}
