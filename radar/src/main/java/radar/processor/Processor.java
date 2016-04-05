package radar.processor;

import java.nio.file.Path;

public interface Processor {
    void start();

    void step(Path data);

    void end();

    void signalStepEnd();
}
