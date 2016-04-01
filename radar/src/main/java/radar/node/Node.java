package radar.node;

import java.nio.file.Path;

public interface Node {
    void start();

    void step(Path data);

    void end();

    void signalStepEnd();
}
