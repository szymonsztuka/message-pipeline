package radar.message;

import java.nio.file.Path;

public interface Reader {
    void open(Path path);
    void close();
    String readMessage();
}
