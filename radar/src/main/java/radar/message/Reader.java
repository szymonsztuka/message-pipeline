package radar.message;

import java.nio.file.Path;

public interface Reader<T> {
    void open(Path path);
    void close();
    T readMessage();
}
