package radar.message;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileReader implements Reader {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileReader.class);
    private BufferedReader reader;

    @Override
    public void open(Path path) {
        BufferedReader temp = null;
        try {
            temp = Files.newBufferedReader(path, Charset.forName("UTF-8"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        reader = temp;
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public String readMessage() {
        if (reader != null) {
            try {
                return reader.readLine();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            logger.error("Reader is null");
        }
        return null;
    }
}
