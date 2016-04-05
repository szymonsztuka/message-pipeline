package radar.message;


public class FileReaderFactory implements ReaderFactory {
    @Override
    public FileReader getReader() {
        return new FileReader(); //TODO
    }
}
