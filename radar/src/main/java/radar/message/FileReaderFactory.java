package radar.message;


public class FileReaderFactory implements ReaderFactory<String> {
    @Override
    public FileReader getReader() {
        return new FileReader(); //TODO
    }
}
