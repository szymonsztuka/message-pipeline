package radar.message;

public interface ReaderFactory<T> {
    Reader<T> getReader();
}
