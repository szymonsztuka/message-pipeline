package radar.message;

public interface ScriptGenerator {
    String generate(String ... args);
    String generateRemoteFileName(String ... args);
}
