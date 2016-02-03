package messagepipeline.message;

public interface ShellScriptGenerator {
    String generate(String ... args);
    String generateRemoteFileName(String ... args);
}
