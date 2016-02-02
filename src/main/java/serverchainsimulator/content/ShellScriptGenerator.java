package serverchainsimulator.content;

public interface ShellScriptGenerator {
    String generate(String ... args);
    String generateRemoteFileName(String ... args);
}
