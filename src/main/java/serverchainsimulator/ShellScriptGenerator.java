package serverchainsimulator;

/**
 * Created by ssztuka on 07/12/2015.
 */
public interface ShellScriptGenerator {
    public String generate(String ... args);
    public String generateRemoteFileName(String ... args);
}
