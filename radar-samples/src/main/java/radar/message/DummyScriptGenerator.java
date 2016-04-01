package radar.message;

public class DummyScriptGenerator implements ScriptGenerator {
    @Override
    public String generate(String... args) {
        return "";
    }

    @Override
    public String generateRemoteFileName(String... args) {
        return "";
    }
}
