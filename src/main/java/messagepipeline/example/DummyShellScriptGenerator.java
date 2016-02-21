package messagepipeline.example;

import messagepipeline.message.ShellScriptGenerator;

public class DummyShellScriptGenerator implements ShellScriptGenerator {
    @Override
    public String generate(String... args) {
        return "";
    }

    @Override
    public String generateRemoteFileName(String... args) {
        return "";
    }
}
