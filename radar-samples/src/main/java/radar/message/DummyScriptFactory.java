package radar.message;

public class DummyScriptFactory implements ScriptFactory {
    @Override
    public Script getScriptGenerator(String... args) {
        return new DummyScript();
    }
}
