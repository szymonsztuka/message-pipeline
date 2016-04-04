package radar.message;

public interface ScriptFactory {

    Script getScriptGenerator(String... args);
}
