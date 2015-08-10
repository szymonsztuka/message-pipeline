package serverchainsimulator;

/**
 * Created by simon on 10/08/15.
 */
public class JvmInstanceConfiguration {
    public final String classpath;

    public JvmInstanceConfiguration(String classpath, String jvmArguments, String mainClass, String programArguments) {
        this.classpath = classpath;
        this.jvmArguments = jvmArguments;
        this.mainClass = mainClass;
        this.programArguments = programArguments;
    }

    public final String jvmArguments;
    public final String mainClass;
    public final String programArguments;
}
