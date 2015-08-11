package serverchainsimulator;

/**
 * Created by simon on 10/08/15.
 */
public class JvmInstanceConfiguration {
    public final String classpath;
    public final String[] jvmArguments;
    public final String mainClass;
    public final String[] programArguments;
    public boolean restart = false;
    
    public JvmInstanceConfiguration(String classpath, String jvmArguments, String mainClass, String programArguments) {
        this.classpath = classpath;
        this.jvmArguments = jvmArguments != null ? jvmArguments.split(" ") : null; 
        this.mainClass = mainClass;
        this.programArguments = programArguments != null ? programArguments.split(" ") : null; 
        this.restart = false;
    }

    public JvmInstanceConfiguration(String classpath, String jvmArguments, String mainClass, String programArguments, String restart) {
    	this(classpath, jvmArguments, mainClass, programArguments);
    	this.restart = Boolean.parseBoolean(restart);
    }
}
