package messagepipeline.experimental;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;

import static messagepipeline.MessagePipeline.*;

public class Test {
    public static void main(String[] args) {

        final Map<String, String> rawProperties = mergeProperties( new String[]{"", ""});
        //System.out.println(rawProperties);
        final Map<String, String> variables = getVariables(rawProperties, "path.");
        final Map<String, String> properties = replaceVariables(rawProperties, variables, Arrays.asList(new String[]{"dd-MMM-yy","yyyy-MMM-dd"}));
        //System.out.println(properties);
        final TreeSet<String> nodesToRun = new TreeSet<>(Arrays.asList(properties.get("run").split("->|,|;")));
        final Map<String, String> selectedProperties = filterProperties(properties, nodesToRun);
        final Map<String, Map<String, String>> nodeToProperties = wrapProperties(selectedProperties);
        System.out.println(nodeToProperties);
    }
}
