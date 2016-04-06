package radar.conf;

import java.util.*;
import java.util.stream.Collectors;

public class Command {
    public Set<String> names = new TreeSet<>(); //TODO this contained below
    public Map<String,Map<String,String>> nameToProperties; //TODO parser more
    public List<Command> childCommands = new ArrayList<>(1);

    public Command() {
    }

    public Command(String name) {
        names.add(name);
    }

    public Set<String> getAllNames(){
        Set<String> result = new TreeSet<>();
        for(Command child: childCommands) {
            result.addAll(child.getAllNames());
        }
        result.addAll(names);
        return result;
    }
    public String toString() {
        return toString("", "");
    }

    private String toString(String rootToParentOffset, String parentToMeOffset) {

        List<String> res = childCommands.stream()
                .map(e -> e.toString(rootToParentOffset + parentToMeOffset,
                        "|" + String.format("%" + names.toString().length() + "s", "")))
                .collect(Collectors.toList());
        char[] repeat = new char[parentToMeOffset.length()];
        Arrays.fill(repeat, '-');
        if(repeat.length > 0) {
            repeat[0] = '|';
            repeat[repeat.length - 1] = '>';
        }
        String formattedLayer = "{" + names.toString().substring(1, names.toString().length() - 1) + "}";
        String result = rootToParentOffset + String.valueOf(repeat) + formattedLayer + "\n";
        for(String e: res) {
            result = result + e;
        }
        return result;
    }
}
