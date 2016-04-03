package radar.conf;

import java.util.*;
import java.util.stream.Collectors;

public class Command {
    public Set<String> layer = new TreeSet<>();
    public List<Command> children = new ArrayList<>(1);

    public Command() {
    }

    public Command(String name) {
        layer.add(name);
    }

    public String toString() {
        return toString("", "");
    }

    private String toString(String rootToParentOffset, String parentToMeOffset) {
        List<String> res = children.stream()
                .map(e -> e.toString(rootToParentOffset + parentToMeOffset,
                        "|" + String.format("%" + layer.toString().length() + "s", "")))
                .collect(Collectors.toList());

        char[] repeat = new char[parentToMeOffset.length()];
        Arrays.fill(repeat, '-');
        if (repeat.length > 0) {
            repeat[0] = '|';
            repeat[repeat.length - 1] = '>';
        }
        String formattedLayer = "{" + layer.toString().substring(1, layer.toString().length() - 1) + "}";
        String result = rootToParentOffset + String.valueOf(repeat) + formattedLayer + "\n";
        for (String e : res) {
            result = result + e;
        }
        return result;
    }
}
