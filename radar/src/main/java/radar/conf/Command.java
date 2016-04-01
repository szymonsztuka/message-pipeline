package radar.conf;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Command {
    public final List<Command> children = new ArrayList<>();
    public final Set<String> layer = new TreeSet<>();
    public final boolean stateful;

    public Command(String name, boolean stateful) {
        layer.add(name);
        this.stateful = stateful;
    }

    public String toString() {
        return layer.toString() + (children.size() > 0 ? ":" + children.size() : "");
    }

    public String toVerboseString(int x, String pad) {
        String z = "";
        for (int i = 0; i < x * 2; i++) {
            z += " ";
        }
        if (children.size() > 0) {
            int max = 0;
            for (Command g : children) {
                int c = g.width(1);
                if (c > max) max = c;
            }
            pad = "";
            for (int i = 0; i < max - 1; i++) {
                pad += "-";
            }
            pad += "-->|";
        }
        if (layer.contains("(")) {
            z = "";
        } else {
            int ff = pad.length() - layer.toString().length();
            if (stateful) z += "[";
            z += layer.toString().substring(1, layer.toString().length() - 1);
            if (stateful) z += "]";
            z += (ff > 0 ? pad.substring(pad.length() - ff, pad.length()) : (children.size() > 0 ? "-->|" : (x > 1 ? "-->|" : "")));
            z += "\n";
            pad = "";
        }
        for (Command g : children) {
            z += g.toVerboseString(x + 1, pad);
        }
        return z;
    }

    public int width(int level) {
        int best = layer.toString().length() + 2 * level;
        for (Command g : children) {
            int c = g.width(level + 1);
            if (c > best) {
                best = c;
            }
        }
        return best + 2;
    }
}
