package messagepipeline.experimental;

import messagepipeline.pipeline.topology.Layer;
import messagepipeline.pipeline.topology.LeafLayer;

import java.util.*;

public class Node {
    public final List<Node> children = new ArrayList<>();
    public final Set<String> layer = new TreeSet();
    public final boolean staetfull;
    public Node(String name, boolean statefull) {
        layer.add(name);
        this.staetfull = statefull;
    }
    public String toString() {
        return layer.toString() + (children.size()>0?":"+children.size():"");
    }
    public String toVerboseString(int x, String padd, boolean nested){
        String z = "";
        for (int i = 0; i < x * 2; i++) {
            z += " ";
        }
        if (children.size() > 0) {
            int max = 0;
            for (Node g: children) {
                int c =  g.width(1);
                if (c  > max) max = c;
            }
            padd = "";
            for (int i = 0; i < max - 1; i++) {
                padd += "-";
            }
            padd += "-->|";
        }
        if (layer.contains("(")) {
            z = "";
        } else {
            int ff = padd.length() - layer.toString().length();
            if(staetfull) z+= "[";
            z += layer.toString().substring(1,layer.toString().length()-1);
            if(staetfull) z+= "]";
            z += (ff>0 ? padd.substring(padd.length()-ff, padd.length()):(children.size()>0?"-->|":(x>1?"-->|":"")));
            z += "\n";
            padd = "";
        }
        for (Node g: children) {
            z += g.toVerboseString(x+1, padd, true);
        }
        return z;
    }

    public int width(int level){
        int best = layer.toString().length() + 2 * level;
        for (Node g: children) {
            int c = g.width(level + 1);
            if (c > best) {
                best = c;
            }
        }
        return best+2;
    }


}
