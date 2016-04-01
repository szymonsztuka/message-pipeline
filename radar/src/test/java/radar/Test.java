package radar;

import radar.lang.PropertiesParser;

import java.util.*;
import java.util.stream.Collectors;


public class Test {

    public static List<String> getProperties( Map<String, Map<String, String>> properties, Set<String> parentKeys, String childKey){
        return properties.entrySet().stream().filter( e -> parentKeys.contains(e.getKey()))
                .filter( e-> e.getValue().keySet().contains(childKey))
                .map(e-> e.getValue().get(childKey))
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        Map<String, Map<String, String>> properties = new HashMap<>();
        Map<String, String> n1 = new HashMap<>();
        n1.put("a1","11");
        n1.put("a2","12");
        n1.put("a3","11");
        Map<String, String> n2 = new HashMap<>();
        n2.put("b1","111");
        n2.put("b2","112");
        Map<String, String> n3 = new HashMap<>();
        n3.put("c1","1111");
        n3.put("a1","1111");
        properties.put("a",n1);
        properties.put("b",n2);
        properties.put("c",n3);
        Set<String> parentKeys = new HashSet<>();
        parentKeys.add("a");
        parentKeys.add("c");
        System.out.println(getProperties( properties, parentKeys, "a1"));
        //expected 11, 1111
        System.out.println(PropertiesParser.getParentKeyToChildProperty( properties, parentKeys, "a1"));
    }
}
