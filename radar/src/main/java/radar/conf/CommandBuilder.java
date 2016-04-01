package radar.conf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CommandBuilder {

    public final Command command;

    public CommandBuilder(String compoundStep){
        command = new Command("run", false);
        parse(command, false, false, false, tokenize(compoundStep).iterator());
    }
    public static List<String> tokenize(String rawCommand) {

        List<String> tokens = new ArrayList<>();
        int start = 0;
        int end = 0;
        int i = 0;
        while (i < rawCommand.length()) {
            switch (rawCommand.charAt(i)) {
                case ';':
                    if (start != end) {
                        tokens.add(rawCommand.substring(start, end));
                    }
                    tokens.add(";");
                    start = i + 1;
                    end = i + 1;
                    break;
                case ',':
                    if (start != end) {
                        tokens.add(rawCommand.substring(start, end));
                    }
                    tokens.add(",");
                    start = i + 1;
                    end = i + 1;
                    break;
                case '(':
                    if (start != end) {
                        tokens.add(rawCommand.substring(start, end));
                    }
                    tokens.add("(");
                    start = i + 1;
                    end = i + 1;
                    break;
                case ')':
                    if (start != end) {
                        tokens.add(rawCommand.substring(start, end));
                    }
                    tokens.add(")");
                    start = i + 1;
                    end = i + 1;
                    break;
                case '[':
                    if (start != end) {
                        tokens.add(rawCommand.substring(start, end));
                    }
                    tokens.add("[");
                    start = i + 1;
                    end = i + 1;
                    break;
                case ']':
                    if (start != end) {
                        tokens.add(rawCommand.substring(start, end));
                    }
                    tokens.add("]");
                    start = i + 1;
                    end = i + 1;
                    break;
                default:
                    end++;
            }
            i++;
        }
        if (start != end) {
            tokens.add(rawCommand.substring(start, end));
        }
        return tokens;
    }

    public static void parse(Command parent, boolean set, boolean scoped, boolean stateful, Iterator<String> tokens) {
        while (tokens.hasNext()) {
            String token = tokens.next();
            if (";".equals(token)) {
                set = false;
                scoped = false;
                stateful = false;
            } else if (",".equals(token)) {
                set = true;
            } else if ("(".equals(token)) {
                scoped = true;
            } else if (")".equals(token)) {
                return;
            } else if ("[".equals(token)) {
                stateful = true;
            } else if ("]".equals(token)) {
                stateful = false;
            } else {
                if (!scoped && !set) {
                    parent.children.add(new Command(token, stateful));
                } else if (scoped) {
                    Command nested = new Command(token, stateful);
                    parent.children.add(nested);
                    parse(nested, false, false, false, tokens);
                } else if (!scoped && set) {
                    parent.layer.add(token);
                }
            }
        }
    }

    /**
     * Test
     */
    public static void main(String[] args) {

        List<String> command = new ArrayList();
        String[] x = new String[]{"p1", ",", "p2", ",", "(", "(", "cim1", ",", "cim2", ",", "(", "fep", ")", ")", ",", "s", ")", ",", "k1", ",", "k2"};
        command.addAll(Arrays.asList(x));
        Command z = new Command("Parent", false);
        System.out.println(z);
        String raw = "p1;p2;p3;(pa1,pa2,(m1,m2,(f1)));k1;k2;r1;r2";
        System.out.println(tokenize(raw));

        Command z2 = new Command("run", false);
        parse(z2, false, false, false, tokenize(raw).iterator());
        System.out.println(z2.toVerboseString(0, ""));

        z2 = new Command("run", false);
        raw = "p1;[p2];p3;([pa,p2],(m1,im2,(f)));k1;k2;re1;re2";
        parse(z2, false, false, false, tokenize(raw).iterator());
        System.out.println(z2.toVerboseString(0, ""));

        z2 = new Command("run", false);
        raw = "(c1,c2,(b1,b2,(a)),d)";
        parse(z2, false, false, false, tokenize(raw).iterator());
        System.out.println(z2.toVerboseString(0, ""));

        z2 = new Command("run", false);
        raw = "(c1,c2,(b1,b2,(a)),(d))";
        parse(z2, false, false, false, tokenize(raw).iterator());
        System.out.println(z2.toVerboseString(0, ""));
    }
}
