package radar.conf;

import java.util.*;

public class CommandParser {

    public final String input;
    public final List<String> tokens;
    public final Command command;

    public CommandParser(String input) {
        this.input = input;
        tokens = tokenize(input);
        command = new Command("root");
        parse(tokens.iterator(), command);
    }

    private void parse(Iterator<String> tokens, Command parent) {
        while (tokens.hasNext()) {
            String token = tokens.next();
            switch (token) {
                case "(": //start of new child
                    Command child = new Command();
                    parent.children.add(child);
                    parse(tokens, child);
                    break;
                case ")": //end of child
                    return;
                case "{":
                    while (tokens.hasNext()) {
                        token = tokens.next();
                        if (token.equals("}")) {
                            break;
                        } else if (!token.equals(",")) {
                            parent.layer.add(token);
                        }
                    }
                    break;
                default: //me
                    parent.layer.add(token);
            }
        }
    }

    private List<String> tokenize(String rawCommand) {

        Set<Character> delimiters = new TreeSet<>();
        delimiters.add('(');
        delimiters.add(')');
        delimiters.add('{');
        delimiters.add('}');
        delimiters.add(',');

        List<String> tokens = new ArrayList<>();
        int start = 0;
        int end = 0;
        int i = 0;
        while (i < rawCommand.length()) {
            if (delimiters.contains(rawCommand.charAt(i))) {
                if (start != end) {
                    tokens.add(rawCommand.substring(start, end));
                }
                tokens.add(String.valueOf(rawCommand.charAt(i)));
                start = i + 1;
                end = i + 1;
            } else {
                end++;
            }
            i++;
        }
        if (start != end) {
            tokens.add(rawCommand.substring(start, end));
        }
        return tokens;
    }
}
