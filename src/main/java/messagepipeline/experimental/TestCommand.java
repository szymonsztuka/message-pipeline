package messagepipeline.experimental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class TestCommand {

    public static void main(String[] args) {


        List<String> command = new ArrayList();
        String[] x = new String[]{"p1",",","p2",",","(","(","cim1",",","cim2",",","(","fep",")",")",",","s",")",",","k1",",","k2"};
        command.addAll(Arrays.asList(x));
        Iterator<String> rawCommand = command.iterator();
        Node z = new Node("Parent",false);
//        parse(z,false, rawCommand);
        System.out.println(z);
        //String raw = "(p1,p2),((cim1,cim2,(fep)),(s)),(k1,k2)";
        String raw = "p1;p2;p3;(pa1,pa2,(m1,m2,(f1)));k1;k2;r1;r2";
        System.out.println(tokenize(raw));


        Node z2 = new Node("run",false);
        parse(z2,false, false,false, tokenize(raw).iterator());
        System.out.println(z2.toVerboseString(0,"",false));

        z2 = new Node("run",false);
        raw = "p1;[p2];p3;([pa,p2],(m1,im2,(f)));k1;k2;re1;re2";
        parse(z2,false, false,false, tokenize(raw).iterator());
        System.out.println(z2.toVerboseString(0,"",false));

    }

    public static List<String> tokenize(String rawCommand){

        List<String> tokenized = new ArrayList<>();
        int start = 0;
        int end = 0;
        int i = 0;
        while(i < rawCommand.length()) {
            switch (rawCommand.charAt(i)) {
                case ';' :  if (start != end) {
                                tokenized.add(rawCommand.substring(start, end));
                            }
                            tokenized.add(";");
                            start = i+1;
                            end = i+1;
                            break ;
                case ',' :  if (start != end) {
                                tokenized.add(rawCommand.substring(start,end));
                            }
                            tokenized.add(",");
                            start = i+1;
                            end = i+1;
                            break ;
                case '(' :  if (start != end) {
                                tokenized.add(rawCommand.substring(start,end));
                            }
                            tokenized.add("(");
                            start = i+1;
                            end = i+1;
                            break ;
                case ')' :  if (start != end) {
                                tokenized.add(rawCommand.substring(start,end));
                            }
                            tokenized.add(")");
                            start = i+1;
                            end = i+1;
                            break ;
                case '[' :  if (start != end) {
                                tokenized.add(rawCommand.substring(start,end));
                            }
                            tokenized.add("[");
                            start = i+1;
                            end = i+1;
                            break ;
                case ']' :  if (start != end) {
                             tokenized.add(rawCommand.substring(start,end));
                            }
                            tokenized.add("]");
                            start = i+1;
                            end = i+1;
                            break ;
                default :  end ++ ;
            }
            i ++;
        }
        if (start!=end) {
            tokenized.add(rawCommand.substring(start, end));
        }
        return tokenized;
    }

    public static void parse(Node parent, boolean set, boolean scoped, boolean staefull, Iterator<String> rawCommand) {
        while(rawCommand.hasNext()) {
            String x = rawCommand.next();
            if (";".equals(x)) {
                set = false;
                scoped = false;
                staefull = false;
            } else if(",".equals(x)) {
                set = true;
            }else if("(".equals(x)) {
                scoped = true;
            } else if(")".equals(x)) {
                return;
            } else if("[".equals(x)) {
                staefull = true;
            } else if("]".equals(x)) {
                staefull = false;
            } else {
                if (!scoped && !set) {
                    parent.children.add(new Node(x, staefull));
                } else if (scoped) {
                    Node nested = new Node(x, staefull);
                    parent.children.add(nested);
                    parse(nested, false, false, false, rawCommand);
                } else if(!scoped && set) {
                    parent.layer.add(x);
                }
            }
        }
    }


}
