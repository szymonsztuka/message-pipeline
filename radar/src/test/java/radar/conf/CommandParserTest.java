package radar.conf;

public class CommandParserTest {


    public static void main(String [] a){

        CommandParser x = new CommandParser("({c1,c2}({b1,b2}(a))(d))");
        System.out.println(x.tokens);
        System.out.println(x.command);

        CommandParser one = new CommandParser("(a)");
        System.out.println("Tokens: " + one.tokens);
        System.out.println(one.command);
    }
}
