package messagepipeline.example;

import messagepipeline.MessagePipeline;
import messagepipeline.message.MessageGenerator;
import messagepipeline.message.MessageReceiver;
import messagepipeline.message.ShellScriptGenerator;

public class DummyMessagePipeline extends MessagePipeline {
    @Override
    protected MessageReceiver getMessageReceiver(String type) {
        return new DummyMessageReceiver();
    }

    @Override
    protected MessageGenerator getMessageGenerator(String type) {
        return new DummyMessageGenerator();
    }

    @Override
    protected ShellScriptGenerator getShellScriptGenerator(String... args) {
        return new DummyShellScriptGenerator();
    }

    public static void main(String[] args) {

        PassThroughTcpServer pts1 = new  PassThroughTcpServer("", 5555, 5556);
        PassThroughTcpServer pts2 = new  PassThroughTcpServer("", 5555, 5557);
        Thread obj1 = new Thread(pts1);
        Thread obj2 = new Thread(pts2);
        obj1.start();
        obj2.start();
        DummyMessagePipeline me = new DummyMessagePipeline();
        String[] files = new String[]{"resources/main/example/dummyproperties.txt"};
        me.start2(files);
    }
}
