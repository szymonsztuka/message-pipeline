package messagepipeline.topology;

import messagepipeline.lang.Command;
import messagepipeline.message.CodecFactoryMethod;
import messagepipeline.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class TopologyBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TopologyBuilder.class);

    public final Sequence sequence;

    public TopologyBuilder(Command command, Map<String, Map<String, String>> nodeToProperties, List<String> fileNames, CodecFactoryMethod codecFactoryMethod) {
        sequence = parseSteps(command.children.get(0), nodeToProperties, fileNames, codecFactoryMethod);//TODO commandBuilder.lang.children.get(0)
    }
    public static Sequence parseSteps(Command command, Map<String, Map<String, String>> allCommands, List<String> names, CodecFactoryMethod codecFactoryMethod) {

        Map<String, Map<String, String>> sequenceCommand = allCommands.entrySet().stream()
                .filter(a -> command.layer.contains(a.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        final List<Sequence> childSequences;
        if (command.children.size() == 0) {  //toto wrap into collector
            childSequences = Collections.emptyList();
        } else {
            childSequences = new ArrayList<>(1);
            for (Command e : command.children) {
                childSequences.add(parseSteps(e, allCommands, names, codecFactoryMethod));
            }
        }
        logger.info("create sequence " + sequenceCommand.keySet() + " " + childSequences.size());
        return createSequence(sequenceCommand, names, childSequences, codecFactoryMethod);
    }

    public static Sequence createSequence(Map<String, Map<String, String>> command, List<String> names, List<Sequence> childSequences, CodecFactoryMethod codecFactoryMethodt) {

        CyclicBarrier startBarrier = new CyclicBarrier(command.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(command.size() + 1);
        List<Runner> runners = new ArrayList<>(command.size());
        for (Map.Entry<String, Map<String, String>> e : command.entrySet()) {
            Node node = createNode(e, codecFactoryMethodt);
            if (node != null) {
                Runner runner = new Runner(
                        e.getKey(),
                        node,
                        startBarrier,
                        stopBarrier);
                runners.add(runner);
            }
        }
        if (names.size() == 0) {
            names = new ArrayList(1);
            names.add("1"); //TODO once off sequence
        }
        Sequence layer = new Sequence(command.keySet().toString(), names, startBarrier, stopBarrier, runners, childSequences);
        return layer;
    }

    public static Node createNode(Map.Entry<String, Map<String, String>> e, CodecFactoryMethod codecFactoryMethod) {

        if ("sender".equals(e.getValue().get("type"))) {
            int clientsNumber;
            if (Integer.parseInt(e.getValue().get("connections.number")) > 0) {
                clientsNumber = Integer.parseInt(e.getValue().get("connections.number"));
            } else {
                clientsNumber = 1;
            }
            List generators = new ArrayList<>(clientsNumber);
            for (int j = 0; j < clientsNumber; j++) {
                generators.add(codecFactoryMethod.getMessageEncoder(e.getValue().get("encoder")));
            }
            return new SocketWriter(Paths.get(e.getValue().get("input")),
                    new InetSocketAddress(e.getValue().get("ip"), Integer.parseInt(e.getValue().get("port"))),
                    generators);
        } else if ("receiver".equals(e.getValue().get("type"))) {
            return new SocketReader(
                    new InetSocketAddress(e.getValue().get("ip"), Integer.parseInt(e.getValue().get("port"))),
                    Paths.get(e.getValue().get("output")),
                    codecFactoryMethod.getMessageDecoder(e.getValue().get("decoder")));
        } else if ("localscript".equals(e.getValue().get("type"))) {
            return new LocalScript(e.getValue().get("script"));
        } else if ("remotescript".equals(e.getValue().get("type"))) {
            return new SshScript(
                    Paths.get(e.getValue().get("output.directory")),
                    e.getValue().get("user"),
                    e.getValue().get("host"),
                    e.getValue().get("password"),
                    codecFactoryMethod.getScriptGenerator(e.getValue().get("lang"))
            );
        } else if ("jvmprocess".equals(e.getValue().get("type"))) {
            return new LongJvmProcess(
                    e.getValue().get("classpath"),
                    e.getValue().get("jvmArguments").split(" "),
                    e.getValue().get("mainClass"),
                    e.getValue().get("programArguments").split(" "),
                    e.getValue().get("processLogFile")
            );
        } else if ("jvmshortprocess".equals(e.getValue().get("type"))) {
            return new ShortJvmProcess(
                    e.getValue().get("classpath"),
                    e.getValue().get("jvmArguments").split(" "),
                    e.getValue().get("mainClass"),
                    e.getValue().get("programArguments").split(" "),
                    e.getValue().get("processLogFile")
            );
        } else {
            return null;
        }
    }
}
