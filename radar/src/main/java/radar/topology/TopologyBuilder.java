package radar.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radar.conf.Command;
import radar.conf.PropertiesParser;
import radar.processor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

public class TopologyBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TopologyBuilder.class);

    private final ProcessorFactory processorFactory;

    public final List<Tap> taps = new ArrayList<>(1);

    public TopologyBuilder(ProcessorFactory processorFactory, Command command, Map<String, Map<String, String>> nodeToProperties) {

        this.processorFactory = processorFactory;
        for (Command child: command.childCommands) {
            Map<String, String> dataStreamPath = PropertiesParser.getParentKeyToChildProperty(nodeToProperties, child.getAllNames(), "input");
            if(dataStreamPath.isEmpty()) {
                logger.info(String.format("Creating tap with default once off step (no property '.input') with pipeline:\n%s", child));
            } else {
                logger.info(String.format("Creating tap with steps sources %s with pipeline:\n%s", dataStreamPath, child));
            }
             taps.add(new Tap(dataStreamPath, createPipeline(child, nodeToProperties)));
        }
    }

    /** recursive call*/
    private Pipeline createPipeline(Command command, Map<String, Map<String, String>> allCommands) {

        List<Pipeline> childPipelines = command.childCommands.stream()
                .map(e -> createPipeline(e, allCommands))
                .collect(Collectors.toList()); //create child pipelines first

        Map<String, Map<String, String>> sequenceCommand = allCommands.entrySet().stream()
                .filter(a -> command.names.contains(a.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        CyclicBarrier startBarrier = new CyclicBarrier(sequenceCommand.size() + 1);
        CyclicBarrier stopBarrier = new CyclicBarrier(sequenceCommand.size() + 1);
        List<Node> nodes = new ArrayList<>(sequenceCommand.size());
        for (Map.Entry<String, Map<String, String>> entry : sequenceCommand.entrySet()) {
            Processor processor = processorFactory.createNode(entry);
            if (processor != null) { //TODO nodes number should equal command.size() otherwise Pipeline will block on a barrier
                int stepEndDelay = 0;
                if(entry.getValue().containsKey("stepEndDelay")) {
                    try {
                        stepEndDelay = Integer.parseInt(entry.getValue().get("stepEndDelay"));
                    } catch (NumberFormatException e) {
                        logger.warn(e.getMessage());
                    }
                }
                nodes.add(new Node(entry.getKey(), processor, startBarrier, stopBarrier, stepEndDelay));
            }
        }
        return new Pipeline(startBarrier, stopBarrier, nodes, childPipelines);
    }
}
