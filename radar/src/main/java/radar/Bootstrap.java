package radar;

import radar.conf.CommandParser;
import radar.conf.PropertiesParser;
import radar.message.DecoderFactory;
import radar.message.EncoderFactory;
import radar.message.ScriptFactory;
import radar.node.NodeFactory;
import radar.topology.Sequence;
import radar.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private final EncoderFactory encoderFactory;
    private final DecoderFactory decoderFactory;
    private final ScriptFactory scriptFactory;

    public Bootstrap(EncoderFactory encoderFactory, DecoderFactory decoderFactory, ScriptFactory scriptFactory) {
        this.encoderFactory = encoderFactory;
        this.decoderFactory = decoderFactory;
        this.scriptFactory = scriptFactory;
    }

    public void run(String[] args) {

        PropertiesParser propertiesParser = new PropertiesParser(args);
        CommandParser commandParser = new CommandParser(propertiesParser.rawCommand);
        TopologyBuilder topologyBuilder = new TopologyBuilder(
                new NodeFactory(encoderFactory, decoderFactory, scriptFactory),
                commandParser.command,
                propertiesParser.nodeToProperties
                );
        for(Sequence seq: topologyBuilder.sequences) {
            Thread th = new Thread(seq);
            th.start();
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}