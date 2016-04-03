package radar;

import radar.conf.CommandParser;
import radar.conf.PropertiesParser;
import radar.message.CodecFactoryMethod;
import radar.topology.Sequence;
import radar.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private final CodecFactoryMethod codecFactoryMethod;

    public Bootstrap(CodecFactoryMethod codecFactoryMethod) {
        this.codecFactoryMethod = codecFactoryMethod;
    }

    public void run(String[] args) {

        PropertiesParser propertiesParser = new PropertiesParser(args);
        CommandParser commandParser = new CommandParser(propertiesParser.rawCommand);
        TopologyBuilder topologyBuilder = new TopologyBuilder(commandParser.command,
                propertiesParser.nodeToProperties,
                codecFactoryMethod);
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