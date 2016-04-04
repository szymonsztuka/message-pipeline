package radar;

import radar.conf.CommandParser;
import radar.conf.PropertiesParser;
import radar.message.DecoderFactory;
import radar.message.EncoderFactory;
import radar.message.ScriptFactory;
import radar.node.NodeFactory;
import radar.topology.Sequence;
import radar.topology.SequenceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Radar {

    private static final Logger logger = LoggerFactory.getLogger(Radar.class);

    private final EncoderFactory encoderFactory;
    private final DecoderFactory decoderFactory;
    private final ScriptFactory scriptFactory;
    private final String[] configurationFiles;

    public Radar(EncoderFactory encoderFactory, DecoderFactory decoderFactory, ScriptFactory scriptFactory, String[] configurationFiles ) {
        this.encoderFactory = encoderFactory;
        this.decoderFactory = decoderFactory;
        this.scriptFactory = scriptFactory;
        this.configurationFiles = configurationFiles;
    }

    public void run() {
        PropertiesParser propertiesParser = new PropertiesParser(configurationFiles);
        CommandParser commandParser = new CommandParser(propertiesParser.rawCommand);
        SequenceBuilder sequenceBuilder = new SequenceBuilder(
                new NodeFactory(encoderFactory,
                        decoderFactory,
                        scriptFactory),
                commandParser.command,
                propertiesParser.nodeToProperties);
        for(Sequence seq: sequenceBuilder.sequences) {
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