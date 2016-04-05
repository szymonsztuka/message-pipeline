package radar;

import radar.conf.CommandParser;
import radar.conf.PropertiesParser;
import radar.message.DecoderFactory;
import radar.message.EncoderFactory;
import radar.message.ReaderFactory;
import radar.message.ScriptFactory;
import radar.node.NodeFactory;
import radar.topology.Sequence;
import radar.topology.SequenceBuilder;

public class Radar<T> {

    private final ReaderFactory<T> readerFactory;
    private final EncoderFactory<T> encoderFactory;
    private final DecoderFactory decoderFactory;
    private final ScriptFactory scriptFactory;
    private final String[] configurationFiles;

    public Radar(ReaderFactory<T> readerFactory, EncoderFactory<T> encoderFactory, DecoderFactory decoderFactory, ScriptFactory scriptFactory, String[] configurationFiles ) {
        this.readerFactory = readerFactory;
        this.encoderFactory = encoderFactory;
        this.decoderFactory = decoderFactory;
        this.scriptFactory = scriptFactory;
        this.configurationFiles = configurationFiles;
    }

    public void run() {
        PropertiesParser propertiesParser = new PropertiesParser(configurationFiles);
        CommandParser commandParser = new CommandParser(propertiesParser.rawCommand);
        NodeFactory<T> nodeFactory = new NodeFactory<>(readerFactory,
                encoderFactory,
                decoderFactory,
                scriptFactory);
        SequenceBuilder sequenceBuilder = new SequenceBuilder(
                nodeFactory, //TODO generic type is lost :|
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