package radar;

import radar.conf.CommandParser;
import radar.conf.PropertiesParser;
import radar.message.ByteConverterFactory;
import radar.message.StringConverterFactory;
import radar.message.ReaderFactory;
import radar.message.ScriptFactory;
import radar.processor.ProcessorFactory;
import radar.topology.Tap;
import radar.topology.TopologyBuilder;

public class Radar {

    private final ReaderFactory readerFactory;
    private final StringConverterFactory stringConverterFactory;
    private final ByteConverterFactory byteConverterFactory;
    private final ScriptFactory scriptFactory;
    private final String[] configurationFiles;

    public Radar(ReaderFactory readerFactory, StringConverterFactory stringConverterFactory, ByteConverterFactory byteConverterFactory, ScriptFactory scriptFactory, String[] configurationFiles ) {
        this.readerFactory = readerFactory;
        this.stringConverterFactory = stringConverterFactory;
        this.byteConverterFactory = byteConverterFactory;
        this.scriptFactory = scriptFactory;
        this.configurationFiles = configurationFiles;
    }

    public void run() {
        PropertiesParser propertiesParser = new PropertiesParser(configurationFiles);
        CommandParser commandParser = new CommandParser(propertiesParser.rawCommand);
        ProcessorFactory processorFactory = new ProcessorFactory(readerFactory,
                stringConverterFactory,
                byteConverterFactory,
                scriptFactory);
        TopologyBuilder topologyBuilder = new TopologyBuilder(
                processorFactory,
                commandParser.command,
                propertiesParser.nameToProperties);
        for(Tap seq: topologyBuilder.taps) {
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