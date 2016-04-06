package radar.processor;

import radar.message.*;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProcessorFactory {

    private final EncoderFactory encoderFactory;
    private final ReaderFactory readerFactory;
    private final DecoderFactory decoderFactory;
    private final ScriptFactory scriptFactory;

    public ProcessorFactory(ReaderFactory readerFactory, EncoderFactory encoderFactory, DecoderFactory decoderFactory, ScriptFactory scriptFactory){
        this.encoderFactory = encoderFactory;
        this.decoderFactory = decoderFactory;
        this.scriptFactory = scriptFactory;
        this.readerFactory = readerFactory;
    }

    public Processor createNode(Map.Entry<String, Map<String, String>> e) {

        if ("sender".equals(e.getValue().get("type"))) {
            int clientsNumber;
            if (Integer.parseInt(e.getValue().get("connections.number")) > 0) {
                clientsNumber = Integer.parseInt(e.getValue().get("connections.number"));
            } else {
                clientsNumber = 1;
            }
            List<Encoder> generators = new ArrayList<>(clientsNumber);
            List<Reader> readers = new ArrayList<>(clientsNumber);
            for (int j = 0; j < clientsNumber; j++) {
                generators.add(encoderFactory.getMessageEncoder(e.getValue().get("encoder")));
                readers.add(readerFactory.getReader());
            }
          return new SocketWriter(Paths.get(e.getValue().get("input")),
                    new InetSocketAddress(e.getValue().get("ip"), Integer.parseInt(e.getValue().get("port"))),
                    readers,
                    generators);
        } else if ("receiver".equals(e.getValue().get("type"))) {
            return new SocketReader(
                    new InetSocketAddress(e.getValue().get("ip"), Integer.parseInt(e.getValue().get("port"))),
                    Paths.get(e.getValue().get("output")),
                    decoderFactory.getMessageDecoder(e.getValue().get("decoder")));
        } else if ("localscript".equals(e.getValue().get("type"))) {
            return new LocalScript(e.getValue().get("script"));
        } else if ("sshscript".equals(e.getValue().get("type"))) {
            return new SshScript(
                    Paths.get(e.getValue().get("output")),
                    e.getValue().get("user"),
                    e.getValue().get("host"),
                    e.getValue().get("password"),
                    e.getValue().get("command"),
                    scriptFactory.getScriptGenerator(e.getValue().get("interpreter")));
        } else if ("sshcopy".equals(e.getValue().get("type"))) {
            return new SshDownload(
                    Paths.get(e.getValue().get("output")),
                    e.getValue().get("user"),
                    e.getValue().get("host"),
                    e.getValue().get("password"),
                    scriptFactory.getScriptGenerator(e.getValue().get("interpreter")));
        } else if ("jvmprocess".equals(e.getValue().get("type"))) {
            return new LongJvmProcess(
                    e.getValue().get("classpath"),
                    e.getValue().get("jvmArguments").split(" "),
                    e.getValue().get("mainClass"),
                    e.getValue().get("programArguments").split(" "),
                    e.getValue().get("processLogFile"));
        } else if ("jvmshortprocess".equals(e.getValue().get("type"))) {
            return new ShortJvmProcess(
                    e.getValue().get("classpath"),
                    e.getValue().get("jvmArguments").split(" "),
                    e.getValue().get("mainClass"),
                    e.getValue().get("programArguments").split(" "),
                    e.getValue().get("processLogFile"));
        } else {
            return null;
        }
    }
}
