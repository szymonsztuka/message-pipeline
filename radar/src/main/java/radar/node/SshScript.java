package radar.node;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import radar.message.ScriptGenerator;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

//TODO split into 2 ssh exec scripts and 1 ssh download
public class SshScript implements Node {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SshScript.class);

    private final String user;
    private final String host;
    private final String password;
    private final ScriptGenerator scriptGenerator;
    private LocalTime currentTime;
    private final Path path;
    private Session session;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public SshScript(Path directory, String user, String host, String password, ScriptGenerator scriptGenerator) {
        this.path = directory;
        this.user = user;
        this.host = host;
        this.password = password;
        this.scriptGenerator = scriptGenerator;
    }

    @Override
    public void start() {
        currentTime = LocalTime.now();
        try {
            JSch.setConfig("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            logger.debug("connecting " + user + "@" + host);
            session.connect();
            logger.debug("connected " + user + "@" + host);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void step(Path data) {
        try {
            Channel channel = session.openChannel("exec");
            SshUtil.execScript(channel, scriptGenerator.generate(currentTime.format(formatter), path.toString()), logger);
            channel.disconnect();
            channel = session.openChannel("exec");
            SshUtil.downloadFile(channel, scriptGenerator.generateRemoteFileName(path.toString()), path.toString(), logger);
            channel.disconnect();
            channel = session.openChannel("exec");
            SshUtil.execScript(channel, "rm -f " + scriptGenerator.generateRemoteFileName(path.toString()), logger);
            channel.disconnect();
            logger.trace("done, await");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        currentTime = LocalTime.now();
    }

    @Override
    public void signalStepEnd() {
    }

    @Override
    public void end() {
        session.disconnect();
    }
}