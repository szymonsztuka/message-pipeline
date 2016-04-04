package radar.node;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.slf4j.LoggerFactory;
import radar.message.Script;

import java.nio.file.Path;

public class SshDownload implements Node {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SshScript.class);

    private final String user;
    private final String host;
    private final String password;
    private final Script script;
    private final Path path;
    private Session session;

    public SshDownload(Path directory, String user, String host, String password, Script script) {
        this.path = directory;
        this.user = user;
        this.host = host;
        this.password = password;
        this.script = script;
    }

    @Override
    public void start() {
        try {
            JSch.setConfig("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            logger.trace("connecting " + user + "@" + host);
            session.connect();
            logger.trace("connected " + user + "@" + host);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void step(Path data) {
        try {
            Channel channel = session.openChannel("exec");
            SshUtil.downloadFile(channel, script.generate(path.toString()), path.toString(), logger);
            channel.disconnect();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void signalStepEnd() {
    }

    @Override
    public void end() {
        session.disconnect();
    }
}
