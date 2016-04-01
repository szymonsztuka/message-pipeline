package radar.node;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import org.slf4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SshUtil {

    public static void execScript(Channel channel, String script, Logger logger) {
        try {
            logger.debug(script);
            ((ChannelExec) channel).setCommand(script);
            InputStream in = channel.getInputStream();
            OutputStream out = channel.getOutputStream();
            ((ChannelExec) channel).setErrStream(new NullOutputStream());
            channel.connect();
            out.flush();
            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    logger.debug(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    logger.trace("exit-status: " + channel.getExitStatus());
                    break;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static int checkAck(InputStream in) throws IOException {

        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if (b == 0) return b;
        if (b == -1) return b;
        if (b == 1 || b == 2) {
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            } while (c != '\n');
            if (b == 1) { // error
                //System.out.print(sb.toString());
            }
            if (b == 2) { // fatal error
                //System.out.print(sb.toString());
            }
        }
        return b;
    }

    public static void downloadFile(Channel channel, String remoteSrcFile, String localDestFile, Logger logger) {
        FileOutputStream fos = null;
        try {
            String command = "scp -f " + remoteSrcFile;
            ((ChannelExec) channel).setCommand(command);
            logger.trace("Download file conf " + command);
            OutputStream out = channel.getOutputStream(); // get I/O streams for remote scp
            InputStream in = channel.getInputStream();
            channel.connect();
            byte[] buf = new byte[1024];
            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();
            while (true) {
                int c = checkAck(in);
                if (c != 'C') {
                    break;
                }
                in.read(buf, 0, 5);// read '0644 '
                long filesize = 0L;
                while (true) {
                    if (in.read(buf, 0, 1) < 0) {
                        break;// error
                    }
                    if (buf[0] == ' ') break;
                    filesize = filesize * 10L + (long) (buf[0] - '0');
                }
                String file;
                for (int i = 0; ; i++) {
                    in.read(buf, i, 1);
                    if (buf[i] == (byte) 0x0a) {
                        file = new String(buf, 0, i);
                        break;
                    }
                }
                logger.trace("filesize=" + filesize + ", file=" + file);
                // send '\0'
                buf[0] = 0;
                out.write(buf, 0, 1);
                out.flush();
                // read a message of lfile
                if (Files.notExists(Paths.get(localDestFile).getParent())) {
                    Files.createDirectories(Paths.get(localDestFile).getParent());
                }
                fos = new FileOutputStream(localDestFile);
                int foo;
                //boolean cr = false;
                while (true) {
                    if (buf.length < filesize) {
                        foo = buf.length;
                    } else {
                        foo = (int) filesize;
                    }
                    foo = in.read(buf, 0, foo);
                    if (foo < 0) {
                        break;// error
                    }
                    final byte[] transformed = new byte[foo * 2];
                    int len = 0;
                    for (int i = 0; i < foo; i++) {
                        if (buf[i] == (byte) '\n') {        // LF
                            if (i - 1 > 0 && buf[i - 1] != (byte) '\r') {
                                transformed[len] = (byte) '\r';
                                len++;
                            }
                        }
                        transformed[len] = buf[i];
                        len++;
                    }
                    final byte[] result = new byte[len];
                    System.arraycopy(transformed, 0, result, 0, len);
                    fos.write(result, 0, len);
                    //fos.write(buf, 0, foo);
                    filesize -= foo;
                    if (filesize == 0L) {
                        break;
                    }
                }
                fos.close();
                fos = null;
                if (checkAck(in) != 0) {
                    return;
                }
                // send '\0'
                buf[0] = 0;
                out.write(buf, 0, 1);
                out.flush();
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
            try {
                if (fos != null) fos.close();
            } catch (Exception ee) {
            }
        }
    }

    /*public static void removeFile(Channel channel, String remoteSrcFile, Logger logger) {
        try {
            String script = "rm -f " + remoteSrcFile;
            logger.debug(script);
            ((ChannelExec) channel).setCommand(script);
            InputStream in = channel.getInputStream();
            OutputStream out = channel.getOutputStream();
            ((ChannelExec) channel).setErrStream(new NullOutputStream());
            channel.connect();
            out.flush();
            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    logger.debug(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    logger.trace("exit-status: " + channel.getExitStatus());
                    break;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }*/

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            //logger.debug(String.valueOf(b));
        }
    }
}
