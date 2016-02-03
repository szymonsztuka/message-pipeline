package messagepipeline.node;

import com.jcraft.jsch.*;
import org.slf4j.*;
import messagepipeline.content.ShellScriptGenerator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

public class RemoteShellScrip implements Runnable {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RemoteShellScrip.class);

  String user;
  String host;
  String password;
  String sudo_pass;
  String command;
  CyclicBarrier barrier;
  List<String> files;
  volatile boolean  go = false;
  ShellScriptGenerator shellScriptGenerator;
  LocalTime currentTime;

  public RemoteShellScrip(String user, String host, String password, String sudo_pass, List<String> files, CyclicBarrier barrier, ShellScriptGenerator shellScriptGenerator) {
      this.user=user;
      this.host=host;
      this.password=password;
      this.sudo_pass=sudo_pass;
      this.command=command;
      this.barrier = barrier;
      this.files = files;
      this.shellScriptGenerator = shellScriptGenerator;
  }

  public void signalBeginOfBatch() {
      go = true;
      logger.info("signalBeginOfBatch go="+ go);
  }

  public void run(){
      currentTime = LocalTime.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
      logger.info("start"+user+ " "+host+ " "+ password);
      try {
    	  JSch.setConfig("StrictHostKeyChecking", "no");
		  JSch jsch = new JSch();
		  Session session = jsch.getSession(user, host, 22);
		  session.setPassword(password);
          logger.debug("connecting: " +user+ " "+host+ " "+ password);
          session.connect();
          logger.debug("Connected");
          for(String file: files) {
              Channel channel=session.openChannel("exec");
              String  script= shellScriptGenerator.generate(currentTime.format(formatter),file);
              logger.debug(script);
              ((ChannelExec)channel).setCommand(script);
              InputStream in=channel.getInputStream();
              OutputStream out=channel.getOutputStream();
              ((ChannelExec)channel).setErrStream(System.err);
              logger.info("await for go=" + go);
              while(!go) {
                 Thread.sleep(20);
              }
              logger.info("go="+ go);
              channel.connect();
              //out.write((sudo_pass+"\n").getBytes());
              out.flush();
              byte[] tmp=new byte[1024];
              while(true) {
                  while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    logger.debug(new String(tmp, 0, i));
                   }
                  if (channel.isClosed()) {
                    logger.debug("exit-status: " + channel.getExitStatus());
                    break;
                  }
              }
              channel.disconnect();
              channel = session.openChannel("exec");
              downloadFile(channel, shellScriptGenerator.generateRemoteFileName(file), file);
              channel = session.openChannel("exec");
              removeFile(channel, shellScriptGenerator.generateRemoteFileName(file));
              go = false;
              logger.info("done, await");
              barrier.await();
              currentTime = LocalTime.now();
      }
      session.disconnect();
    } catch(Exception e){
        logger.error(e.getMessage(),e);
    }
  }

  private void downloadFile(Channel channel, String remoteSrcFile, String localDestFile){

    FileOutputStream fos = null;
    try {
    String command = "scp -f "+remoteSrcFile; ///home/ssztuka/
    ((ChannelExec)channel).setCommand(command);
      logger.debug("Download file command "+ command);
    // get I/O streams for remote scp
    OutputStream out = channel.getOutputStream();
    InputStream in = channel.getInputStream();

    channel.connect();

    byte[] buf = new byte[1024];

    // send '\0'
    buf[0] = 0; out.write(buf, 0, 1); out.flush();

    while(true){

      int c=checkAck(in);
      if(c!='C'){
        break;
      }

      // read '0644 '
      in.read(buf, 0, 5);

      long filesize=0L;
      while(true){
        if(in.read(buf, 0, 1)<0){
          // error
          break;
        }
        if(buf[0]==' ')break;
        filesize=filesize*10L+(long)(buf[0]-'0');

      }

      String file;
      for(int i=0;;i++){
        in.read(buf, i, 1);
        if(buf[i]==(byte)0x0a){
          file=new String(buf, 0, i);
          break;
        }

      }
      logger.debug("filesize="+filesize+", file="+file);

      // send '\0'
      buf[0]=0; out.write(buf, 0, 1); out.flush();

      // read a content of lfile
      if (Files.notExists(Paths.get(localDestFile).getParent())) {
        Files.createDirectories(Paths.get(localDestFile).getParent());
      }

      fos=new FileOutputStream( localDestFile);
      int foo;
      while(true){
        if(buf.length<filesize) foo=buf.length;
        else foo=(int)filesize;
        foo=in.read(buf, 0, foo);
        if(foo<0){
          // error
          break;
        }
        fos.write(buf, 0, foo);
        filesize-=foo;
        if(filesize==0L) break;
      }
      fos.close();
      fos=null;

      if(checkAck(in)!=0){
        return;
      }

      // send '\0'
      buf[0]=0; out.write(buf, 0, 1); out.flush();
    }
      channel.disconnect();
    }
    catch(Exception e){
      logger.error(e.toString(), e);
      try{if(fos!=null)fos.close();}catch(Exception ee){}
    }
  }

  private void removeFile(Channel channel, String remoteSrcFile){
    try {
    String  script= "rm -f " + remoteSrcFile; // /home/ssztuka/
    logger.debug(script);
    ((ChannelExec)channel).setCommand(script);
    InputStream in=channel.getInputStream();
    OutputStream out=channel.getOutputStream();
    ((ChannelExec)channel).setErrStream(System.err);
    logger.info("await for go=" + go);
    while(!go) {
      Thread.sleep(20);
    }
    logger.info("go="+ go);
    channel.connect();
    //out.write((sudo_pass+"\n").getBytes());
    out.flush();
    byte[] tmp=new byte[1024];
    while(true) {
      while (in.available() > 0) {
        int i = in.read(tmp, 0, 1024);
        if (i < 0) break;
        logger.debug(new String(tmp, 0, i));
      }
      if (channel.isClosed()) {
        logger.debug("exit-status: " + channel.getExitStatus());
        break;
      }
    }
    channel.disconnect();
    }
    catch(Exception e){
      logger.error(e.getMessage(),e);
    }
  }

  static int checkAck(InputStream in) throws IOException {

    int b=in.read();

    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //          -1
    if(b==0) return b;
    if(b==-1) return b;

    if(b==1 || b==2){
      StringBuffer sb=new StringBuffer();
      int c;
      do {
        c=in.read();
        sb.append((char)c);

      }
      while(c!='\n');
      if(b==1){ // error
        //System.out.print(sb.toString());
      }
      if(b==2){ // fatal error
        //System.out.print(sb.toString());
      }
    }
    return b;
  }
}