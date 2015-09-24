package serverchainsimulator;

import com.jcraft.jsch.*;
import org.slf4j.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

public class RemoteShellScripTask implements Runnable {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RemoteShellScripTask.class);

  String user;
  String host;
  String password;
  String sudo_pass;
  String command;
  CyclicBarrier barrier;
  List<String> files;
  volatile boolean  go = false;

	   public RemoteShellScripTask(String user, String host, String password, String sudo_pass, String command, List<String> files, CyclicBarrier barrier) {
         this.user=user;
         this.host=host;
         this.password=password;
         this.sudo_pass=sudo_pass;
         this.command=command;
         this.barrier = barrier;
         this.files = files;
       }

  public void signalBeginOfBatch() {
    go = true;
    logger.info("signalBeginOfBatch go="+ go);
  }

  public void run(){
    try{
    	JSch.setConfig("StrictHostKeyChecking", "no");		 
		JSch jsch = new JSch();
		Session session = jsch.getSession(user, host, 22);
		session.setPassword(password);
   
      session.connect();
      System.out.println("Connected");

      Channel channel=session.openChannel("exec");
   




      for(String file: files) {
        ((ChannelExec)channel).setCommand(command + file);

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
            System.out.print(new String(tmp, 0, i));
          }
          if (channel.isClosed()) {
            System.out.println("exit-status: " + channel.getExitStatus());
            break;
          }
        }
        channel.disconnect();
       // try{Thread.sleep(1000);}catch(Exception ee){}
        go = false;
        logger.info("done, await");
        barrier.await();
      }

      session.disconnect();
    }
    catch(Exception e){
      System.out.println(e);
    }
  }
}