package serverchainsimulator;

import com.jcraft.jsch.*;

import java.io.*;

public class RemoteShellScrip{
	
	
	   public static void get(String user, String host, String password, String sudo_pass, String command ){

    try{
    	JSch.setConfig("StrictHostKeyChecking", "no");		 
		JSch jsch = new JSch();
		Session session = jsch.getSession(user, host,22);
		session.setPassword(password);
   
      session.connect();
      System.out.println("Connected");

      Channel channel=session.openChannel("exec");
   

      ((ChannelExec)channel).setCommand(command);
      
      InputStream in=channel.getInputStream();
      OutputStream out=channel.getOutputStream();
      ((ChannelExec)channel).setErrStream(System.err);

      channel.connect();
System.out.println("no sudo");
      //out.write((sudo_pass+"\n").getBytes());
      out.flush();

      byte[] tmp=new byte[1024];
      while(true){
        while(in.available()>0){
          int i=in.read(tmp, 0, 1024);
          if(i<0)break;
          System.out.print(new String(tmp, 0, i));
        }
        if(channel.isClosed()){
          System.out.println("exit-status: "+channel.getExitStatus());
          break;
        }
        try{Thread.sleep(1000);}catch(Exception ee){}
      }
      channel.disconnect();
      session.disconnect();
    }
    catch(Exception e){
      System.out.println(e);
    }
  }
}