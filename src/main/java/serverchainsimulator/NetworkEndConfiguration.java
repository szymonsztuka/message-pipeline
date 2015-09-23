package serverchainsimulator;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;

public class NetworkEndConfiguration {

    public final InetSocketAddress adress;
    public final Path directory;
    public final int noClients;
    public final boolean sendAtTimestamp;
    //tring user, String host, String password, String sudo_pass, String command

    public NetworkEndConfiguration(String ip, String port, String directory, String noClients, boolean sendAtTimestamp) {
    	adress = new InetSocketAddress(ip, Integer.parseInt(port));
    	this.directory = Paths.get(directory);
    	int val =0;
    	try {
    		val = Integer.parseInt(noClients);
    	} catch (NumberFormatException  e) {
    		val = 1;
    	}
    	this.noClients = val;
    	/*if(sendAtTimestamp!=null && "t".equalsIgnoreCase(sendAtTimestamp.trim())) {
    		this.sendAtTimestamp = true;
    	} else {
    		this.sendAtTimestamp = false;
    	}*/
    	this.sendAtTimestamp = sendAtTimestamp;
    	
	}

	String user;
	String host;
	String password;
	String sudo_pass;
	String command;

	public NetworkEndConfiguration(String user, String host, String password, String sudo_pass, String command) {
		this.user = user;
		this.host = host;
		this.password = password;
		this. sudo_pass = sudo_pass;
		this.command = command;

		adress =null;
		directory =null;
		noClients=0;
		sendAtTimestamp = false;

	}
}
