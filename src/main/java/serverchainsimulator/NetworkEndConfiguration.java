package serverchainsimulator;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;

public class NetworkEndConfiguration {

    public final InetSocketAddress adress;
    public final Path directory;
    
    public NetworkEndConfiguration(String ip, String port, String directory) {
    	adress = new InetSocketAddress(ip, Integer.parseInt(port));
    	this.directory = Paths.get(directory);
	}
}
