package messagepipeline.addons;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class PassThroughTcpServer2 implements Runnable {

    private final int clientPort;
    private final int serverPort;
    private final String ip;

    public PassThroughTcpServer2(String ip, int clientPort, int serverPort) {
        this.clientPort = clientPort;
        this.serverPort = serverPort;
        if (ip == null || "".equals(ip)) {
            this.ip = ip;
        } else {
            this.ip = "127.0.0.1";
        }
    }

    public static void main(String[] args) {
        (new Thread(new PassThroughTcpServer2("", Integer.parseInt(args[0]), Integer.parseInt(args[1])))).start();
    }

    public void run() {
        System.out.println(this.getClass().toString());
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        while (true) {
            try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {// System.out.println("e");
                System.out.println("szsz serverSocketChannel.getLocalAddress() " + serverSocketChannel.getLocalAddress());
                if (serverSocketChannel.isOpen()) {
                    serverSocketChannel.configureBlocking(true);
                    serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                    serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                    serverSocketChannel.bind(new InetSocketAddress(ip, serverPort));
                    System.out.println("serverSocketChannel.getLocalAddress() open " + serverSocketChannel.getLocalAddress());
                    System.out.println("a");
                    try (SocketChannel serverChannel = serverSocketChannel.accept()) {
                        System.out.println("b");
                        try (SocketChannel clientChannel = SocketChannel.open()) {
                            System.out.println("c");
                            if (clientChannel.isOpen()) {
                                System.out.println("d");
                                clientChannel.configureBlocking(true);
                                clientChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
                                clientChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
                                clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                                clientChannel.setOption(StandardSocketOptions.SO_LINGER, 5);
                                clientChannel.connect(new InetSocketAddress(ip, clientPort));
                                if (clientChannel.isConnected()) {
                                    System.out.println("Incoming connection from: " + serverChannel.getRemoteAddress());
                                    while (clientChannel.read(buffer) != -1) { //client reads
                                        System.out.println("i");
                                        buffer.flip();
                                        serverChannel.write(buffer); //server writes
                                        if (buffer.hasRemaining()) {
                                            buffer.compact();
                                        } else {
                                            buffer.clear();
                                        }
                                    }
                                    System.out.println("z");
                                }
                                System.out.println("serverSocketChannel.getLocalAddress() end " + serverSocketChannel.getLocalAddress());
                            } else {
                                System.out.println("The server socket channel cannot be opened!");
                            }
                        } catch (IOException ex) {
                            System.err.println(ex);
                            ex.printStackTrace();
                        }
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    System.out.println("g2");
                }
            } catch (IOException ex) {
                System.err.println(ex);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("bye bye main loop");
        }
    }
}