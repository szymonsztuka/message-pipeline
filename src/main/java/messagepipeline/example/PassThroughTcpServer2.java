package messagepipeline.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class PassThroughTcpServer2 implements Runnable {

    final int clientPort;
    final int serverPort;
    String ip;
    public volatile boolean run = true;

    public PassThroughTcpServer2(String ip, int clientPort, int serverPort){
        this.clientPort = clientPort;
        this.serverPort = serverPort;
        if (ip == null || "".equals(ip)) {
            this.ip = ip;
        } else {
            this.ip = "127.0.0.1";
        }
    }
    public static void main(String[] args) {
        Thread obj1 = new Thread(new PassThroughTcpServer2("", Integer.parseInt(args[0]), Integer.parseInt(args[1])));
        //Thread obj2 = new Thread(new  PassThroughTcpServer("", 5555, 5557));
        obj1.start();
        //obj2.start();
    }

    public void run() {
        System.out.println(this.getClass().toString());
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        while(true) {
           // System.out.println("a");

            try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {// System.out.println("e");
                System.out.println("szsz serverSocketChannel.getLocalAddress() "+serverSocketChannel.getLocalAddress());
                if (serverSocketChannel.isOpen()) {
                    serverSocketChannel.configureBlocking(true);
                    serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                    serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                    serverSocketChannel.bind(new InetSocketAddress(ip, serverPort));
                    System.out.println("serverSocketChannel.getLocalAddress() open "+serverSocketChannel.getLocalAddress() +" "+run);
                    //System.out.println("Waiting for connections ...");
                    //while (run) {
                        System.out.println("a");
                        try (SocketChannel serverChannel = serverSocketChannel.accept()) {
                            System.out.println("b");
            /////////
            try (SocketChannel clientChannel = SocketChannel.open()) { System.out.println("c");
                if (clientChannel.isOpen()) { System.out.println("d");
                    clientChannel.configureBlocking(true);
                    clientChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
                    clientChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
                    clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                    clientChannel.setOption(StandardSocketOptions.SO_LINGER, 5);
                    clientChannel.connect(new InetSocketAddress(ip, clientPort));
                    if (clientChannel.isConnected()) { //System.out.println("d");
                         //System.out.println("h");
                                        System.out.println("Incoming connection from: " + serverChannel.getRemoteAddress());
                                        while (clientChannel.read(buffer) != -1) { //client reads !!!!!!!!!!!
                                            System.out.println("i");
                                            buffer.flip();


                                            serverChannel.write(buffer);//server writes !!!!!!!!!!!
                                            //buffer.rewind();
                                            //System.out.println();
                                            //System.out.println("j");
                                            if (buffer.hasRemaining()) {
                                                buffer.compact();
                                            } else {
                                                buffer.clear();
                                            }
                                        }
                                        System.out.println("z");
                                        run =false;

                                }
                                System.out.println("serverSocketChannel.getLocalAddress() end "+serverSocketChannel.getLocalAddress());
                            } else {
                                System.out.println("The server socket channel cannot be opened!");
                            }
                        } catch (IOException ex) {
                            System.err.println(ex);
                             ex.printStackTrace();
                        }
                        ///////////////////////
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        } System.out.println("g2");
                   // }
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
       // System.out.println("bye bye");
    }


/*
    final int DEFAULT_PORT = 5555;
    final String ip = "127.0.0.1";
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    ByteBuffer helloBuffer = ByteBuffer.wrap("Hello !".getBytes());
    ByteBuffer randomBuffer;
    CharBuffer charBuffer;
    Charset charset = Charset.defaultCharset();
    CharsetDecoder decoder = charset.newDecoder();
       try (SocketChannel socketChannel = SocketChannel.open()) {
        if (socketChannel.isOpen()) {
        socketChannel.configureBlocking(true);
        socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
        socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.SO_LINGER, 5);
        socketChannel.connect(new InetSocketAddress(ip, DEFAULT_PORT));
        if (socketChannel.isConnected()) {
        socketChannel.write(helloBuffer);
        while (socketChannel.read(buffer) != -1) {
        buffer.flip();
        charBuffer = decoder.decode(buffer);
        System.out.println(charBuffer.toString());
        if (buffer.hasRemaining()) {
        buffer.compact();
        } else {
        buffer.clear();
        }
        int r = new Random().nextInt(100);
        if (r == 50) {
        System.out.println("50 was generated! Close the socket channel!");
        break;
        } else {
        randomBuffer = ByteBuffer.wrap("Random number:".
        concat(String.valueOf(r)).getBytes());
        socketChannel.write(randomBuffer);
        }
        }
        } else {
        System.out.println("The connection cannot be established!");
        }
        } else {
        System.out.println("The socket channel cannot be opened!");
        }
        } catch (IOException ex) {
        System.err.println(ex);
        }
        }
        }
        */
}