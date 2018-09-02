package edu.berkeley.cs.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LogServer implements Runnable {

  private static final String EOM = "CLOSE";
  private static final String ABORT = "ABORT";

  private Selector selector;
  private ServerSocketChannel serverSocket;
  private ByteBuffer buffer;
  private int numConnections;
  private Set<String> ids;
  private Set<SocketChannel> waitingForTrigger;
  private int numTriggers;
  private int connectionsPerTrigger;

  public LogServer(int port, int numConnections, int numTriggers) throws IOException {
    this.selector = Selector.open();
    this.serverSocket = ServerSocketChannel.open();
    this.serverSocket.bind(new InetSocketAddress("0.0.0.0", port));
    this.serverSocket.configureBlocking(false);
    this.serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    this.buffer = ByteBuffer.allocate(4096);
    this.numConnections = numConnections;
    this.numTriggers = numTriggers;
    this.connectionsPerTrigger = numConnections / numTriggers;
    this.ids = new HashSet<>();
    this.waitingForTrigger = new HashSet<>();
  }

  public LogServer(int port) throws IOException {
    this(port, 1, 1);
  }

  @Override
  public void run() {
    try {
      System.out.println("Log server waiting for connections");
      int numClosed = 0;
      while (serverSocket.isOpen()) {
        int readyChannels = selector.select();
        if (readyChannels == 0) {
          continue;
        }

        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        Iterator<SelectionKey> iter = selectedKeys.iterator();
        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          if (key.isAcceptable()) {
            SocketChannel client = serverSocket.accept();
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ);
            System.out.println("Received connection from " + client.getRemoteAddress());
          } else if (key.isReadable()) {
            SocketChannel client = (SocketChannel) key.channel();
            client.read(buffer);
            buffer.flip();
            String msgBuf = StandardCharsets.UTF_8.decode(buffer).toString().trim();
            if (msgBuf.contains(EOM)) {
              printMessages(client.getRemoteAddress(), msgBuf.replace(EOM, "Finished execution"));
              client.close();
              numClosed++;
              if (numClosed == numConnections) {
                serverSocket.close();
              }
            } else if (msgBuf.contains(ABORT)) {
              printMessages(client.getRemoteAddress(), msgBuf.replace(ABORT, "Aborted execution"));
              client.close();
            } else if (msgBuf.contains("LAMBDA_ID:")) {
              String id = msgBuf.replace("LAMBDA_ID:", "");
              buffer.clear();
              if (ids.contains(id)) {
                buffer.put("ABORT\n".getBytes());
                buffer.flip();
                client.write(buffer);
                buffer.clear();
              } else {
                System.out.println("Queuing " + client.getRemoteAddress() + ", ID=[" + id + "]");
                ids.add(id);
                waitingForTrigger.add(client);
                if (waitingForTrigger.size() == connectionsPerTrigger) {
                  System.out.println("Running " + numTriggers + " functions...");
                  for (SocketChannel channel: waitingForTrigger) {
                    System.out.println("Running " + channel.getRemoteAddress() + "...");
                    buffer.put("OK\n".getBytes());
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                  }
                  waitingForTrigger.clear();
                }
              }
            } else {
              printMessages(client.getRemoteAddress(), msgBuf);
            }
            buffer.clear();
          }
          iter.remove();
        }
      }
      selector.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printMessages(SocketAddress address, String msgBuf) {
    for (String msg : msgBuf.split("\\r?\\n")) {
      System.err.println("Function @ " + address + ": " + msg);
    }
  }
}
