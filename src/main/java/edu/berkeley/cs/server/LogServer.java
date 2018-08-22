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

  public LogServer(int port, int numConnections) throws IOException {
    this.selector = Selector.open();
    this.serverSocket = ServerSocketChannel.open();
    this.serverSocket.bind(new InetSocketAddress("0.0.0.0", port));
    this.serverSocket.configureBlocking(false);
    this.serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    this.buffer = ByteBuffer.allocate(4096);
    this.numConnections = numConnections;
    this.ids = new HashSet<>();
  }

  @Override
  public void run() {
    try {
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
              printMessages(client.getRemoteAddress(), msgBuf.replace(EOM, "Aborted execution"));
              client.close();
            } else if (msgBuf.contains("LAMBDA_ID:")) {
              String id = msgBuf.replace("LAMBDA_ID:", "");
              System.out.println("Received connection from " + client.getRemoteAddress() + " with ID=[" + id + "]");
              buffer.clear();
              if (ids.contains(id)) {
                System.out.println("Aborting " + client.getRemoteAddress() + ": Duplicate function");
                buffer.put("ABORT\n".getBytes());
              } else {
                ids.add(id);
                buffer.put("OK\n".getBytes());
              }
              buffer.flip();
              client.write(buffer);
              buffer.clear();
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
