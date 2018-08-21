package edu.berkeley.cs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

class LogServer implements Runnable {

  private static final String POISON_PILL = "CLOSE";

  private Selector selector;
  private ServerSocketChannel serverSocket;
  private ByteBuffer buffer;
  private int numConnections;

  LogServer(int port, int numConnections) throws IOException {
    this.selector = Selector.open();
    this.serverSocket = ServerSocketChannel.open();
    this.serverSocket.bind(new InetSocketAddress("0.0.0.0", port));
    this.serverSocket.configureBlocking(false);
    this.serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    this.buffer = ByteBuffer.allocate(4096);
    this.numConnections = numConnections;
  }

  @Override
  public void run() {
    try {
      int numClosed = 0;
      while (serverSocket.isOpen()) {
        int readyChannels = selector.select();
        if (readyChannels == 0)
          continue;

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
            String msg = new String(buffer.array()).trim();
            if (msg.equals(POISON_PILL)) {
              client.close();
              System.err.println("Function @ " + client.getRemoteAddress() + " Finished execution");
              numClosed++;
              if (numClosed == numConnections) {
                serverSocket.close();
              }
            } else {
              System.err.println("Function @ " + client.getRemoteAddress() + ": " + msg);
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
}
