package edu.berkeley.cs.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ControlServer implements Runnable {

  private static final String ABORT = "ABORT\n";

  private Selector selector;
  private ServerSocketChannel serverSocket;
  private ByteBuffer buffer;
  private int numConnections;
  private Set<String> ids;
  private ArrayList<SocketChannel> ready;
  private int numTriggers;
  private int connectionsPerTrigger;
  private int triggerPeriod;

  public ControlServer(int port, int numConnections, int numTriggers, int triggerPeriod) throws IOException {
    this.selector = Selector.open();
    this.serverSocket = ServerSocketChannel.open();
    this.serverSocket.bind(new InetSocketAddress("0.0.0.0", port));
    this.serverSocket.configureBlocking(false);
    this.serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    this.buffer = ByteBuffer.allocate(4096);
    this.numConnections = numConnections;
    this.numTriggers = numTriggers;
    this.connectionsPerTrigger = numConnections / numTriggers;
    this.triggerPeriod = triggerPeriod;
    this.ids = new HashSet<>();
    this.ready = new ArrayList<>();
  }

  public ControlServer(int port) throws IOException {
    this(port, 1, 1, 0);
  }

  @Override
  public void run() {
    try {
      System.out.println("Control server waiting for connections");
      boolean run = true;
      while (run) {
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
            buffer.clear();
            client.read(buffer);
            buffer.flip();
            String msgBuf = StandardCharsets.UTF_8.decode(buffer).toString().trim();

            String id = msgBuf.replace("LAMBDA_ID:", "");

            if (ids.contains(id)) {
              buffer.clear();
              buffer.put(ABORT.getBytes());
              buffer.flip();
              client.write(buffer);
            } else {
              System.out.println("[ControlServer] Queuing " + client.getRemoteAddress() + ", ID=[" + id + "]");
              ids.add(id);
              ready.add(client);
              System.out.println("[ControlServer] Progress: " + ids.size() + "/" + numConnections);
              if (ready.size() == numConnections) {
                run = false;
              }
            }
          }
          iter.remove();
        }
      }
      selector.close();

      for (int i = 0; i < numTriggers; i++) {
        System.out.println("[ControlServer] Running " + numTriggers + " functions...");
        for (int j = 0; j < connectionsPerTrigger; j++) {
          SocketChannel channel = ready.get(i);
          System.out.println("[ControlServer] Running " + channel.getRemoteAddress() + "...");
          buffer.clear();
          buffer.put("OK\n".getBytes());
          buffer.flip();
          channel.write(buffer);
        }
        System.out.println("[ControlServer] End of wave " + i);
        Thread.sleep(triggerPeriod * 1000);
      }

      serverSocket.close();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

}
