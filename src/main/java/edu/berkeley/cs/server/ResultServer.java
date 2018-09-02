package edu.berkeley.cs.server;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ResultServer implements Runnable {
  private static final String EOF = "::";
  private static final String EOC = "::::";

  private ServerSocket serverSocket;
  private AtomicInteger numClosed;
  private int numConnections;

  public ResultServer(int port, int numConnections) throws IOException {
    this.serverSocket = new ServerSocket(port);
    this.numClosed = new AtomicInteger(0);
    this.numConnections = numConnections;
  }

  public ResultServer(int port) throws IOException {
    this(port, 1);
  }

  @Override
  public void run() {
    System.out.println("Result server waiting for connections");
    while (!serverSocket.isClosed()) {
      try {
        Socket socket = serverSocket.accept();
        Thread t = new Thread(() -> {
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String fileName;
            while (!(fileName = in.readLine()).equals(EOC)) {
              PrintWriter out = new PrintWriter(new FileWriter(fileName));
              String line;
              while (!(line = in.readLine()).equals(EOF)) {
                out.write(line + "\n");
              }
              out.close();
            }
            in.close();
            int n = numClosed.incrementAndGet();
            if (n == numConnections) {
              serverSocket.close();
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
        t.start();
      } catch (IOException e) {
        return;
      }
    }
  }
}
