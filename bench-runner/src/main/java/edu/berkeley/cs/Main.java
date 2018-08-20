package edu.berkeley.cs;

import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import org.ini4j.Ini;

public class Main {

  static class LogServer implements Runnable {

    private ServerSocket socket;
    private Socket clientSocket;
    private BufferedReader in;

    LogServer(int port) throws IOException {
      socket = new ServerSocket(port);
    }

    @Override
    public void run() {
      try {
        clientSocket = socket.accept();
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        while (clientSocket.isConnected()) {
          String log = in.readLine();
          if (log.equalsIgnoreCase("CLOSE")) {
            break;
          }
          System.err.println("Function @ " + clientSocket.toString() + ": " + log);
        }
        in.close();
        clientSocket.close();
        socket.close();
      } catch (IOException e) {
        System.err.println("Error: " + e.getMessage());
      }
    }
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: bench_runner [command] [conf_file]");
      return;
    }
    String command = args[0];
    String iniFile = args[1];

    Thread serverThread = new Thread(new LogServer(8888));
    serverThread.start();

    Ini ini = new Ini();
    ini.load(new File(iniFile));
    Map<String, String> conf = ini.get("crail");
    BenchmarkService service;
    if (command.equalsIgnoreCase("invoke")) {
      service = LambdaInvokerFactory.builder()
          .lambdaClient(AWSLambdaClientBuilder.defaultClient())
          .build(BenchmarkService.class);

    } else if (command.equalsIgnoreCase("invoke-local")) {
      service = new BenchmarkService();
    } else {
      System.err.println("Unrecognized command: " + command);
      return;
    }
    assert service != null;
    service.handler(conf);

    serverThread.join();
  }
}
