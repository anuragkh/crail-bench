package edu.berkeley.cs;

import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import edu.berkeley.cs.crail.CrailBenchmarkService;
import edu.berkeley.cs.log.LogServer;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.ini4j.Ini;

public class Main {

  private static CrailBenchmarkService localService() {
    return new CrailBenchmarkService() {
      private void mHandler(Map<String, String> conf) {
        super.handler(conf);
      }

      @Override
      public void handler(Map<String, String> conf) {
        Thread t = new Thread(() -> mHandler(conf));
        t.setDaemon(true);
        t.start();
      }
    };
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: bench_runner [command] [conf_file]");
      return;
    }
    String command = args[0];
    String iniFile = args[1];

    Thread serverThread = new Thread(new LogServer(8888, 1));
    serverThread.start();

    Ini ini = new Ini();
    ini.load(new File(iniFile));
    Map<String, String> conf = ini.get("crail");
    CrailBenchmarkService service;
    if (command.equalsIgnoreCase("invoke")) {
      service = LambdaInvokerFactory.builder()
          .lambdaClient(AWSLambdaClientBuilder.defaultClient())
          .build(CrailBenchmarkService.class);
    } else if (command.equalsIgnoreCase("invoke-local")) {
      service = localService();
    } else {
      System.err.println("Unrecognized command: " + command);
      return;
    }
    assert service != null;
    service.handler(conf);

    serverThread.join();
  }
}
