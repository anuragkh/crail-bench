package edu.berkeley.cs;

import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import edu.berkeley.cs.crail.CrailBenchmarkService;
import edu.berkeley.cs.server.ControlServer;
import edu.berkeley.cs.server.LogServer;
import edu.berkeley.cs.server.ResultServer;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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

  private static BenchmarkService makeService(String command, Map<String, String> conf) {
    BenchmarkService service = null;
    if (command.equalsIgnoreCase("invoke")) {
      conf.put("local", "false");
      service = LambdaInvokerFactory.builder().build(BenchmarkService.class);
    } else if (command.equalsIgnoreCase("invoke-local")) {
      conf.put("local", "true");
      service = localService();
    } else {
      System.err.println("Unrecognized command: " + command);
      System.exit(0);
    }
    return service;
  }

  private static BenchmarkService[] makeServices(String command, Map<String, String> conf, int n) {
    BenchmarkService[] services = new BenchmarkService[n];
    for (int i = 0; i < n; i++) {
      services[i] = makeService(command, conf);
    }
    return services;
  }

  private static void invokePeriodically(BenchmarkService[] services, Map<String, String> conf,
      int period, int numPeriods) throws InterruptedException {
    int perPeriod = services.length / numPeriods;
    for (int i = 0; i < numPeriods; i++) {
      for (int j = 0; j < perPeriod; j++) {
        int lambdaId = i * perPeriod + j;
        System.out.println("Launching lambda_id=" + lambdaId);
        Map<String, String> mConf = new HashMap<>(conf);
        mConf.put("lambda_id", String.valueOf(lambdaId));
        services[lambdaId].handler(mConf);
      }
      Thread.sleep(period * 1000);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: bench_runner [command] [conf_file]");
      return;
    }
    String command = args[0];
    String iniFile = args[1];

    Thread logThread;
    Thread controlThread;
    Thread resultThread = null;

    Ini ini = new Ini();
    ini.load(new File(iniFile));
    Map<String, String> conf = ini.get("crail");
    String mode = conf.getOrDefault("mode", "create_write_read_destroy");
    int logPort = Integer.parseInt(conf.getOrDefault("logger_port", "8888"));
    int controlPort = Integer.parseInt(conf.getOrDefault("control_port", "8889"));
    int resultPort = Integer.parseInt(conf.getOrDefault("result_port", "8890"));
    if (mode.startsWith("scale:")) {
      String[] parts = mode.split(":");
      mode = parts[1];
      conf.put("mode", mode);
      int n = Integer.parseInt(parts[2]);
      int period = Integer.parseInt(parts[3]);
      int numPeriods = Integer.parseInt(parts[4]);
      int numFunctions = n * numPeriods;
      System.out.println("Running scale benchmark with mode=" + mode + " n=" + n + " period=" +
          period + " numPeriods=" + numPeriods);

      logThread = new Thread(new LogServer(logPort, numFunctions));
      logThread.start();

      controlThread = new Thread(new ControlServer(controlPort, numFunctions, numPeriods, period));
      controlThread.start();

      if (!command.equalsIgnoreCase("invoke-local")) {
        resultThread = new Thread(new ResultServer(resultPort, numFunctions));
        resultThread.start();
      }

      BenchmarkService[] services = makeServices(command, conf, numFunctions);
      invokePeriodically(services, conf, period, numPeriods);
    } else {
      logThread = new Thread(new LogServer(logPort));
      logThread.start();

      controlThread = new Thread(new ControlServer(controlPort));
      controlThread.start();

      if (!command.equalsIgnoreCase("invoke-local")) {
        resultThread = new Thread(new ResultServer(resultPort));
        resultThread.start();
      }

      makeService(command, conf).handler(conf);
    }

    logThread.join();
    if (resultThread != null) {
      resultThread.join();
    }
  }
}
