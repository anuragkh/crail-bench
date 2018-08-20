package edu.berkeley.cs;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;

public class BenchmarkService {
  private static final int MAX_NUM_OPS = 1000;
  private static final int MAX_DATA_SIZE = 1073741824;
  private static final int MAX_ERRORS = 1000;
  private static final String RESULT_BUCKET = "bench-results";

  class Logger implements Closeable {
    private Socket socket;
    private PrintWriter out;

    Logger(String host, int port) throws IOException {
      this.socket = new Socket(host, port);
      this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    void info(String msg) {
      log("INFO", msg);
    }

    void warn(String msg) {
      log("WARN", msg);
    }

    void error(String msg) {
      log("ERROR", msg);
    }

    private void write(String data) {
      this.out.write(data + "\n");
      this.out.flush();
    }

    private void log(String msgType, String msg) {
      write(msgType + " " + msg);
    }

    PrintWriter getPrintWriter() {
      return out;
    }

    void flush() {
      this.out.flush();
    }

    public void close() throws IOException {
      write("CLOSE");
      this.socket.shutdownInput();
      this.socket.shutdownOutput();
      this.socket.close();
    }
  }

  @LambdaFunction(functionName = "CrailBenchmark")
  public void handler(Map<String, String> conf) {
    Properties properties = new Properties();
    properties.putAll(conf);

    int objSize = Integer.parseInt(conf.getOrDefault("size", "1024"));
    String host = properties.getProperty("logger_host");
    int port = Integer.parseInt(properties.getProperty("logger_port"));

    Logger log;
    try {
      log = new Logger(host, port);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    try {
      benchmark(new Crail(), objSize, numOps(objSize), properties, log);
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace(log.getPrintWriter());
      log.flush();
    }

    try {
      log.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void benchmark(Crail c, int size, int nOps, Properties conf, Logger log)
      throws Exception {
    int errCount = 0;

    StringBuilder lr = new StringBuilder();
    StringBuilder lw = new StringBuilder();
    StringBuilder tr = new StringBuilder();
    StringBuilder tw = new StringBuilder();

    log.info("Initializing storage interface...");
    c.init(conf);

    log.info("Starting writes...");
    long wBegin = nowUs();
    for (int i = 0; i < nOps; ++i) {
      long tBegin = nowUs();
      try {
        c.write(String.valueOf(i));
      } catch (RuntimeException e) {
        log.warn("WriteOp failed: ");
        e.printStackTrace(log.getPrintWriter());
        log.flush();
        --i;
        errCount++;
        if (errCount > MAX_ERRORS) {
          log.error("Too many errors");
          System.exit(-1);
        }
      }
      long tEnd = nowUs();
      lw.append(String.valueOf(tEnd - tBegin)).append("\n");
    }
    long wEnd = nowUs();
    log.info("Finished writes, starting reads...");
    errCount = 0;
    long rBegin = nowUs();
    for (int i = 0; i < nOps; ++i) {
      long tBegin = nowUs();
      try {
        String retValue = c.read(String.valueOf(i));
        assert retValue.length() == size;
      } catch (RuntimeException e) {
        log.warn("ReadOp failed: ");
        e.printStackTrace(log.getPrintWriter());
        log.flush();
        --i;
        errCount++;
        if (errCount > MAX_ERRORS) {
          log.error("Too many errors");
          System.exit(-1);
        }
      }
      long tEnd = nowUs();
      lr.append(String.valueOf(tEnd - tBegin)).append("\n");
    }
    long rEnd = nowUs();

    log.info("Finished benchmark.");
    double wElapsedS = ((double) (wEnd - wBegin)) / 1000000.0;
    double rElapsedS = ((double) (rEnd - rBegin)) / 1000000.0;
    tw.append(String.valueOf(nOps / wElapsedS)).append("\n");
    tr.append(String.valueOf(nOps / rElapsedS)).append("\n");

    c.destroy();
    log.info("Destroyed storage interface.");

    String lrPath = "/tmp/crail_" + String.valueOf(size) + "_read_latency.txt";
    String lwPath = "/tmp/crail_" + String.valueOf(size) + "_write_latency.txt";
    String trPath = "/tmp/crail_" + String.valueOf(size) + "_read_throughput.txt";
    String twPath = "/tmp/crail_" + String.valueOf(size) + "_write_throughput.txt";

    writeToS3(lrPath, lr.toString(), log);
    writeToS3(lwPath, lw.toString(), log);
    writeToS3(trPath, tr.toString(), log);
    writeToS3(twPath, tw.toString(), log);

    log.info("Wrote all results to S3");
  }

  private static void writeToS3(String key, String value, Logger log) {
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new ProfileCredentialsProvider())
        .build();
    s3Client.putObject(RESULT_BUCKET, key, value);
    log.info("Uploaded results to s3://" + RESULT_BUCKET + "/" + key);
  }

  private static long nowUs() {
    return System.nanoTime() / 1000;
  }

  private int numOps(int objSize) {
    return Math.min(MAX_NUM_OPS, MAX_DATA_SIZE / objSize);
  }
}
