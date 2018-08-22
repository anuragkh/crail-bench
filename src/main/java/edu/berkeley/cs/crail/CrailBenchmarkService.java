package edu.berkeley.cs.crail;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import edu.berkeley.cs.BenchmarkService;
import edu.berkeley.cs.keygen.KeyGenerator;
import edu.berkeley.cs.keygen.SequentialKeyGenerator;
import edu.berkeley.cs.keygen.ZipfKeyGenerator;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;

public class CrailBenchmarkService implements BenchmarkService {

  private static final int MAX_ERRORS = 1000;
  private static final String RESULT_BUCKET = "bench-results";
  private static final int BENCHMARK_READ = 1;
  private static final int BENCHMARK_WRITE = 2;
  private static final int BENCHMARK_DESTROY = 4;
  private static final String CRAIL_HOME = "CRAIL_HOME";
  private static final String LAMBDA_TASK_ROOT = "LAMBDA_TASK_ROOT";

  public class Logger implements Closeable {

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

  public void handler(Map<String, String> conf) {
    long startUs = nowUs();

    Properties props = new Properties();
    props.putAll(conf);

    String distribution = conf.getOrDefault("distribution", "sequential");
    int size = Integer.parseInt(conf.getOrDefault("size", "1024"));
    int nOps = Integer.parseInt(conf.getOrDefault("num_ops", "1000"));
    KeyGenerator kGen;
    if (distribution.equalsIgnoreCase("zipf")) {
      kGen = new ZipfKeyGenerator(0.0, nOps);
    } else if (distribution.equalsIgnoreCase("sequential")) {
      kGen = new SequentialKeyGenerator();
    } else {
      throw new RuntimeException("Unrecognized key distribution: " + distribution);
    }
    String modeStr = conf.getOrDefault("mode", "write_read_destroy");
    int mode = 0;
    if (modeStr.contains("read")) {
      mode |= BENCHMARK_READ;
    }
    if (modeStr.contains("write")) {
      mode |= BENCHMARK_WRITE;
    }
    if (modeStr.contains("destroy")) {
      mode |= BENCHMARK_DESTROY;
    }
    boolean warmUp = Boolean.parseBoolean(conf.getOrDefault("warm_up", "true"));
    long timeoutUs = Long.parseLong(conf.getOrDefault("timeout", "240")) * 1000 * 1000;
    long remaining = timeoutUs - (nowUs() - startUs);
    String host = conf.getOrDefault("logger_host", "localhost");
    int port = Integer.parseInt(conf.getOrDefault("logger_port", "8888"));

    Logger log;
    try {
      log = new Logger(host, port);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    try {
      benchmark(new Crail(), props, kGen, size, nOps, mode, warmUp, remaining, log);
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

  private static void benchmark(Crail c, Properties conf, KeyGenerator keyGen,
      int size, int nOps, int mode, boolean warmUp, long maxUs, Logger log)
      throws Exception {
    int errCount = 0;
    int warmUpCount = nOps / 10;
    long startUs = nowUs();
    String outPrefix = "crail/crail_" + String.valueOf(size);

    if (System.getenv(CRAIL_HOME) == null) {
      String crailHome = System.getenv(LAMBDA_TASK_ROOT);
      if (crailHome != null) {
        log.info("Setting environment variable CRAIL_HOME to " + crailHome);
        injectEnvironmentVariable(CRAIL_HOME, crailHome);
      } else {
        log.warn("CRAIL_HOME is not set, may not load appropriate configuration variables");
      }
    }

    log.info("Initializing storage interface...");
    c.init(conf, log);

    if ((mode & BENCHMARK_WRITE) == BENCHMARK_WRITE) {
      StringBuilder lw = new StringBuilder();
      StringBuilder tw = new StringBuilder();

      if (warmUp) {
        log.info("Warm-up writes...");
        for (int i = 0; i < warmUpCount && timeBound(startUs, maxUs, log); ++i) {
          try {
            c.write(keyGen.next());
          } catch (RuntimeException e) {
            handleError(log, ++errCount, e);
          }
        }
      }

      log.info("Starting writes...");
      long wBegin = nowUs();
      for (int i = 0; i < nOps && timeBound(startUs, maxUs, log); ++i) {
        long tBegin = nowUs();
        try {
          c.write(keyGen.next());
        } catch (RuntimeException e) {
          --i;
          handleError(log, ++errCount, e);
        }
        long tEnd = nowUs();
        lw.append(String.valueOf(tEnd - tBegin)).append("\n");
      }
      long wEnd = nowUs();
      log.info("Finished writes.");

      double wElapsedS = ((double) (wEnd - wBegin)) / 1000000.0;
      tw.append(String.valueOf(nOps / wElapsedS)).append("\n");

      writeToS3(outPrefix + "_write_latency.txt", lw.toString(), log);
      writeToS3(outPrefix + "_write_throughput.txt", tw.toString(), log);
    }

    errCount = 0;
    keyGen.reset();
    if ((mode & BENCHMARK_READ) == BENCHMARK_READ) {
      StringBuilder lr = new StringBuilder();
      StringBuilder tr = new StringBuilder();

      if (warmUp) {
        log.info("Warm-up reads...");
        for (int i = 0; i < warmUpCount && timeBound(startUs, maxUs, log); ++i) {
          try {
            String retValue = c.read(keyGen.next());
            assert retValue.length() == size;
          } catch (RuntimeException e) {
            handleError(log, ++errCount, e);
          }
        }
      }

      log.info("Starting reads...");
      long rBegin = nowUs();
      for (int i = 0; i < nOps && timeBound(startUs, maxUs, log); ++i) {
        long tBegin = nowUs();
        try {
          String retValue = c.read(keyGen.next());
          assert retValue.length() == size;
        } catch (RuntimeException e) {
          --i;
          handleError(log, ++errCount, e);
        }
        long tEnd = nowUs();
        lr.append(String.valueOf(tEnd - tBegin)).append("\n");
      }
      long rEnd = nowUs();
      log.info("Finished reads.");

      double rElapsedS = ((double) (rEnd - rBegin)) / 1000000.0;
      tr.append(String.valueOf(nOps / rElapsedS)).append("\n");

      writeToS3(outPrefix + "_read_latency.txt", lr.toString(), log);
      writeToS3(outPrefix + "_read_throughput.txt", tr.toString(), log);
    }

    if ((mode & BENCHMARK_DESTROY) == BENCHMARK_DESTROY) {
      c.destroy();
      log.info("Destroyed storage interface.");
    }
  }

  private static void handleError(Logger log, int errCount, RuntimeException e) {
    if (errCount > MAX_ERRORS) {
      log.error("Too many errors; last error:");
      e.printStackTrace(log.getPrintWriter());
      log.flush();
      System.exit(1);
    }
  }

  private static void writeToS3(String key, String value, Logger log) {
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(Regions.US_EAST_1)
        .build();
    s3Client.putObject(RESULT_BUCKET, key, value);
    log.info("Uploaded results to s3://" + RESULT_BUCKET + "/" + key);
  }

  private static boolean timeBound(long startUs, long maxUs, Logger log) {
    if (nowUs() - startUs < maxUs) {
      return true;
    }
    log.warn("Benchmark timed out...");
    return false;
  }

  private static long nowUs() {
    return System.nanoTime() / 1000;
  }

  private static void injectEnvironmentVariable(String key, String value)
      throws Exception {

    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

    Field unmodifiableMapField = getAccessibleField(processEnvironment,
        "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(key, value, unmodifiableMap);

    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    map.put(key, value);
  }

  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {

    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  private static void injectIntoUnmodifiableMap(String key, String value, Object map)
      throws ReflectiveOperationException {

    Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);
    ((Map<String, String>) obj).put(key, value);
  }

}