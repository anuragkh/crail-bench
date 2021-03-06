package edu.berkeley.cs.crail;

import edu.berkeley.cs.BenchmarkService;
import edu.berkeley.cs.keygen.KeyGenerator;
import edu.berkeley.cs.keygen.SequentialKeyGenerator;
import edu.berkeley.cs.keygen.ZipfKeyGenerator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class CrailBenchmarkService implements BenchmarkService {

  private static final int MAX_ERRORS = 1000;
  private static final int BENCHMARK_READ = 1;
  private static final int BENCHMARK_WRITE = 2;
  private static final int BENCHMARK_CREATE = 4;
  private static final int BENCHMARK_DESTROY = 8;
  private static final int BENCHMARK_LOAD = 16;
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

  public class Controller implements Closeable {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    Controller(String host, int port) throws IOException {
      this.socket = new Socket(host, port);
      this.out = new PrintWriter(socket.getOutputStream(), true);
      this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    boolean signal(String id) {
      write("LAMBDA_ID:" + id);
      try {
        String response = in.readLine();
        if (response.equalsIgnoreCase("ABORT")) {
          this.socket.shutdownInput();
          this.socket.shutdownOutput();
          this.socket.close();
          return false;
        } else {
          return response.equals("OK");
        }
      } catch (IOException e) {
        return false;
      }
    }

    private void write(String data) {
      this.out.write(data + "\n");
      this.out.flush();
    }

    @Override
    public void close() throws IOException {
      this.socket.shutdownInput();
      this.socket.shutdownOutput();
      this.socket.close();
    }
  }

  public interface ResultWriter extends Closeable {

    void writeResult(String fileName) throws IOException;
  }

  public class NetworkResultWriter implements ResultWriter {

    private static final String EOF = "::";
    private static final String EOC = "::::";

    private Socket socket;
    private PrintWriter out;

    NetworkResultWriter(String host, int port) throws IOException {
      this.socket = new Socket(host, port);
      this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    public void writeResult(String fileName) throws IOException {
      this.out.write(fileName + "\n");
      this.out.write(new String(Files.readAllBytes(Paths.get(fileName)), StandardCharsets.UTF_8));
      this.out.write(EOF + "\n");
      this.out.flush();
    }

    @Override
    public void close() {
      this.out.write(EOC + "\n");
      this.out.close();
    }
  }

  public class LocalResultWriter implements ResultWriter {

    LocalResultWriter() {
    }

    @Override
    public void writeResult(String fileName) {
      // Do nothing
    }

    @Override
    public void close() {
      // Do nothing
    }
  }

  public void handler(Map<String, String> conf) {

    Properties props = new Properties();
    props.putAll(conf);

    String distribution = conf.getOrDefault("distribution", "sequential");
    int size = Integer.parseInt(conf.getOrDefault("size", "1024"));
    int nOps = Integer.parseInt(conf.getOrDefault("num_ops", "1000"));
    KeyGenerator kGen;
    if (distribution.startsWith("zipf:")) {
      String[] parts = distribution.split(":");
      kGen = new ZipfKeyGenerator(Double.parseDouble(parts[2]), Integer.parseInt(parts[1]));
    } else if (distribution.equalsIgnoreCase("sequential")) {
      kGen = new SequentialKeyGenerator();
    } else {
      throw new RuntimeException("Unrecognized key distribution: " + distribution);
    }
    String modeStr = conf.getOrDefault("mode", "create_write_read_destroy");
    int mode = 0;
    if (modeStr.contains("read")) {
      mode |= BENCHMARK_READ;
    }
    if (modeStr.contains("write")) {
      mode |= BENCHMARK_WRITE;
    }
    if (modeStr.contains("create")) {
      mode |= BENCHMARK_CREATE;
    }
    if (modeStr.contains("destroy")) {
      mode |= BENCHMARK_DESTROY;
    }
    if (modeStr.contains("load")) {
      mode |= BENCHMARK_LOAD;
    }
    int numLoadThreads = Integer.parseInt(conf.getOrDefault("load_threads", "64"));
    boolean warmUp = Boolean.parseBoolean(conf.getOrDefault("warm_up", "true"));
    long timeoutUs = Long.parseLong(conf.getOrDefault("timeout", "240")) * 1000 * 1000;
    String host = conf.getOrDefault("host", "localhost");
    int logPort = Integer.parseInt(conf.getOrDefault("logger_port", "8888"));
    int controlPort = Integer.parseInt(conf.getOrDefault("control_port", "8889"));
    String id = conf.getOrDefault("lambda_id", "0");
    boolean local = Boolean.parseBoolean(conf.getOrDefault("local", "false"));

    Logger log;
    try {
      log = new Logger(host, logPort);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    Controller controller;
    try {
      controller = new Controller(host, controlPort);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    if (!controller.signal(id)) {
      return;
    }

    ResultWriter rw;
    try {
      if (local) {
        rw = new LocalResultWriter();
      } else {
        int resultPort = Integer.parseInt(conf.getOrDefault("result_port", "8890"));
        rw = new NetworkResultWriter(host, resultPort);
      }
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    Crail c = new Crail();
    try {
      benchmark(id, c, props, kGen, size, nOps, numLoadThreads, mode, warmUp, timeoutUs, log, rw);
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace(log.getPrintWriter());
      log.flush();
    }

    try {
      log.close();
      rw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void benchmark(String id, Crail c, Properties conf, KeyGenerator keyGen,
      int size, int nOps, int numLoadThreads, int mode, boolean warmUp, long maxUs, Logger log,
      ResultWriter rw) throws Exception {

    long startUs = nowUs();
    int errCount = 0;
    int warmUpCount = nOps / 10;
    String outPrefix = "/tmp/crail_" + id + "_" + String.valueOf(size);

    log.info("Running function ID=[" + id + "], num_ops=" + nOps + ", timeoutUs=" + maxUs);

    if (System.getenv(CRAIL_HOME) == null) {
      String crailHome = System.getenv(LAMBDA_TASK_ROOT);
      if (crailHome != null) {
        log.info("Setting environment variable CRAIL_HOME to " + crailHome);
        injectEnv(crailHome);
      } else {
        log.warn("CRAIL_HOME is not set, may not load appropriate configuration variables");
      }
    }

    log.info("Initializing storage interface...");
    c.init(conf, log, (mode & BENCHMARK_CREATE) == BENCHMARK_CREATE);

    if ((mode & BENCHMARK_LOAD) == BENCHMARK_LOAD) {
      log.info("Loading data...");
      c.load(nOps, numLoadThreads);
      log.info("Loading complete.");
    }

    if ((mode & BENCHMARK_WRITE) == BENCHMARK_WRITE) {
      BufferedWriter lw = new BufferedWriter(new FileWriter(outPrefix + "_write_latency.txt"));
      BufferedWriter tw = new BufferedWriter(new FileWriter(outPrefix + "_write_throughput.txt"));

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
        lw.append(String.valueOf(tEnd)).append("\t").append(String.valueOf(tEnd - tBegin)).append("\n");
      }
      long wEnd = nowUs();
      log.info("Finished writes.");

      double wElapsedS = ((double) (wEnd - wBegin)) / 1000000.0;
      tw.append(String.valueOf(nOps / wElapsedS)).append("\n");

      lw.close();
      rw.close();
      rw.writeResult(outPrefix + "_write_latency.txt");
      rw.writeResult(outPrefix + "_write_throughput.txt");
    }

    errCount = 0;
    keyGen.reset();
    if ((mode & BENCHMARK_READ) == BENCHMARK_READ) {
      BufferedWriter lr = new BufferedWriter(new FileWriter(outPrefix + "_read_latency.txt"));
      BufferedWriter tr = new BufferedWriter(new FileWriter(outPrefix + "_read_throughput.txt"));

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
        lr.append(String.valueOf(tEnd)).append("\t").append(String.valueOf(tEnd - tBegin)).append("\n");
      }
      long rEnd = nowUs();
      log.info("Finished reads.");

      double rElapsedS = ((double) (rEnd - rBegin)) / 1000000.0;
      tr.append(String.valueOf(nOps / rElapsedS)).append("\n");

      lr.close();
      tr.close();
      rw.writeResult(outPrefix + "_read_latency.txt");
      rw.writeResult(outPrefix + "_read_throughput.txt");
    }

    if ((mode & BENCHMARK_DESTROY) == BENCHMARK_DESTROY) {
      c.destroy();
      log.info("Destroyed storage interface.");
    }
  }

  private static void handleError(Logger log, int errCount, RuntimeException e) throws IOException {
    if (errCount > MAX_ERRORS) {
      log.error("Too many errors; last error:");
      e.printStackTrace(log.getPrintWriter());
      log.flush();
      log.close();
      System.exit(1);
    }
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

  private static void injectEnv(String value) throws Exception {
    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

    Field unmodifiableMapField = getAccessibleField(processEnvironment,
        "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(value, unmodifiableMap);

    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    map.put(CrailBenchmarkService.CRAIL_HOME, value);
  }

  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {

    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  private static void injectIntoUnmodifiableMap(String value, Object map)
      throws ReflectiveOperationException {
    Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);
    ((Map<String, String>) obj).put(CrailBenchmarkService.CRAIL_HOME, value);
  }

}
