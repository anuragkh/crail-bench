package edu.berkeley.cs.crail;

import edu.berkeley.cs.crail.CrailBenchmarkService.Logger;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailResult;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.Upcoming;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;

class Crail implements Closeable {

  private CrailStore mStore;
  private CrailBuffer mBuffer;
  private String mBasePath;

  private static final String DEFAULT_NAMENODE = "crail://localhost:9060";
  private static final String DEFAULT_STORAGE = "org.apache.crail.storage.tcp.TcpStorageTier";
  private static final String DEFAULT_BLOCKSIZE = "4096";
  private static final String DEFAULT_BUFFERSIZE = "4096";
  private static final String DEFAULT_RPC = "org.apache.crail.namenode.rpc.tcp.TcpNameNode";
  private static final String DEFAULT_CACHEPATH = "/tmp/cache";
  private static final String DEFAULT_CACHELIMIT = "268435456";

  void init(Properties conf, Logger log, boolean create) throws Exception {
    CrailConfiguration c = new CrailConfiguration();
    c.set("crail.namenode.address", conf.getProperty("namenode_address", DEFAULT_NAMENODE));
    c.set("crail.storage.types", conf.getProperty("storage_mode", DEFAULT_STORAGE));
    c.set("crail.blocksize", conf.getProperty("block_size", DEFAULT_BLOCKSIZE));
    c.set("crail.buffersize", conf.getProperty("buffer_size", DEFAULT_BUFFERSIZE));
    c.set("crail.namenode.rpctype", conf.getProperty("rpc_type", DEFAULT_RPC));
    c.set("crail.cachepath", conf.getProperty("cache_path", DEFAULT_CACHEPATH));
    c.set("crail.cachelimit", conf.getProperty("cache_limit", DEFAULT_CACHELIMIT));
    mStore = CrailStore.newInstance(c);
    int mObjectSize = Integer.parseInt(conf.getProperty("size", "1024"));
    mBasePath = conf.getProperty("path", "/test");

    if (mObjectSize == CrailConstants.BUFFER_SIZE) {
      mBuffer = mStore.allocateBuffer();
    } else if (mObjectSize < CrailConstants.BUFFER_SIZE) {
      CrailBuffer _buf = mStore.allocateBuffer();
      _buf.clear().limit(mObjectSize);
      mBuffer = _buf.slice();
    } else {
      mBuffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(mObjectSize));
    }

    if (create) {
      createBasePath();
      log.info("Path created: " + mBasePath);
    }
  }

  void load(int numKeys, int batchSize, Logger logger) throws Exception {
    int numBatches = numKeys / batchSize;
    for (int b = 0; b < numBatches; b++) {
      // Create all files first
      ArrayList<Upcoming<CrailNode>> fileHandles = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        fileHandles.add(createFileAsync(mBasePath + "/" + String.valueOf(i)));
      }

      ArrayList<Future<CrailResult>> writeResults = new ArrayList<>();
      ArrayList<CrailOutputStream> writeStreams = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        mBuffer.clear();
        CrailFile f = fileHandles.get(i).get().asFile();
        CrailOutputStream out = f.getDirectOutputStream(Integer.MAX_VALUE);
        writeResults.add(out.write(mBuffer));
        writeStreams.add(out);
      }

      for (int i = 0; i < batchSize; i++) {
        writeResults.get(i).get().getLen();
        writeStreams.get(i).close();
      }
      logger.info("Finished batch " + b);
    }
  }

  void write(String key) {
    try {
      CrailFile f = createFile(mBasePath + "/" + key);
      mBuffer.clear();
      CrailOutputStream out = f.getDirectOutputStream(Integer.MAX_VALUE);
      out.write(mBuffer).get().getLen();
      out.close();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  String read(String key) {
    try {
      CrailFile f = lookupFile(mBasePath + "/" + key);
      CrailInputStream is = f.getDirectInputStream(f.getCapacity());
      mBuffer.clear();
      is.read(mBuffer).get().getLen();
      is.close();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return StandardCharsets.UTF_8.decode(mBuffer.getByteBuffer()).toString();
  }

  void destroy() throws Exception {
    mStore.freeBuffer(mBuffer);
    mStore.delete(mBasePath, true);
    mStore.getStatistics().print("close");
  }

  private void createBasePath() throws Exception {
    mStore.create(mBasePath, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, true).get();
  }

  private CrailFile createFile(String key) throws Exception {
    return createFileAsync(key).get().asFile();
  }

  private Upcoming<CrailNode> createFileAsync(String key) throws Exception {
    return mStore
        .create(key, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.PARENT,
            true);
  }

  private CrailFile lookupFile(String key) throws FileNotFoundException {
    try {
      return mStore.lookup(key).get().asFile();
    } catch (Exception e) {
      throw new FileNotFoundException("File does not exist: " + key);
    }
  }

  @Override
  public void close() throws IOException {
    if (mStore != null) {
      try {
        mStore.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
