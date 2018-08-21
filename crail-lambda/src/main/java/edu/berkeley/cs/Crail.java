package edu.berkeley.cs;

import edu.berkeley.cs.BenchmarkService.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailResult;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;

class Crail {

  private CrailStore mStore;
  private CrailBuffer mBuffer;
  private String mBasePath;
  private int mObjectSize;

  private static CrailNodeType NODE_TYPE = CrailNodeType.DATAFILE;
  private static CrailStorageClass STORAGE_CLASS = CrailStorageClass.DEFAULT;
  private static CrailLocationClass LOCATION_CLASS = CrailLocationClass.DEFAULT;

  void init(Properties conf) throws Exception {
    CrailConfiguration cConf = new CrailConfiguration();
    mStore = CrailStore.newInstance(cConf);
    mObjectSize = Integer.parseInt(conf.getProperty("size", "1024"));
    mBasePath = conf.getProperty("path", "/test");

    System.out.println("path: " + mBasePath + ", size: " + mObjectSize);

    if (mObjectSize == CrailConstants.BUFFER_SIZE) {
      mBuffer = mStore.allocateBuffer();
    } else if (mObjectSize < CrailConstants.BUFFER_SIZE) {
      CrailBuffer _buf = mStore.allocateBuffer();
      _buf.clear().limit(mObjectSize);
      mBuffer = _buf.slice();
    } else {
      mBuffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(mObjectSize));
    }

    if (!createBasePath()) {
      System.err.println("Path already exists: " + mBasePath);
    }
  }

  void write(String key) {
    try {
      CrailFile f = createFile(mBasePath + "/" + key);
      mBuffer.clear();
      f.getDirectOutputStream(CrailConstants.BUFFER_SIZE).write(mBuffer).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  String read(String key) {
    try {
      CrailFile f = lookupFile(mBasePath + "/" + key);
      CrailInputStream is = f.getDirectInputStream(f.getCapacity());
      mBuffer.clear();
      Future<CrailResult> result = is.read(mBuffer);
      result.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return StandardCharsets.UTF_8.decode(mBuffer.getByteBuffer()).toString();
  }

  void destroy() throws Exception {
    mStore.freeBuffer(mBuffer);
    mStore.delete(mBasePath, true);
    mStore.getStatistics().print("close");
    mStore.close();
  }

  private boolean createBasePath() {
    try {
      mStore.create(mBasePath, CrailNodeType.DIRECTORY, STORAGE_CLASS, LOCATION_CLASS, true).get();
    } catch (Exception e) {
      if (e instanceof IOException && e.getMessage().startsWith("File already exists")) {
        return false;
      } else {
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  private CrailFile createFile(String key) throws Exception {
    return mStore.create(key, NODE_TYPE, STORAGE_CLASS, LOCATION_CLASS, true).get().asFile();
  }

  private CrailFile lookupFile(String key) throws Exception {
    return mStore.lookup(key).get().asFile();
  }
}
