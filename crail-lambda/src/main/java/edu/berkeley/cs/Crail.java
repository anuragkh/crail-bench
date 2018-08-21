package edu.berkeley.cs;

import edu.berkeley.cs.BenchmarkService.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;

class Crail implements Closeable {

  private CrailStore mStore;
  private CrailBuffer mBuffer;
  private String mBasePath;

  void init(Properties conf, Logger log) throws Exception {
    CrailConfiguration cConf = new CrailConfiguration();
    mStore = CrailStore.newInstance(cConf);
    int mObjectSize = Integer.parseInt(conf.getProperty("size", "1024"));
    mBasePath = conf.getProperty("path", "/test");

    log.info("Initializing Crail with path: " + mBasePath + ", size: " + mObjectSize);

    if (mObjectSize == CrailConstants.BUFFER_SIZE) {
      mBuffer = mStore.allocateBuffer();
    } else if (mObjectSize < CrailConstants.BUFFER_SIZE) {
      CrailBuffer _buf = mStore.allocateBuffer();
      _buf.clear().limit(mObjectSize);
      mBuffer = _buf.slice();
    } else {
      mBuffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(mObjectSize));
    }

    log.info("Buffer size: " + mBuffer.capacity());

    if (!createBasePath()) {
      log.warn("Path already exists: " + mBasePath);
    } else {
      log.info("Path created: " + mBasePath);
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
      throw new RuntimeException(e);
    }
  }

  String read(String key) {
    try {
      CrailFile f = lookupFile(mBasePath + "/" + key);
      CrailInputStream is = f.getDirectInputStream(f.getCapacity());
      mBuffer.clear();
      is.read(mBuffer).get().getLen();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return StandardCharsets.UTF_8.decode(mBuffer.getByteBuffer()).toString();
  }

  void destroy() throws Exception {
    mStore.freeBuffer(mBuffer);
    mStore.delete(mBasePath, true);
    mStore.getStatistics().print("close");
  }

  private boolean createBasePath() {
    try {
      mStore.create(mBasePath, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT, true).get();
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private CrailFile createFile(String key) throws Exception {
    return mStore
        .create(key, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.PARENT,
            true).get().asFile();
  }

  private CrailFile lookupFile(String key) throws Exception {
    return mStore.lookup(key).get().asFile();
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
