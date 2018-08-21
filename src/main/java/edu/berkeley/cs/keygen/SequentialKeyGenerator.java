package edu.berkeley.cs.keygen;

public class SequentialKeyGenerator implements KeyGenerator {
  private long currentKey;

  public SequentialKeyGenerator() {
    currentKey = 0;
  }

  @Override
  public String next() {
    return String.valueOf(currentKey++);
  }

  @Override
  public void reset() {
    currentKey = 0;
  }
}
