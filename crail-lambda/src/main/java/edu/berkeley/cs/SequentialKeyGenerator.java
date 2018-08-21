package edu.berkeley.cs;

public class SequentialKeyGenerator implements KeyGenerator {
  private long currentKey;

  SequentialKeyGenerator() {
    currentKey = 0;
  }

  @Override
  public String next() {
    return String.valueOf(currentKey++);
  }
}
