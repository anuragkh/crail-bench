package edu.berkeley.cs.keygen;

public interface KeyGenerator {
  String next();
  void reset();
}
