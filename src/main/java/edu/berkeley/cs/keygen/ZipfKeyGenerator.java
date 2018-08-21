package edu.berkeley.cs.keygen;

import static java.lang.Math.pow;

public class ZipfKeyGenerator implements KeyGenerator {

  private int n;
  private double[] zDistribution;

  public ZipfKeyGenerator(double theta, int n) {
    this.n = n;
    this.zDistribution = new double[this.n];

    /*
     * Zipfian - p(i) = c / i ^^ (1 - theta)
     * At theta = 1, uniform
     * At theta = 0, pure zipfian
     */

    double sum = 0.0;
    double exponent = 1.0 - theta;
    for (int i = 1; i <= this.n; i++) {
      sum += 1.0 / pow((double) i, exponent);
    }

    double c = 1.0 / sum;
    double sumCumulative = 0.0;
    for (int i = 0; i < this.n; i++) {
      sumCumulative += c / pow((double) (i + 1), exponent);
      this.zDistribution[i] = sumCumulative;
    }
  }

  @Override
  public String next() {
    double r = Math.random();
    int lo = 0;
    int hi = this.n;
    while (lo != hi) {
      int mid = (lo + hi) / 2;
      if (this.zDistribution[mid] <= r) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return String.valueOf(lo);
  }

  @Override
  public void reset() {
    // Do nothing
  }
}
