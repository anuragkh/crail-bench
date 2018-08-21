package edu.berkeley.cs;

import com.amazonaws.services.lambda.invoke.LambdaFunction;
import java.util.Map;

public interface BenchmarkService {
  @LambdaFunction(functionName = "CrailBenchmark")
  void handler(Map<String, String> conf);
}
