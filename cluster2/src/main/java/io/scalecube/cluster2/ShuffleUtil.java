package io.scalecube.cluster2;

import java.util.List;
import java.util.Random;

public class ShuffleUtil {

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void shuffle(List list, Random random) {
    for (int i = 0, n = list.size(); i < n; i++) {
      final Object current = list.get(i);
      final int k = random.nextInt(n);
      final Object obj = list.get(k);
      if (i != k) {
        list.set(i, obj);
        list.set(k, current);
      }
    }
  }
}
