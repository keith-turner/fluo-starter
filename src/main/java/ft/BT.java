package ft;

import java.util.HashSet;
import java.util.TreeSet;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;

public class BT {
  public static void main(String[] args) {
    BytesBuilder bb = Bytes.builder();

    bb.append("kerbalnaut:");
    int prefixLen = bb.getLength();

    HashSet<Bytes> hashset = new HashSet<>();
    TreeSet<Bytes> treeset = new TreeSet<>();

    for (int i = 1; i <= 20; i++) {
      bb.setLength(prefixLen);
      bb.append(String.format("%04d", i));
      Bytes bytes = bb.toBytes();
      hashset.add(bytes);
      treeset.add(bytes);
    }

    System.out.println(hashset.size());
    System.out.println(treeset.size());
  }
}
