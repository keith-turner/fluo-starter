package ft;

import com.google.common.hash.Hashing;

public class Document {
  public final String uri;
  public final String content;

  public Document(String uri, String content) {
    this.uri = uri;
    this.content = content;
  }

  public String hash() {
    //use short prefix of hash for example
    return Hashing.sha1().hashString(content).toString().substring(0, 7);
  }
}
