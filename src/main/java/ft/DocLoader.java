package ft;

import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;

public class DocLoader implements Loader {

  private final Document doc;

  public static final Column HASH_COL = new Column("uri", "hash");
  public static final Column REF_COUNT_COL = new Column("doc", "refc");
  public static final Column REF_STATUS_COL = new Column("doc", "refs");
  public static final Column CONTENT_COL = new Column("doc", "content");

  public DocLoader(Document doc) {
    this.doc = doc;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {
    String newHash = doc.hash();
    String oldHash = tx.gets("u:" + doc.uri, HASH_COL);

    // check if uri already has the same content hash
    if (newHash.equals(oldHash))
      return;

    tx.set("u:" + doc.uri, HASH_COL, newHash);

    if (oldHash != null) {
      // TODO decrement the reference count at row "d:"+oldHash 
      // TODO set REF_STATUS_COL to "unreferenced" when the reference count goes from 1 to 0
      int oldRefCount = Integer.parseInt(tx.gets("d:" + oldHash, REF_COUNT_COL));
      if (oldRefCount == 1) {
        tx.set("d:" + oldHash, REF_STATUS_COL, "unreferenced");
      }
      tx.set("d:" + oldHash, REF_COUNT_COL, (oldRefCount - 1) + "");
    }

    String newRefCountString = tx.gets("d:" + newHash, REF_COUNT_COL);
    int newRefCount;
    if (newRefCountString == null) {
      // In this case the content does not exists in the system.
      tx.set("d:" + newHash, CONTENT_COL, doc.content);
      newRefCount = 0;
    } else {
      newRefCount = Integer.parseInt(newRefCountString);
    }

    //TODO set REF_STATUS_COL to "referenced" when the reference count goes from 0 to 1
    
    if (newRefCount == 0) {
      // the content is transitioning from unreferenced to referenced
      tx.set("d:" + newHash, REF_STATUS_COL, "referenced");
    }

    //increment the reference count
    tx.set("d:" + newHash, REF_COUNT_COL, (newRefCount + 1) + "");

  }
}
