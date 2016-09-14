package ft;

import java.io.File;
import java.nio.file.Files;

// Normaly using * with imports is a bad practice, however in this case it makes experimenting with
// Fluo easier.
import org.apache.fluo.api.client.*;
import org.apache.fluo.api.config.*;
import org.apache.fluo.api.data.*;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.observer.*;

public class Main {
  public static void main(String[] args) throws Exception {

    String tmpDir = Files.createTempDirectory(new File("target").toPath(), "mini").toString();
    // System.out.println("tmp dir : "+tmpDir);

    FluoConfiguration fluoConfig = new FluoConfiguration();
    fluoConfig.setApplicationName("class");
    fluoConfig.setMiniDataDir(tmpDir);

    preInit(fluoConfig);

    System.out.print("Starting MiniFluo ... ");

    try (MiniFluo mini = FluoFactory.newMiniFluo(fluoConfig);
        FluoClient client = FluoFactory.newClient(mini.getClientConfiguration())) {

      System.out.println("started.");

      excercise(mini, client);
    }
  }

  private static void preInit(FluoConfiguration fluoConfig) {
    fluoConfig.addObserver(new ObserverConfiguration(ContentObserver.class.getName()));
  }


  // some test data
  private static Document[] docs1 = new Document[] {
      new Document("http://news.com/a23",
          "Jebediah orbits Mun for 35 days.  No power, forgot solar panels."),
      new Document("http://news.com/a24",
          "Bill plans to rescue Jebediah after taking tourist to Minimus.")};

  private static Document[] docs2 = new Document[] {new Document("http://oldnews.com/a23",
      "Jebediah orbits Mun for 35 days.  No power, forgot solar panels.")};

  private static Document[] docs3 = new Document[] {
      new Document("http://news.com/a23",
          "Jebediah orbits Mun for 38 days.  No power, forgot solar panels."),
      new Document("http://news.com/a24",
          "Crisis at KSC.  Tourist stuck at Minimus.  Bill forgot solar panels.")};

  /**
   * Utility method for loading documents and printing out Fluo table after load completes.
   */
  private static void loadAndPrint(MiniFluo mini, FluoClient client, Document[] docs) {

    try (LoaderExecutor loaderExecutor = client.newLoaderExecutor()) {
      for (Document document : docs) {
        loaderExecutor.execute(new DocLoader(document));
      }
    } // this will close loaderExecutor and wait for all load transactions to complete

    mini.waitForObservers();

    System.out.println("**** begin table dump ****");
    try (Snapshot snap = client.newSnapshot()) {
      snap.scanner().build().forEach(rcv -> System.out.println("  " + rcv));
    }
    System.out.println("**** end table dump ****\n");
  }

  private static void excercise(MiniFluo mini, FluoClient client) {
    loadAndPrint(mini, client, docs1);
    loadAndPrint(mini, client, docs2);
    loadAndPrint(mini, client, docs3);
  }
}
