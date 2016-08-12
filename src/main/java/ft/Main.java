package ft;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

// Normaly using * with imports is a bad practice, however in this case it makes experimenting with
// Fluo easier.
import org.apache.fluo.api.client.*;
import org.apache.fluo.api.config.*;
import org.apache.fluo.api.data.*;
import org.apache.fluo.api.mini.MiniFluo;
import ft.logic.NewEdgeObserver;
import ft.logic.TagObserver;
import ft.logic.TweetLoader;
import ft.model.Tweet;
import ft.persistence.ClusterCoefficientIndex;

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

  static final Tweet[] tweets1 = new Tweet[] {Tweet.newTweet("@user1").withTags("#tag1").build(),
      Tweet.newTweet("@user1").withTags("#tag2").build(),
      Tweet.newTweet("@user1").withTags("#tag3").withMentions("@user2").build(),
      Tweet.newTweet("@user2").withTags("#tag1").build(),
      Tweet.newTweet("@user3").withTags("#tag1").build(),
      Tweet.newTweet("@user4").withMentions("@user1", "@user6").build(),
      Tweet.newTweet("@user4").withTags("#tag2").build(),
      Tweet.newTweet("@user5").withTags("#tag2").withMentions("@user1").build(),
      Tweet.newTweet("@user6").withTags("#tag2", "#tag3").withMentions("@user7").build()};

  static final Tweet[] tweets2 =
      new Tweet[] {Tweet.newTweet("@user1").withTags("#tag1", "#tag2").build(),
          Tweet.newTweet("@user1").withMentions("@user5", "@user7").build(),
          Tweet.newTweet("@user3").withMentions("@user7").build()};

  static final Tweet[] tweets3 =
      new Tweet[] {Tweet.newTweet("@user7").withTags("#tag1", "#tag3").build(),
          Tweet.newTweet("@user1").withMentions("@user6").build()};

  static final Tweet[] tweets4 = new Tweet[] {Tweet.newTweet("@user1").withTags("#tag4").build(),
      Tweet.newTweet("@user2").withTags("#tag4").withMentions("@user3").build(),
      Tweet.newTweet("@user3").withTags("#tag4").build(),
      Tweet.newTweet("@user4").withTags("#tag4").build(),
      Tweet.newTweet("@user5").withTags("#tag4").build(),
      Tweet.newTweet("@user6").withTags("#tag4").build(),
      Tweet.newTweet("@user7").withTags("#tag4").build()};

  private static void preInit(FluoConfiguration fluoConfig) {
    fluoConfig.addObserver(new ObserverConfiguration(NewEdgeObserver.class.getName()));
    fluoConfig.addObserver(new ObserverConfiguration(TagObserver.class.getName()));
  }

  private static void load(FluoClient client, Tweet... tweets) {
    try (LoaderExecutor loader = client.newLoaderExecutor()) {
      for (Tweet tweet : tweets) {
        // TODO create TweetLoader
        loader.execute(new TweetLoader(tweet));
      }
    }
  }

  private static void print(FluoClient client) {
    System.out.println();
    try (Snapshot snap = client.newSnapshot()) {
      // TODO print out cluster coefficients in descending order
      ClusterCoefficientIndex.printIndex(snap);
    }
  }

  private static void excercise(MiniFluo mini, FluoClient client) {

    load(client, tweets1);
    mini.waitForObservers();
    print(client);

    // should not change anything
    load(client, tweets2);
    mini.waitForObservers();
    print(client);

    load(client, tweets3);
    mini.waitForObservers();
    print(client);

    load(client, tweets4);
    mini.waitForObservers();
    print(client);
  }
}
