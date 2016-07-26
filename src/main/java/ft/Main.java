package ft;

import java.io.File;
import java.nio.file.Files;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.mini.MiniFluo;

public class Main {
  public static void main(String[] args) throws Exception {

    String tmpDir = Files.createTempDirectory(new File("target").toPath(), "mini").toString();
    //System.out.println("tmp dir : "+tmpDir);

    FluoConfiguration fluoConfig = new FluoConfiguration();
    fluoConfig.setApplicationName("class");
    fluoConfig.setMiniDataDir(tmpDir);

    try(MiniFluo mini = FluoFactory.newMiniFluo(fluoConfig); FluoClient client = FluoFactory.newClient(mini.getClientConfiguration())){
      experiment(mini, client);
    }
  }

  private static void experiment(MiniFluo mini, FluoClient client) {
    //experiment with Fluo here
  }
}
