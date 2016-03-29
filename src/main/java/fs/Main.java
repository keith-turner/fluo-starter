package fs;

import java.io.File;
import java.nio.file.Files;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;

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

  }
}