Fluo Tour
---------

The git repository provides a barebones Maven+Java environment for the Fluo
Tour.  As you go through the Tour edit [Main.java](src/main/java/ft/Main.java)
and then use the following command to execute Main. This command will get all
of the correct dependencies on the classpath and execute Main.


```bash
mvn clean compile exec:java
```

The command takes a bit to run because it starts a MiniAccumulo and MiniFluo
each time.

