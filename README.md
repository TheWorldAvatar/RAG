### Generating OWL files from TBox CSV files

Build the base lib of TWA by navigating to the `/TheWorldAvatar/JPS_BASE_LIB/` folder and running:
```
mvn clean install -DskipTests
```
Then, from within the `target/` folder, run:
```
java -cp jps-base-lib.jar uk.ac.cam.cares.jps.base.converter.TBoxGeneration <path/to/tbox.csv>
```
