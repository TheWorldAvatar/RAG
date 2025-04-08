### Generating OWL files from TBox CSV files

Build the base lib of TWA by navigating to the `/TheWorldAvatar/JPS_BASE_LIB/` folder and running:
```
mvn clean install -DskipTests
```
Then, from within the `target/` folder, run:
```
java -cp jps-base-lib.jar uk.ac.cam.cares.jps.base.converter.TBoxGeneration <path/to/tbox.csv>
```
### Validating ABox TTL files against TBox OWL files

1) Open Protégé.
2) Open the ABox TTL file.
3) On the "Ontology imports" tab, click the 'plus' icon next to "Direct Imports". Import the TBox OWL file.
4) Save as, in Turtle syntax, a new TTL file.
5) Close Protégé.
6) Open the new TTL file in any text editor.
7) Delete all the annotation property statements at the beginning of the file (but not the import statement!). Save the changes.
8) Open Protégé.
9) Open the edited TTL file.
10) Click on the bottom right icon in the status bar in order to view the log. Any errors will be shown there.

### Local development

In order to host the front-end for local, non-containerised development, run the `fastapi` development server by issuing the following command in an activated virtual environment:
```
fastapi dev app.py
```
