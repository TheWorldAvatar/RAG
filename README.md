# Local deployment for development purposes

The RAG systems consist of a backend and a frontend. The backend contains the core question-answering functionality, whereas the frontend provides a graphical interface in the form of a webpage where a user can enter questions. While the frontend requires the backend in order to function, the backend can run on its own.

Prerequisites are Docker and Python. The use of Visual Studio Code is recommended, but not essential.

The workflow described in this section is suitable for development purposes only. It is _not_ suitable for production deployments.

## Backend

### First-time set-up

Create a virtual environment:
```
python3 -m venv <venv_name>
```

Activate the virtual environment, e.g. on Windows:
```
cd <venv_name>\Scripts
activate.bat
```

Install the required dependencies:
```
pip install -r requirements.txt
```

Create a configuration file by copying `config-template.yaml` and renaming the copy to a name of your choice. Then customise the contents of the new file according to your own needs. Whichever RAG system you are subsequently running, make sure it reads the correct configuration file!

### Spin up the graph database

Amongst many possible ways, it is recommended to achieve this using the [stack manager](https://github.com/TheWorldAvatar/stack/tree/main/stack-manager), as part of a [The World Avatar (TWA)](https://theworldavatar.io/) container stack:

1) Clone the [stack](https://github.com/TheWorldAvatar/stack) repository.
2) Populate the `stack-manager/inputs/secrets` folder in the stack repository with `postgis_password` and `geoserver_password` files.
3) Start a default stack, i.e. one without any configuration file, with a name of your choice using the stack manager as described, e.g. via `sudo ./stack.sh start <stack_name>`.

### Data preparation

If you already have TBox OWL and ABox TTL files available, you can skip the download, generation, and instantiation steps and proceed directly to the upload steps below.

#### Download raw data

Raw data can be downloaded from a publicly accessible API by uncommenting the relevant sections from the main part of `dipapi.py` as desired, and running the file.

#### Generate TBox

TBoxes can be generated in CSV format from the downloaded raw data files by uncommenting the relevant sections from the main part of `dipapi.py` as desired, and running the file.

#### Convert TBox CSV into OWL file

Any TBox CSV file generated in the previous step can be converted into OWL format using an existing generic [converter](https://github.com/TheWorldAvatar/baselib/tree/main/src/main/java/uk/ac/cam/cares/jps/base/converter) as follows:

Clone the [TWA base lib](https://github.com/TheWorldAvatar/baselib) repository, navigate to its root folder, and run:
```
mvn clean install -DskipTests
```
Then, from within the `target/` folder, run:
```
java -cp jps-base-lib.jar uk.ac.cam.cares.jps.base.converter.TBoxGeneration <path/to/tbox.csv>
```
Note that the above commands require sufficiently recent installations of a Java Development Kit (JDK) and Maven, with sufficiently privileged GitHub credentials in place.

#### Instantiate the data

The downloaded raw data files can be instantiated as ABox TTL files by uncommenting the relevant sections from the main part of `instantiation.py` as desired, and running the file.

#### Upload the TBox into the graph database

Assuming the graph database is up and running, the TBox OWL file can be uploaded manually as follows:

1) Open a web-browser and navigate to the page [http://localhost:3838/blazegraph/ui/](http://localhost:3838/blazegraph/ui/) (by default).
2) Create a new namespace and make sure that its name is consistent with the TBox endpoint URL specified in your configuration file. Once created, make sure to click the `Use` link next to the newly created namespace.
3) On the `Update` tab, select the TBox OWL file, and press the `Update` button. There is no need to change the file type, as this is determined automatically when the file is selected.

#### Upload the ABox into the graph database

The ABox TTL file is typically too large to be uploaded through the user-interface of the graph database. The recommended way to upload the file is via the [stack data uploader](https://github.com/TheWorldAvatar/stack/tree/main/stack-data-uploader) as follows:

1) Copy the `json` configuration file from the `stack/data-uploader` folder in this repository into the `stack-data-uploader/inputs/config` folder in the stack repository.
2) Copy the ABox TTL file into the folder `stack-data-uploader/inputs/data/debates/complete` (which may need to be created) in the stack repository.
3) Run the stack data uploader as described, e.g. via `sudo ./stack.sh start <stack_name>`.

Depending on the size of the file, the upload process can take _several_ minutes.

#### Embeddings and vector store caches (optional)

If you have cached embeddings and/or a vector store cache available, then copy the relevant folders into the root folder of this repository, and make sure the names of the folders are consistent with what is specified in your configuration file.

#### Running the backend stand-alone

The backend can be executed stand-alone, i.e. without using any frontend or user-interface, by uncommenting the relevant sections from the main part of (e.g.) `hybridrag.py` as desired, and running the file.

By default, this will read a plain-text question from a pre-defined catalogue, generate an answer, and print the answer to the console, in addition to recording the answer in the catalogue. Extensive logging of the internal workings of the RAG system is also available.

It is also possible, by uncommenting the relevant sections from the main part of the code, to query speech texts from the knowledge graph, embed them, and store the results in a vector store. This step is essential for the functioning of the RAG system and must be carried out prior to its first use, unless cached embeddings and/or a vector store cache are available. WARNING: Calculating embeddings can cost real money (depending on your chosen model) and can become expensive for large quantities of information!

## Frontend

In order to host the frontend for local, non-containerised development, run the `fastapi` development server by issuing the following command in an activated virtual environment:
```
fastapi dev app.py
```
Make sure that `app.py` reads the correct configuration file prior to starting the server.

Once the server has started successfully, the frontend is then accessible by opening a web-browser and navigating to [http://localhost:8000/](http://localhost:8000/) (by default).

Also note the auto-generated [http://localhost:8000/docs/](http://localhost:8000/docs/) (Swagger UI) and [http://localhost:8000/redoc/](http://localhost:8000/redoc/) (ReDoc) routes.

# Miscellaneous

## Validating ABox TTL files against TBox OWL files (optional)

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

Independently or in addition, you may also want to use `graphanalysis.py`.
