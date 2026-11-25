# SWE 307 Big Data Project 3

## 1. What this project does

This project is a small end-to-end expense data pipeline. It simulates a Point
of Sale (POS) device, routes each expense through Kafka, processes the stream
with Java Spark, stores expenses in Cassandra, and displays employee and
expense information in a Spring Boot web application.

```text
emp.csv + dept.csv
       │
       ├──> MySQL ───────────────────────────┐
       │                                     │ employee details
       └──> POS department lookup            ▼
             │                        Spring Boot web app ──> Browser
             ▼                               ▲
       Kafka department topics               │ totals / records
             │                               │
             ▼                               │
       Java Spark stream ──> Cassandra ──> Java Spark query

Employee photo ──> HDFS ──> Spring Boot image endpoint ──> Browser
```

The project has two ways to produce Kafka expense records:

1. **Live simulation:** `POSserver` generates a new expense every second.
2. **Provided sample data:** `ExpenseCsvImporter` reads `expense.csv` and
   publishes its rows to Kafka once.

Use the live simulator for the continuous demo. Use the CSV importer when you
want a repeatable sample dataset. Do not run multiple Spark streaming consumers
at the same time: they must not share the same checkpoint.

## 2. Assignment requirements and implementation

| Requirement | Where it is implemented | How to demonstrate it |
|---|---|---|
| An expense has employee ID, date/time, description, type, count and payment | `POSserver/.../Main.java`; `expense.csv`; `KafkaToCassandraStream.java` | Show a POS line, a Kafka message or a Cassandra row with all six fields. |
| POS produces one record per second | `POSserver/.../Main.java` | The POS terminal prints one record about every second. |
| Four department Kafka topics | `POSserver/.../Topic.java`; Kafka setup below | Run `kafka-topics.sh --bootstrap-server localhost:9092 --list`. |
| Route a record to the employee's department | `Topic.java`; employee/department CSV lookup in `ExpenseCsvImporter` | Compare `empno` and `deptno` in the CSVs with the topic name printed by the producer. |
| Spark consumes Kafka and stores records in Cassandra | `spark-query/.../KafkaToCassandraStream.java` | See `Stored Kafka batch ...` in Spark's terminal, then query `project3.expenses`. |
| Calculate a user's cumulative expense with Spark | `webapp/.../SparkService.java` | Open `/total/7788`, or search for SCOTT on the home page. |
| Read employee and department information from MySQL | `EmployeeController.java`; `scripts/mysql-schema.sql` | Run the MySQL count query below and view employee rows on the web page. |
| Store and fetch employee photos from HDFS | `UploadController.java`; `ImageController.java` | Upload a JPG, check `/images` in HDFS, and open `/img/scott.jpg`. |
| Display the requested employee report | `webapp/src/main/resources/templates/index.html` | The home page shows photo, name, manager, salary, commission, department and expense total. |
| Run without Docker | Local-service setup in this guide | Run the services directly on the host computer. |

### Important Spark deployment detail

The Java streaming app and the web app currently configure Spark with
`.master("local[*]")`. This runs Spark on the same computer, using its local
cores. It uses Spark for stream processing and for Cassandra queries, but it
does **not** submit these jobs to a separately started Spark Standalone master
at `spark://127.0.0.1:7077`.

The assignment asks for a single-node Spark installation. If the instructor
requires proof that the Standalone cluster itself executes the jobs (rather
than accepting Spark local mode), the application's Spark master configuration
must be changed to the cluster URL and tested on that installation. Do not
present `jps` showing Spark daemons alone as proof that this application's jobs
ran on that cluster.

`scripts/streaming.scala` is an older Spark-shell example. It is an alternative
stream consumer, not an additional service to run alongside
`KafkaToCassandraStream`. The commands in the older notes use Scala 2.12 and
Spark 3.4.1 packages, while the supported Gradle Java app uses Scala 2.13 and
Spark 3.4.0. Use the Java app and its Gradle dependencies unless you deliberately
align and test the Spark-shell dependency versions.

## 3. Project layout

```text
p3-bigdata/
├── build.gradle                    Shared Gradle settings (Java 11)
├── settings.gradle                 Declares the three Gradle modules
├── gradlew                         Gradle wrapper; use this instead of installing Gradle
├── emp.csv                         Employee source data (14 employees)
├── dept.csv                        Department source data (4 departments)
├── expense.csv                     Sample expense input (176 data records)
├── README.md                       This guide
├── POSserver/                      Maven Java POS producer and CSV importer
│   ├── pom.xml
│   └── src/main/java/com/sau/
│       ├── Main.java               Live one-record-per-second producer
│       ├── Topic.java              Department number -> Kafka topic name
│       └── ExpenseCsvImporter.java Publishes expense.csv rows to Kafka
├── generator/                      Alternative Gradle Kafka producer
│   ├── build.gradle
│   └── src/main/java/com/sau/generator/Generator.java
├── spark-query/                    Java Spark applications for streaming/query
│   ├── build.gradle
│   └── src/main/java/com/sau/
│       ├── KafkaToCassandraStream.java  Main continuous Kafka -> Cassandra job
│       └── Main.java                    One-shot Cassandra query example
├── webapp/                         Spring Boot MVC, MySQL, Spark and HDFS
│   ├── build.gradle
│   └── src/main/
│       ├── java/com/sau/
│       │   ├── WebApplication.java
│       │   ├── EmployeeController.java  Home page, search and total endpoint
│       │   ├── SparkService.java         Reads/aggregates Cassandra with Spark
│       │   ├── ImageController.java     Reads employee photos from HDFS
│       │   └── UploadController.java    Uploads JPG photos to HDFS
│       └── resources/
│           ├── application.properties  Web port and MySQL connection
│           └── templates/index.html    Employee report page
└── scripts/
    ├── mysql-schema.sql            Creates tables and imports emp.csv/dept.csv
    ├── cassandra-schema.cql        Creates project3.expenses
    └── streaming.scala             Legacy/alternative Spark-shell example
```

The repository root is a Gradle multi-project build. `generator`, `spark-query`
and `webapp` are Gradle subprojects. `POSserver` is its own Maven project and
is not included in `settings.gradle`. The root `src/` directory is not one of
the application modules.

## 4. Data files and database schemas

### `emp.csv`

The employee file has these columns:

```text
empno,ename,job,mgr,hiredate,sal,comm,deptno,img
```

`empno` is the employee ID used as the POS `user_id`. `deptno` determines the
employee's Kafka topic. Blank `mgr` and `comm` cells are imported as SQL `NULL`.
The dates use `DD-MMM-YYYY` (for example `17-DEC-1980`) and the MySQL import
converts them to SQL dates.

### `dept.csv`

```text
deptno,dname,loc
```

It contains ACCOUNTING (10), RESEARCH (20), SALES (30), and OPERATIONS (40).
The live POS maps the first three departments from the supplied employee list.
No supplied employee belongs to department 40, so it is normal for the
`operations` topic to exist but receive no live messages.

### `expense.csv`

The provided expense file has a header and 176 data rows:

```text
empno, date_time, description, type, count, payment
```

The importer maps `empno` to `user_id` and sends each row to the topic determined
by that employee's department. It preserves the sample date/time and payment.
These sample timestamps are from 2025; live POS records use the current time.
Do not expect imported sample timestamps to look like today's live records.

### MySQL

The web app reads the `project3.emp` and `project3.dept` tables. The employee
page joins employee records to the manager (`emp.mgr`) and department
(`emp.deptno`) so the browser can display the manager's name and department.
`scripts/mysql-schema.sql` creates the tables and imports the provided employee
and department CSV files.

### Cassandra

`scripts/cassandra-schema.cql` creates:

```text
project3.expenses(
  user_id int,
  date_time timestamp,
  description text,
  type text,
  count int,
  payment decimal,
  PRIMARY KEY (user_id, date_time)
)
```

The partition key is `user_id`, so a user's expense rows are grouped together.
`date_time` is the clustering key and orders that user's rows by time. Cassandra
identifies a row by `(user_id, date_time)`; writing the same key again replaces
that row rather than creating a second copy.

## 5. Software and local services

This guide is written for the macOS setup used during development. Install
services locally; **Docker/container technology is not used**.

Required software:

- Java 11 (the Gradle and Maven Java projects target Java 11)
- Git, the included Gradle wrapper, and Maven
- Kafka with ZooKeeper, or a Kafka installation configured for its own metadata
  mode
- Cassandra
- MySQL
- Hadoop HDFS
- Spark 3.4.0 for the Gradle application

The Gradle Spark module uses Spark 3.4.0 with Scala 2.13 and Cassandra
Connector 3.4.1. The POS Maven module uses Kafka client 3.8.0. Use compatible
installed services and keep network ports/configuration consistent with this
guide.

Check Java and project build tools from the project root:

```sh
java -version
mvn -version
./gradlew --version
```

The application defaults are:

| Service | Default address/configuration |
|---|---|
| Kafka | `localhost:9092` |
| ZooKeeper (if this Kafka installation uses it) | `localhost:2181` |
| Cassandra native transport | `127.0.0.1:9042` |
| MySQL | `localhost:3306`, database `project3`, user `root` |
| HDFS NameNode | `hdfs://localhost:9000` |
| Web application | `http://localhost:8080` |
| Spark Structured Streaming checkpoint | `~/.p3-bigdata/spark-checkpoint` |

If your service uses another address, update the application configuration or
set the environment variables described in the configuration section.

## 6. Start the local services

Run commands from the correct installation directory if the commands are not
on your `PATH`. The following sequence follows the ZooKeeper-based Kafka setup
in the development notes. If your Kafka distribution uses KRaft, follow that
distribution's startup instructions instead of starting ZooKeeper.

### 6.1 Start HDFS

```sh
start-dfs.sh
jps
```

For a healthy single-node HDFS setup, look for `NameNode`, `DataNode`, and
`SecondaryNameNode`. Confirm HDFS responds:

```sh
hdfs dfs -ls /
```

The web app reads images from `hdfs://localhost:9000/images`. The `/images`
directory can be created with:

```sh
hdfs dfs -mkdir -p /images
```

### 6.2 Start Cassandra

In its own terminal:

```sh
cassandra -f
```

Keep that terminal open and wait for Cassandra to finish starting. In another
terminal, check it:

```sh
nodetool status
cqlsh localhost 9042 -e "DESCRIBE KEYSPACES"
```

For a local single-node cluster, `nodetool status` should show the local node
as `UN` (Up/Normal).

### 6.3 Start Kafka and ZooKeeper

Set `KAFKA_HOME` to your Kafka installation path. Example:

```sh
export KAFKA_HOME=/usr/local/kafka
```

For ZooKeeper-based Kafka, start each process in a separate terminal and leave
both running:

```sh
"$KAFKA_HOME/bin/zookeeper-server-start.sh" \
  "$KAFKA_HOME/config/zookeeper.properties"
```

```sh
"$KAFKA_HOME/bin/kafka-server-start.sh" \
  "$KAFKA_HOME/config/server.properties"
```

Confirm Kafka responds:

```sh
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list
```

Create the required lowercase topics. `--if-not-exists` makes this safe to run
again:

```sh
for topic in accounting research sales operations; do
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done
```

The POS and Spark code uses these lowercase topic names. Old uppercase topics,
if present, are not used by the current application.

### 6.4 Start MySQL

On macOS with Homebrew:

```sh
brew services start mysql
```

Check the service:

```sh
mysql -u root -e "SELECT VERSION();"
```

## 7. First-time database and photo setup

Run the following commands from the **project root** (the directory containing
`emp.csv`, `dept.csv`, `expense.csv`, and `gradlew`).

Create the Cassandra keyspace/table:

```sh
cqlsh localhost 9042 -f scripts/cassandra-schema.cql
```

Allow the MySQL client to load local CSV files, then create the database/tables
and import `dept.csv` and `emp.csv`:

```sh
mysql -u root -e "SET GLOBAL local_infile=1"
mysql --local-infile=1 -u root < scripts/mysql-schema.sql
```

The import script uses `REPLACE` for the CSV rows, so re-running it refreshes
the matching employee and department records. It does not import expenses into
MySQL; expenses belong in Cassandra.

Verify the MySQL import:

```sh
mysql -u root project3 -e "SELECT COUNT(*) AS employees FROM emp;"
mysql -u root project3 -e "SELECT COUNT(*) AS departments FROM dept;"
```

Expected counts are 14 employees and 4 departments.

Employee image filenames are in `emp.csv`, but the repository does not include
the actual JPG image files. Upload each available JPG through the web form,
selecting the employee whose image it is. The upload controller saves it to
`/images/<filename from emp.csv>` in HDFS. For example, upload Scott's image
for SCOTT so the browser can read `/images/scott.jpg`.

Check HDFS files:

```sh
hdfs dfs -ls /images
```

An image that has not been uploaded will return HTTP 404 from the image
endpoint; it does not prevent the employee data itself from being loaded.

## 8. Run the application

Start each long-running process **once**, from the project root, in its own
terminal. Leave the terminals open while testing.

### Terminal 1: start the Spark Kafka-to-Cassandra stream

```sh
./gradlew :spark-query:run
```

Wait for:

```text
Spark is consuming accounting, research, sales and operations.
```

As batches are processed, the terminal prints:

```text
Stored Kafka batch ... in Cassandra.
```

This process runs continuously. It is the only supported streaming consumer
you should start for the main demo.

### Optional: publish the provided `expense.csv`

With Kafka and the Spark consumer running, use another terminal once:

```sh
mvn -f POSserver/pom.xml compile exec:java@import-expenses \
  -Dexec.args="emp.csv dept.csv expense.csv"
```

The importer prints `Published 176 expense CSV records to Kafka.` It uses
`emp.csv` and `dept.csv` to find each employee's department and publishes the
six expense fields to that department's Kafka topic. Spark then stores the
records in Cassandra. Re-running this command republishes the sample rows; it
is not necessary for the live demo.

### Terminal 2: start the live POS producer

```sh
mvn -f POSserver/pom.xml compile exec:java
```

It prints one line each second, for example:

```text
research <- 7788,2026-11-25 15:12:19,Macaroni,Food,2,35.45
```

The six comma-separated fields are `user_id,date_time,description,type,count,
payment`. Keep this process running to see new live expenses.

### Terminal 3: start the web application

```sh
./gradlew :webapp:bootRun
```

When startup succeeds, open:

```text
http://localhost:8080
```

The page can also be opened while the POS and Spark processes are running.
Refresh the page to request updated totals. Searching for `SCOTT` shows his
total and, when there are at most three matching employees, up to 50 recent
expense records for each match.

### Stop the applications

Use **Ctrl+C** in each application terminal. Stop the POS, web app and Spark
application this way when you finish. Stop Kafka, ZooKeeper, Cassandra and HDFS
using their own documented service/daemon shutdown commands; do not kill an
arbitrary Java process.

## 9. Verify and test each stage

The values are random for live POS data, so do not expect an exact running
total. Use the same employee ID across the stages to prove that a record
travels through the system.

### Test A: service health

```sh
jps
nodetool status
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list
mysql -u root project3 -e "SELECT COUNT(*) FROM emp;"
```

Expected:

- HDFS daemons appear in `jps`.
- `nodetool status` shows the local Cassandra node as `UN`.
- Kafka lists `accounting`, `operations`, `research` and `sales`.
- MySQL returns `14` for the employee count.

If Spark Standalone is separately installed and started, its `Master` and
`Worker` may also appear in `jps`. Their presence alone does not mean this
application used the Standalone cluster; see the Spark deployment note above.

### Test B: see the POS producer output

The POS terminal should print one new line per second. Example:

```text
sales <- 7499,2026-11-25 15:24:00,Fridge,Appliances,2,4480.00
```

Check the employee in MySQL to confirm the correct department:

```sh
mysql -u root project3 -e \
  "SELECT e.empno,e.ename,e.deptno,d.dname FROM emp e JOIN dept d USING (deptno) WHERE e.empno=7499;"
```

ALLEN (`7499`) belongs to SALES (`30`), so the producer should use the
lowercase `sales` topic.

### Test C: verify Kafka messages

In a new terminal, read a few messages from a topic:

```sh
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic research --from-beginning --max-messages 3
```

This may display older retained messages before recent ones. The producer's
`topic <- record` line is the quickest way to see the current topic and record.

### Test D: verify Spark processing

The Spark terminal should show `Stored Kafka batch ... in Cassandra.` after
receiving Kafka records. This proves Spark has processed a batch; verify the
stored values in Cassandra as the next step.

### Test E: verify Cassandra rows

For a known employee such as SCOTT (`7788`):

```sh
cqlsh localhost 9042 -e \
  "SELECT user_id,date_time,description,type,count,payment FROM project3.expenses WHERE user_id=7788 LIMIT 10"
```

The CSV sample rows have timestamps on 2025-12-10; live producer rows have
current timestamps. Use the latest rows to verify the live stream:

```sh
cqlsh localhost 9042 -e \
  "SELECT user_id,date_time,description,payment FROM project3.expenses WHERE user_id=7788 ORDER BY date_time DESC LIMIT 10"
```

The table clusters each employee's rows by `date_time` ascending, so Cassandra
can also return the same partition in reverse time order with `ORDER BY
date_time DESC`.

### Test F: verify Spark's total endpoint

```sh
curl -i http://localhost:8080/total/7788
```

Expect HTTP `200` and a decimal total with two fractional digits, for example
`12345.67`. The number changes as new expenses arrive.

`spark-query/src/main/java/com/sau/Main.java` contains an older one-shot query
example, but Gradle's configured `application` entry point is the streaming
app. Therefore, `./gradlew :spark-query:run` starts the stream, not the
one-shot query. Use `/total/7788` for the supported total check; do not claim
the old query class is separately wired as a runnable Gradle task.

### Test G: verify the browser report

Open `http://localhost:8080`. Check that:

- The employee list includes names and departments from MySQL.
- Manager names come from the manager self-join.
- Salary, commission and department are displayed.
- The expense total agrees with `/total/<empno>` for the same employee.
- Searching `SCOTT` shows Scott's recent expense records when records exist.
- The total changes after new POS records are processed and the page is
  refreshed.

### Test H: verify HDFS photo storage and serving

```sh
hdfs dfs -ls /images
curl -I http://localhost:8080/img/scott.jpg
```

After uploading a valid image for SCOTT, expect the file in HDFS and HTTP `200`
from the image endpoint. Open `http://localhost:8080/img/scott.jpg` in a browser
to view it.

## 10. What each module does

### `POSserver`

This is the Java POS producer used by the main demo. `Main.java` chooses an
employee and an item, builds the CSV record and publishes it to Kafka once per
second. `Topic.java` maps department numbers to lowercase Kafka topic names.
`ExpenseCsvImporter.java` is a separate one-shot entry point that reads the
three supplied CSV files and publishes the sample expense rows.

`POSserver/pom.xml` declares Kafka client dependencies and Maven entry points.
Use **either** the CSV importer for a finite sample import **or** the live
producer for continuously changing data; they are two producer modes, not two
required persistent services.

### `generator`

This is a second, independent Kafka producer kept as a Gradle module. It also
produces a record every second, but has a different product list and hard-coded
employee/topic arrays. It is not the producer used by the README's main
workflow. Do not run it at the same time as `POSserver`, or both producers will
add records and it will be less clear which one generated them.

### `spark-query`

`KafkaToCassandraStream.java` is the main streaming job. It subscribes to all
four lowercase topics, decodes the Kafka message, converts the six CSV fields
to typed Spark columns, rejects malformed records, and writes each micro-batch
to `project3.expenses` with the Cassandra connector.

The checkpoint stores Spark's Kafka offsets and stream progress locally at
`~/.p3-bigdata/spark-checkpoint`. It is what allows a restarted stream to
continue from its prior progress. With a brand-new checkpoint, the configured
`startingOffsets=earliest` setting reads the Kafka records still retained in
the subscribed topics; a restart with an existing checkpoint resumes from its
saved offsets. This can replay historical/sample messages on a first run. Run
one copy of this job at a time and do not delete the checkpoint as a routine
shutdown step.

`Main.java` is a separate one-shot Spark/Cassandra query example. It is not the
streaming job. The configured Gradle application entry point is
`KafkaToCassandraStream`.

### `webapp`

`WebApplication.java` starts Spring Boot. `EmployeeController.java` gets
employees and departments from MySQL, serves the home page, and calls
`SparkService.java` to calculate totals or retrieve recent expense rows.
`SparkService` reads Cassandra through the Spark Cassandra connector; it does
not calculate expense totals in SQL/MySQL.

`ImageController.java` reads JPGs from HDFS and serves `/img/<name>`.
`UploadController.java` accepts a JPG upload, finds the employee's expected
filename in MySQL and writes the image under `/images` in HDFS.
`templates/index.html` renders the report and upload/search forms.

### `scripts`

`mysql-schema.sql` creates the employee/department tables and imports their CSV
data. `cassandra-schema.cql` creates the Cassandra keyspace and expense table.
`streaming.scala` is an older alternative Spark-shell example; it is not needed
when the Java streaming job is running.

## 11. Configuration

Defaults work for a local development machine:

- `KAFKA_BOOTSTRAP_SERVERS` changes the Kafka address for `POSserver` and the
  Spark streaming app.
- `CASSANDRA_HOST` changes the Cassandra host for Spark streaming.
- `P3_SPARK_CHECKPOINT` changes the streaming checkpoint directory.
- `DB_URL`, `DB_USER` and `DB_PASSWORD` change the Spring Boot MySQL connection.

For example:

```sh
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CASSANDRA_HOST=127.0.0.1
export P3_SPARK_CHECKPOINT="$HOME/.p3-bigdata/spark-checkpoint"
export DB_URL='jdbc:mysql://localhost:3306/project3?useSSL=false&serverTimezone=Europe/Istanbul'
export DB_USER=root
export DB_PASSWORD=''
```

Set variables in the same terminal before launching the application. HDFS is
currently configured in `ImageController.java` and `UploadController.java` as
`hdfs://localhost:9000`; changing the NameNode address requires updating those
classes.

## 12. Troubleshooting

### `Port 8080 is already in use`

The web app is already running, or another process owns port 8080. Do not start
a second copy. Try `http://localhost:8080`; stop the existing web-app terminal
with Ctrl+C if you need to restart it.

### Spark says `Concurrent update to the commit log`

Two Spark stream jobs are using the same checkpoint. Stop the duplicate
`spark-query`, `spark-shell` or IDE-launched stream. Restart only one
`./gradlew :spark-query:run` process. Do not delete the checkpoint to work
around a duplicate process.

### Kafka has no lowercase topics

Create `accounting`, `research`, `sales` and `operations` using the topic
commands above. Topic names are case-sensitive; uppercase legacy topics do not
replace the lowercase topics.

### MySQL says local data loading is disabled

Enable it on the server and client, then rerun the import from the project root:

```sh
mysql -u root -e "SET GLOBAL local_infile=1"
mysql --local-infile=1 -u root < scripts/mysql-schema.sql
```

### The web page returns HTTP 500 or Spark cannot reach Cassandra

Check that MySQL and Cassandra are running, that
`project3.expenses` exists, and that the connection settings match the local
services. Look at the web-app terminal for the root exception.

### HDFS images return 404

The requested JPG has not been uploaded to HDFS at `/images/<name>`, or its
filename does not match the `img` value in MySQL. Check `hdfs dfs -ls /images`
and upload the photo for the corresponding employee.

### A live record never appears under `operations`

The supplied employee CSV has no employee in department 40. The topic should
still exist, but the live simulator has no department-40 employee to select.

### Maven prints an SLF4J warning

The POS process may print that no SLF4J binding was found. It is a logging
configuration warning; the producer can still send records. Verify the Kafka
topic and Cassandra row rather than treating the warning alone as a failure.

## 13. Common instructor questions

**Where does `user_id` come from?**  
It is the `empno` column in `emp.csv`/MySQL `emp`. In each expense record it
identifies the employee who made the purchase.

**How do you know which Kafka topic receives a record?**  
Look up the employee's `deptno` in `emp.csv`, then that department number in
`dept.csv`. The POS maps that department to its lowercase `dname`. For example,
SCOTT (`7788`) has department 20, RESEARCH, so his record goes to `research`.

**How can you prove Spark is being used?**  
The streaming application's main class builds a Spark `SparkSession`, reads
Kafka with Spark Structured Streaming, transforms records as Spark columns,
and writes micro-batches using the Spark Cassandra connector. The web
`SparkService` also creates Spark DataFrames from Cassandra and uses Spark's
`sum(payment)` aggregation. Show the code and the Spark terminal's batch
messages. Be clear that the current master is `local[*]`, not a Standalone
master at port 7077.

**Where is the cumulative total calculated?**  
In `webapp/src/main/java/com/sau/SparkService.java`. It reads `project3.expenses`
from Cassandra, filters by `user_id`, and applies Spark `sum(payment)`. The
controller returns the total to the page or `/total/<empno>`.

**How can you prove a record went through all the systems?**  
Show a producer line with an employee ID and topic; show the matching message
in Kafka or Spark's `Stored Kafka batch` log; query Cassandra for that
`user_id`; then show the corresponding employee and updated total on the web
page.

**Why does the web app use both MySQL and Cassandra?**  
MySQL stores relatively stable employee/department details. Cassandra stores
the incoming expense events. The page joins employee information from MySQL
with totals calculated from Cassandra through Spark.

**What does HDFS store?**  
Employee photos. Expense records are stored in Cassandra, not HDFS.

**Why may `operations` have no messages?**  
The department exists, but none of the 14 employees in the provided `emp.csv`
belongs to department 40.

**Why does an expense total change?**  
The live POS chooses random employees and products and publishes new records.
Spark stores them continuously. The web page calculates the cumulative sum
from Cassandra when requested; refresh the page to see the latest total.

**What is the difference between `expense.csv` and the live producer?**  
`expense.csv` is a finite historical sample with 176 rows and 2025 timestamps.
`POSserver` creates new records continuously with current timestamps.

## 14. Test-suite status

The project currently has no automated unit or integration test cases in its
Gradle modules. Running:

```sh
./gradlew :generator:test :spark-query:test :webapp:test
mvn -f POSserver/pom.xml test
```

checks/runs the test tasks, but Gradle reports `NO-SOURCE` when a module has no
test files. The end-to-end checks in section 9 are manual integration tests
against the real local Kafka, Spark, Cassandra, MySQL and HDFS services.

## 15. Final demo checklist

- [ ] HDFS, Cassandra, Kafka (and ZooKeeper, if required) and MySQL are running.
- [ ] The four lowercase Kafka topics exist.
- [ ] MySQL has the 14 employees and 4 departments imported from the CSV files.
- [ ] Cassandra has the `project3.expenses` table.
- [ ] At least the photos used in the demo are present in HDFS `/images`.
- [ ] One Spark streaming consumer is running.
- [ ] The POS producer is printing a record once per second.
- [ ] The web app opens at `http://localhost:8080`.
- [ ] A chosen employee's total appears on the page and at `/total/<empno>`.
- [ ] A matching Cassandra row can be shown for the chosen employee.
- [ ] The Spark deployment mode (`local[*]` versus Standalone cluster) is
      described accurately if asked.
