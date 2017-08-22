
== Welcome to Sqoop!

This Project is forked from hortonworks/sqoop-relase.

== Release Notes
1. Add the function of importing data to Apache Kafka from SQL.
2. Fix bug-1:
3. Fix bug-2: 

== Instructions for use (importing data to kafka)
Example: sqoop import --connect jdbc:mysql://localhost:3306/DatabaseName --username root --password 123456  --table SqlTableName  --topic KafkaTopic --broker-list master:6667
Description: 
1. topic -- the kafka topic 
2. broker-list -- the kafka broker list, format: IP:PORT 

== Compiling Sqoop

Compiling Sqoop requires the following tools:

* Apache ant (1.7.1)
* Java JDK 1.6

Additionally, building the documentation requires these tools:

* asciidoc
* make
* python 2.5+
* xmlto
* tar
* gzip

To compile Sqoop, run +ant package+. There will be a fully self-hosted build
provided in the +build/sqoop-(version)/+ directory. 

You can build just the jar by running +ant jar+.

See the COMPILING.txt document for for information.

