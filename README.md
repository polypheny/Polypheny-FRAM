# Polypheny-FRAM #

[![CodeFactor](https://www.codefactor.io/repository/github/polypheny/polypheny-fram/badge/master)](https://www.codefactor.io/repository/github/polypheny/polypheny-fram/overview/master)

_Polypheny-FRAM_ is a plugin for Polypheny-DB making it the distributed polystore.

Polypheny-FRAM is a research system developed at the University of Basel, Switzerland. 
Polypheny-FRAM is not intended to be used in a productive environment! Instead, it helps distributed database researchers to test and evaluate novel data management protocols.


## Getting Started (Standalone Mode) ##
 1) Clone the repository
 2) Run `gradlew jdk8_zipAll` &mdash; OpenJDK 8 or 11 required
 3) Enter either the Windows or the Linux distribution which have been assembled in `build/distributions` 
    - Linux:   run `bin/polypheny-fram`
    - Windows: run `bin\polypheny-fram.exe -c --console`
       > Note: The console arguments attach a console to the process. Otherwise, Polypheny-FRAM would run as background process. This is a limitation of [packr](https://github.com/libgdx/packr). See [here](https://github.com/libgdx/packr#executable-command-line-interface) for details.
 4) Connect to Polypheny-FRAM using the Apache Calcite Avatica JDBC driver version 1.16.0 ([@MvnRepository](https://mvnrepository.com/artifact/org.apache.calcite.avatica/avatica-core/1.16.0)). Use the following connection details:
    - Driver Class:   `org.apache.calcite.avatica.remote.Driver`
    - Connection URL: `jdbc:avatica:remote:url=http://localhost:20591;serialization=protobuf`

> Note: In the standalone mode, Polypheny-FRAM uses HSQLDB as the underlying data storage.


## Polypheny-DB Plugin ##
Polypheny-FRAM can extend Polypheny-DB to provide data management protocols running on top of a cluster of Polypheny-DB nodes. Basically, distributing the polystore.


## Credits ##
_Polypheny-FRAM_ is based on the [Apache Calcite](https://calcite.apache.org/) server project and uses several other projects:

* [Apache Avatica](https://calcite.apache.org/avatica/): A framework for building JDBC drivers
* [Apache Calcite](https://calcite.apache.org/): A framework for building databases
* [HSQLDB](http://hsqldb.org/): A relational database written in Java
* [JavaCC](https://javacc.org/): A parser generator
* [JGroups](http://www.jgroups.org/): A powerful library for reliable messaging
* [Project Lombok](https://projectlombok.org/): A library providing compiler annotations

Those projects are used "as is" and are integrated as libraries. 


## Acknowledgements ##
The Polypheny-DB project is supported by the Swiss National Science Foundation (SNSF) under the contract no. 200021_172763.


## License ##
The Apache License, Version 2.0
