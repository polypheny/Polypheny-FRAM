# Polypheny-FRAM #

[![GitHub CI](https://img.shields.io/github/workflow/status/polypheny/Polypheny-FRAM/CI/master?label=GitHub%20CI&logo=GitHub&logoColor=white)](https://github.com/polypheny/Polypheny-FRAM/actions?query=workflow%3ACI) 
[![CodeFactor](https://img.shields.io/codefactor/grade/github/polypheny/Polypheny-FRAM/master?label=CodeFactor&logo=CodeFactor&logoColor=white)](https://www.codefactor.io/repository/github/polypheny/polypheny-fram/overview/master)
[![DBIS Nexus](https://img.shields.io/nexus/maven2/https/nexus.dmi.unibas.ch/org.polypheny/polypheny-fram.svg?nexusVersion=3&label=Latest%20Artifact&logo=data:image/svg%2Bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAzNyA0Mi4yNSI+PGRlZnM+PHN0eWxlPi5jbHMtMXtmaWxsOiMyOWI0NzM7fS5jbHMtMntmaWxsOiNmZmY7fTwvc3R5bGU+PC9kZWZzPjx0aXRsZT5OZXh1c1JlcG9fSWNvbl93aGl0ZStjb2xvcjwvdGl0bGU+PGcgaWQ9IlByb2R1Y3RfTG9nb3NfVmVydGljYWxfV2hpdGVfQ29sb3IiIGRhdGEtbmFtZT0iUHJvZHVjdCBMb2dvcyBWZXJ0aWNhbCBXaGl0ZSArIENvbG9yIj48cGF0aCBjbGFzcz0iY2xzLTEiIGQ9Ik0xOS4xMywyN1YxOS41NmEzLjc3LDMuNzcsMCwwLDEsMi43Ny0xLjI4LDMuNTIsMy41MiwwLDAsMSwuODEuMDdWMTUuNDZhNC45MSw0LjkxLDAsMCwwLTMuNTgsMS43OVYxNS43NGgtM1YyN1oiLz48cG9seWdvbiBjbGFzcz0iY2xzLTIiIHBvaW50cz0iMCAxMC4xOCAwIDMxLjU2IDE4LjUgNDIuMjUgMzcgMzEuNTYgMzcgMTEuMyAzMi40NiAxMy45MiAzMi40NiAyOC45NCAxOC41IDM3IDQuNTQgMjguOTQgNC41NCAxMi44MSAxNy42MyA1LjI1IDE3LjYzIDAgMCAxMC4xOCIvPjxwb2x5Z29uIGNsYXNzPSJjbHMtMSIgcG9pbnRzPSIxOS40NCAwIDE5LjQ0IDUuMjUgMzEuNzMgMTIuMzUgMzYuMjcgOS43MyAxOS40NCAwIi8+PC9nPjwvc3ZnPg==)](https://nexus.dmi.unibas.ch/#browse/search/maven=attributes.maven2.groupId%3Dorg.polypheny%20AND%20attributes.maven2.artifactId%3Dpolypheny-fram)

_Polypheny-FRAM_ is a plugin for Polypheny-DB making it the distributed polystore.

Polypheny-FRAM is a research system developed at the University of Basel, Switzerland. 
Polypheny-FRAM is not intended to be used in a productive environment! Instead, it helps distributed database researchers to test and evaluate novel data management protocols.


## Getting Started (Standalone Mode) ##
 1) Clone the repository
 2) Run `gradlew jdk11_zipAll`  
    > Note: OpenJDK 8 or 11 required to run this step.
 3) Enter either the Windows or the Linux distribution which have been assembled in `build/distributions` 
    - Linux:   run `bin/polypheny-fram`
    - Windows: run `bin\polypheny-fram`
      > Note for the Windows JDK 8 release (built with the target `jdk8_zipWindows64Package`):\
        Append the console arguments "bin\polypheny-fram.exe -c --console" to attach a console to the process. Otherwise, Polypheny-FRAM would run as background process. This is a limitation of [packr](https://github.com/libgdx/packr). See [here](https://github.com/libgdx/packr#executable-command-line-interface) for details.
 4) a) Benchmark Polypheny-FRAM using [OLTPBench for Polypheny-FRAM](https://github.com/nouda/oltpbench). Currently available benchmarks: TPC-C and YCSB.
    
    b) Connect to Polypheny-FRAM using [polypheny-jdbc-driver:1.3](https://nexus.dmi.unibas.ch/#browse/search/maven=attributes.maven2.groupId%3Dorg.polypheny%20AND%20attributes.maven2.artifactId%3Dpolypheny-jdbc-driver%20AND%20version%3D1.3) with the following connection details:
       - Driver Class: `org.polypheny.jdbc.Driver`
       - Connection URL: `jdbc:polypheny://localhost/`
       > Note: You can also use the Apache Calcite Avatica driver [avatica-core:1.16.0](https://mvnrepository.com/artifact/org.apache.calcite.avatica/avatica-core/1.16.0). 
         Use the following connection details:
         > - Driver Class:   `org.apache.calcite.avatica.remote.Driver`
         > - Connection URL: `jdbc:avatica:remote:url=http://localhost:20591;serialization=protobuf`


### Configuration ###
Polypheny-FRAM uses the [typesafe](https://github.com/lightbend/config) library. 
Copy the file `conf/sample-application.conf` to `conf/application.conf` and edit the `conf/application.conf` to fit your needs.   
You can find the HOCON syntax [here](https://github.com/lightbend/config/blob/master/HOCON.md).

### Persistency of Data ###
In the standalone mode, Polypheny-FRAM uses HSQLDB as the underlying data storage. By default, HSQLDB is configured to use in memory databases. Therefore, after Polypheny-FRAM has terminated, all data is lost.


## Polypheny-DB Plugin ##
Polypheny-FRAM can extend Polypheny-DB to provide data management protocols running on top of a cluster of Polypheny-DB nodes. Basically, distributing the polystore.

> Note: The extension mode is currently not available. Use the standalone mode to explore Polypheny-FRAM.


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
