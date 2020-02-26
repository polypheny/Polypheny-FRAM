# Polypheny-FRAM #

[![GitHub CI](https://img.shields.io/github/workflow/status/polypheny/Polypheny-FRAM/CI/master?label=CI&logo=GitHub&logoColor=white)](https://github.com/polypheny/Polypheny-FRAM/actions?query=workflow%3ACI) 
[![CodeFactor](https://img.shields.io/codefactor/grade/github/polypheny/Polypheny-FRAM/master?label=CodeFactor&logo=CodeFactor&logoColor=white)](https://www.codefactor.io/repository/github/polypheny/polypheny-fram/overview/master)
[![DBIS Nexus](https://img.shields.io/nexus/maven2/https/nexus.dmi.unibas.ch/org.polypheny/polypheny-fram.svg?nexusVersion=3&label=Nexus&logo=data:image/svg%2Bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAzNyA0Mi4yNSI+PGRlZnM+PHN0eWxlPi5jbHMtMXtmaWxsOiMyOWI0NzM7fS5jbHMtMntmaWxsOiNmZmY7fTwvc3R5bGU+PC9kZWZzPjx0aXRsZT5OZXh1c1JlcG9fSWNvbl93aGl0ZStjb2xvcjwvdGl0bGU+PGcgaWQ9IlByb2R1Y3RfTG9nb3NfVmVydGljYWxfV2hpdGVfQ29sb3IiIGRhdGEtbmFtZT0iUHJvZHVjdCBMb2dvcyBWZXJ0aWNhbCBXaGl0ZSArIENvbG9yIj48cGF0aCBjbGFzcz0iY2xzLTEiIGQ9Ik0xOS4xMywyN1YxOS41NmEzLjc3LDMuNzcsMCwwLDEsMi43Ny0xLjI4LDMuNTIsMy41MiwwLDAsMSwuODEuMDdWMTUuNDZhNC45MSw0LjkxLDAsMCwwLTMuNTgsMS43OVYxNS43NGgtM1YyN1oiLz48cG9seWdvbiBjbGFzcz0iY2xzLTIiIHBvaW50cz0iMCAxMC4xOCAwIDMxLjU2IDE4LjUgNDIuMjUgMzcgMzEuNTYgMzcgMTEuMyAzMi40NiAxMy45MiAzMi40NiAyOC45NCAxOC41IDM3IDQuNTQgMjguOTQgNC41NCAxMi44MSAxNy42MyA1LjI1IDE3LjYzIDAgMCAxMC4xOCIvPjxwb2x5Z29uIGNsYXNzPSJjbHMtMSIgcG9pbnRzPSIxOS40NCAwIDE5LjQ0IDUuMjUgMzEuNzMgMTIuMzUgMzYuMjcgOS43MyAxOS40NCAwIi8+PC9nPjwvc3ZnPg==)](https://nexus.dmi.unibas.ch/#browse/search/maven=attributes.maven2.groupId%3Dorg.polypheny%20AND%20attributes.maven2.artifactId%3Dpolypheny-fram)

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
