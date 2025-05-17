# Example OLAP Data Set(s)

This project is intended to provide portable access to example OLAP data sets. Currently only 
[Microsoft's AdventureWorks DW dataset](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works) is included. 

The AdventureWorks dataset is somewhat difficult to work with outside of SQL Server due to the unqique encoding challenges with the data files. Microsoft 
provides the data encoded in UTF-16 with BOM pipe-delimited files with no headers. Which is incredibly hard to parse for anything other than SQL Server. 
And the SQL is also specific to SQL Server.

This codebase extracts the table and column information from the SQL Server scripts, normalizes it for most databases, and then uses that information to
properly parse the pipe-delimited files using [Convirgance](https://github.com/InvirganceOpenSource/convirgance).


## Usage

Requires Java JDK 17 or higher. After downloading a release or compiling the project, you can run the executable jar:

```java -jar dataset.jar <command> [options]```

The tool provides three commands for generating SQL, converting the data, and loading the data into a database.

#### Generate SQL

```java -jar dataset.jar sql```

Extracts the table create commands and writes a normalized SQL file for each table to `AdventureWorks/sql/<table>.sql`. SQL files have not been
tested on all databases, so you may need to make adjustments to make the SQL work for your DBMS.

#### Convert Data

```java -jar dataset.jar convert```

Converts the data from its raw UTF-16 pipe-delimited format into a variety of formats including CSV, JSON, and Convirgance JBIN under `AdventureWorks/csv`, `AdventureWorks/json`, and `AdventureWorks/jbin` respectively.

#### Load Database

```java -jar dataset.jar load <jdbc url> <username> <password>```

Uses [Convirgance (JDBC)](https://github.com/InvirganceOpenSource/convirgance-jdbc) to download a database driver for the provided JDBC connection URL,
connects the database, and attempts to use Convirgance to create the required tables and load them with data. Note that existing tables will not be 
deleted or truncated, so beware that the load will fail if loaded tables already exist.


## License

All code and data are licensed under the MIT license. Microsoft is the owner of all data in the `AdventureWorks/raw` directory. All other code is owned by Invirgance.
