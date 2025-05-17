# Example OLAP Data Set(s)

This project is intended to provide portable access to example OLAP data sets. Currently only 
[Microsoft's AdventureWorks DW dataset](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works) is included. 

The AdventureWorks dataset is somewhat difficult to work with outside of SQL Server due to the unqique encoding challenges with the data files. Microsoft 
provides the data encoded in UTF-16 with BOM pipe-delimited files with no headers. Which is incredibly hard to parse for anything other than SQL Server. 
And the SQL is also specific to SQL Server.

This codebase extracts the table and column information from the SQL Server scripts, normalizes it for most databases, and then uses that information to
properly parse the pipe-delimited files using [Convirgance](https://github.com/InvirganceOpenSource/convirgance).


## License

All code and data are licensed under the MIT license. Microsoft is the owner of all data in the `AdventureWorks/raw` directory. All other code is owned by Invirgance.
