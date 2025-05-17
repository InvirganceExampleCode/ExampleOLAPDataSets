/*
 * Copyright 2024 INVIRGANCE LLC

Permission is hereby granted, free of charge, to any person obtaining a copy 
of this software and associated documentation files (the “Software”), to deal 
in the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE.
 */
package com.invirgance.example.olap;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.dbms.BatchOperation;
import com.invirgance.convirgance.dbms.DBMS;
import com.invirgance.convirgance.dbms.Query;
import com.invirgance.convirgance.input.PipeDelimitedInput;
import com.invirgance.convirgance.jdbc.AutomaticDriver;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.StoredConnection;
import com.invirgance.convirgance.jdbc.datasource.DriverDataSource;
import com.invirgance.convirgance.jdbc.schema.Catalog;
import com.invirgance.convirgance.jdbc.schema.DatabaseSchemaLayout;
import com.invirgance.convirgance.jdbc.schema.Schema;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.source.FileSource;
import com.invirgance.convirgance.transform.IdentityTransformer;
import java.io.File;
import javax.sql.DataSource;

/**
 *
 * @author jbanes
 */
public class LoadDatabase implements Tool
{
    private String url = "jdbc:derby://localhost:1527/sampling";
    private String username = "adventureworks";
    private String password = "adventureworks";
    
    public DatabaseSchemaLayout getSchema()
    {
        AutomaticDriver driver = AutomaticDrivers.getDriverByURL(url);
        StoredConnection connection = driver.createConnection("AdventureWorks").driver().url(url).username(username).password(password).build();
        
        return connection.getSchemaLayout();
    }
    
    public DataSource getSource()
    {
        return DriverDataSource.getDataSource(url, "adventureworks", "adventureworks");
    }
    
    public String getInsertSQL(String table, String[] columns)
    {
        var buffer = new StringBuffer("insert into ");
        var first = true;
        
        buffer.append(table);
        buffer.append(" values (");
        
        for(var column : columns)
        {
            if(!first) buffer.append(", ");
            
            buffer.append(':');
            buffer.append(column);
            
            first = false;
        }
        
        buffer.append(")");
        
        return buffer.toString();
    }
    
    public void load(String table)
    {
        var create = TableExtractor.getCreateTable(table);
        
        load(create);
    }
    
    public void load(TableExtractor.CreateTable create)
    {
        var columns = create.getColumnNames();
        var sql = getInsertSQL(create.getTableName(), columns);
        
        var source = new FileSource("AdventureWorks/raw/" + create.getTableName() + ".csv");
        var input = new PipeDelimitedInput(columns, "UTF-16"); // <- UTF-16 with BOM!
        
        Iterable<JSONObject> stream;
        
        stream = input.read(source);
        stream = new IdentityTransformer() {
            
            private String[] binary = new String[]{ "EmployeePhoto", "LargePhoto", "SalesTerritoryImage" };
            private String[] empty = new String[]{ "EnglishProductName", "SpanishProductName", "FrenchProductName" };
            
            @Override
            public JSONObject transform(JSONObject record) throws ConvirganceException
            {
                // Parse binary data
                for(String key : binary)
                {
                    if(record.containsKey(key) && !record.isNull(key))
                    {
                        record.put(key, parseBinaryString(record.getString(key)));
                    }
                }
                
                // Set nulls to empty string
                for(String key : empty)
                {
                    if(record.containsKey(key) && record.isNull(key)) record.put(key, "");
                }
                
                return record;        
            }
        }.transform(stream);
        
        // Bulk load the data in a transaction
        new DBMS(getSource()).update(new BatchOperation(new Query(sql), stream));
    }
    
    public boolean skip(String name)
    {
        if(name.equalsIgnoreCase("DatabaseLog")) return true;
        
        return false;
    }
    
    @Override
    public String getName()
    {
        return "load";
    }
    
    private byte[] parseBinaryString(String value)
    {
        byte[] data = new byte[value.length()/2];
        
        for(int i=0; i<data.length; i++)
        {
            data[i] = (byte)Integer.parseInt(value.substring(i*2, i*2+2), 16);
        }
        
        return data;
    }
    
    private String normalize(String sql)
    {
        sql = sql.replaceAll(" bit", " int");
        sql = sql.replaceAll(" BIT", " INT");
        sql = sql.replaceAll("datetime", "timestamp");
        sql = sql.replaceAll("DATETIME", "TIMESTAMP");
        sql = sql.replaceAll("nchar", "char");
        sql = sql.replaceAll("NCHAR", "CHAR");
        sql = sql.replaceAll("nvarchar", "varchar");
        sql = sql.replaceAll("NVARCHAR", "VARCHAR");
        sql = sql.replaceAll("money", "decimal(19, 4)");
        sql = sql.replaceAll("MONEY", "DECIMAL(19, 4)");
        sql = sql.replaceAll("varbinary\\(max\\)", "blob");
        sql = sql.replaceAll("VARBINARY\\(MAX\\)", "BLOB");
        sql = sql.replaceAll("varchar\\(max\\)", "varchar(4096)");
        sql = sql.replaceAll("VARCHAR\\(MAX\\)", "VARCHAR(4096)");
        sql = sql.replaceAll("Unknown ", "\"Unknown\" ");
        sql = sql.replaceAll("n'", "'");
        sql = sql.trim();
        
        if(sql.charAt(sql.length()-1) == ';') sql = sql.substring(0, sql.length()-1);
        
        return sql;
    }

    @Override
    public void execute(String[] args)
    {
        DatabaseSchemaLayout layout = getSchema();
        Catalog catalog = layout.getCurrentCatalog();
        Schema schema = layout.getCurrentSchema();
        
        String sql;
        
        for(var create : TableExtractor.getCreateTables())
        {
            // Skip SQL Server internal tables
            if(create.getTableName().equalsIgnoreCase("sysdiagrams")) continue;
            
            System.out.print("Loading " + create.getTableName() + "... ");
            
            // Some tables do not have data
            if(skip(create.getTableName()) || !new File("AdventureWorks/raw/" + create.getTableName() + ".csv").exists()) 
            {
                System.out.println("Done");
                
                continue;
            }
            
            if(schema.getTable(create.getTableName()) == null)
            {
                sql = normalize(create.toSQL());

                new DBMS(layout.getDataSource()).update(new Query(sql));
            }
            
            load(create);
            
            System.out.println("Done");
        }
    }

    @Override
    public String getHelp()
    {
        return """
                load <jdbc url> <username> <password>
                
                    Creates the necessary tables and loads the AdventureWorks data
                    into the specified database. The load will fail if the table
                    already exists and contains data.

                    For safety reasons, these scripts will not attempt to drop or
                    truncate pre-existing tables. 

                    jdbc url - (Required) The JDBC connection URL for the database
                    username - (Required) The database username to log in as
                    password - (Required) The database password to log in with""";
              
    }
    
}
