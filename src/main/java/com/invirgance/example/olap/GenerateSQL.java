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
import com.invirgance.convirgance.target.FileTarget;
import java.io.File;

/**
 *
 * @author jbanes
 */
public class GenerateSQL implements Tool
{
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
    
    public String getCreateSQL(String table)
    {
        return TableExtractor.getCreateTable(table).toSQL();
    }
    
    public static String normalizeSQL(String sql)
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
    
    public void generate()
    {
        File file;
        
        for(var create : TableExtractor.getCreateTables())
        {
            System.out.print("Generating " + create.getTableName() + "... ");

            file = new File("AdventureWorks/sql/" + create.getTableName() + ".sql");

            new FileTarget(file).writeString(normalizeSQL(create.toSQL()));

            System.out.println("Done");
        }
    }
    
    public void generate(String table)
    {
        TableExtractor.CreateTable create = TableExtractor.getCreateTable(table);
        File file;
            
        if(create == null) throw new ConvirganceException("Table " + table + " not found!");

        file = new File("AdventureWorks/sql/" + create.getTableName() + ".sql");

        new FileTarget(file).writeString(normalizeSQL(create.toSQL()));
    }

    @Override
    public String getName()
    {
        return "sql";
    }

    @Override
    public void execute(String[] args)
    {
        if(args.length > 1)
        {
            generate(args[1]);
            
            return;
        }
        
        generate();
    }

    @Override
    public String getHelp()
    {
        return """
                sql [table]
                
                    Outputs the SQL needed to recreate each table in the AdventureWorks
                    data set. All tables are exported unless the table name is specifed.

                    SQL files are written to AdventureWorks/sql/<table>.sql

                    table - (Optional) Specify the name of the table""";
    }
}
