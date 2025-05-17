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

import javax.sql.DataSource;

/**
 *
 * @author jbanes
 */
public class Main
{
    public static Tool[] tools = new Tool[] {
        new ConvertData(),
        new GenerateSQL(),
        new LoadDatabase()
    };
    
    public static String getInsertSQL(String table, String[] columns)
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
    
    public static void printHelp()
    {
        System.out.println();
        System.out.println("This package allows the AdventureWorks Data Warehouse data");
        System.out.println("to be read and ported to nearly any SQL-compatible");
        System.out.println("database. The data can be converted into UTF-8 CSV, JSON,");
        System.out.println("or Convirgance JBIN format to make loads easier. SQL create");
        System.out.println("statements can also be generated and adjusted as needed.");
        System.out.println();
        System.out.println("If you're just trying to create an example database, loading");
        System.out.println("the data directly is probably the fastest option.");
        System.out.println();
        System.out.println("Usage:");
        System.out.println();
        System.out.println("    java -jar dataset.jar <command> [options]");
        System.out.println();
        
        System.out.println("Commands:");
        System.out.println();
        
        for(Tool tool : tools)
        {
            System.out.println(tool.getHelp());
            System.out.println();
            System.out.println();
        }
        
        System.exit(0);
    }
    
    public static void main(String[] args) throws Exception
    {
        if(args.length < 1) printHelp();
        
        for(Tool tool : tools)
        {
            if(args[0].equals(tool.getName())) 
            {
                tool.execute(args);
                
                return;
            }
        }
        
        printHelp();
    }
}
