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
import com.invirgance.convirgance.input.PipeDelimitedInput;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.output.CSVOutput;
import com.invirgance.convirgance.output.JBINOutput;
import com.invirgance.convirgance.output.JSONOutput;
import com.invirgance.convirgance.source.FileSource;
import com.invirgance.convirgance.target.FileTarget;
import com.invirgance.convirgance.transform.CoerceStringsTransformer;
import java.io.File;

/**
 *
 * @author jbanes
 */
public class ConvertData implements Tool
{
    public void convert(String table)
    {
        var create = TableExtractor.getCreateTable(table);
        
        convert(create);
    }
    
    public void convert(TableExtractor.CreateTable create)
    {
        var table = create.getTableName();
        var columns = create.getColumnNames();
        
        var source = new FileSource("AdventureWorks/raw/" + table + ".csv");
        var input = new PipeDelimitedInput(columns, "UTF-16"); // <- UTF-16 with BOM!
        
        var json = new FileTarget("AdventureWorks/json/" + table + ".json");
        var csv = new FileTarget("AdventureWorks/csv/" + table + ".csv");
        var jbin = new FileTarget("AdventureWorks/jbin/" + table + ".bin");
        
        Iterable<JSONObject> stream = input.read(source);
        
        stream = new CoerceStringsTransformer().transform(stream);
        
        new JSONOutput().write(json, stream);
        new CSVOutput().write(csv, stream);
        new JBINOutput().write(jbin, stream);
    }

    @Override
    public String getName()
    {
        return "convert";
    }

    @Override
    public void execute(String[] args)
    {
        TableExtractor.CreateTable table;
        
        if(args.length > 1)
        {
            table = TableExtractor.getCreateTable(args[1]);
            
            if(table == null) throw new ConvirganceException("Table " + args[1] + " not found!");
            
            convert(table);
            
            return;
        }
        
        for(var create : TableExtractor.getCreateTables())
        {
            System.out.print("Converting " + create.getTableName() + "... ");
            
            // Some tables do not have data
            if(!new File("AdventureWorks/raw/" + create.getTableName() + ".csv").exists()) 
            {
                System.out.println("Done");
                
                continue;
            }
            
            convert(create);
            
            System.out.println("Done");
        }
    }

    @Override
    public String getHelp()
    {
        return """
                convert [table]
                
                    Converts the raw data into various formats such as CSV, JSON,
                    and JBIN. All data is converted unless the table name is
                    specified.

                    Data is output to new directories under AdventureWorks/<format>.

                    table - (Optional) Specify the name of the table to convert""";
    }
}
