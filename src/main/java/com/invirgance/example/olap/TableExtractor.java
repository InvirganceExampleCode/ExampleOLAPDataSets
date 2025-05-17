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

import com.invirgance.convirgance.json.JSONArray;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.source.FileSource;
import java.util.List;

/**
 *
 * @author jbanes
 */
public class TableExtractor
{
    private static JSONArray<String> parseSQLLine(String line)
    {
        var token = new StringBuffer();
        var list = new JSONArray<String>();
        
        var left = '.';
        var quote = false;
        
        char c;
        
        for(var i=0; i<line.length(); i++)
        {
            c = line.charAt(i);
            
            if(quote)
            {
                if((left == '[' && c == ']') || (left == '"' && c == '"'))
                {
                    if(token.length() > 0) list.add(token.toString());

                    token.setLength(0);
                    
                    quote = false;
                }
                else
                {
                    token.append(c);
                }
            }
            else if(Character.isWhitespace(c))
            {
                if(token.length() > 0) list.add(token.toString());
                
                token.setLength(0);
            }
            else if(c == '[' || c == '"')
            {
                left = c;
                quote = true;
                
                if(token.length() > 0) list.add(token.toString());
                
                token.setLength(0);
            }
            else if(c == ',')
            {
                if(token.length() > 0) list.add(token.toString());
                
                list.add(",");
                token.setLength(0);
            }
            else
            {
                token.append(c);
            }
        }
        
        if(token.length() > 0) list.add(token.toString());
        
        return list;
    }
    
    public static JSONArray<CreateTable> parse(String sql)
    {
        var lines = sql.split("\n");
        var tokens = new JSONArray<String>();
        var tables = new JSONArray<CreateTable>();
        
        String token;
        CreateTable create = null;
        CreateColumn column;
        
        
        for(var line : lines)
        {
            line = line.trim();
            
            if(line.startsWith("CREATE") || create != null)
            {
                tokens.addAll(parseSQLLine(line));
            }
            
            while(!tokens.isEmpty())
            {
                token = tokens.remove(0);

                if(token.equalsIgnoreCase("create"))
                {
                    if(!tokens.remove(0).equalsIgnoreCase("table")) tokens.clear();
                    else create = new CreateTable();
                }
                else if(create != null && create.table == null)
                {
                    create.table = token;
                    
                    while(!tokens.isEmpty() && tokens.get(0).equals("."))
                    {
                        create.table += tokens.remove(0);
                        create.table += tokens.remove(0);
                    }
                }
                else if(create != null && token.equals("("))
                {
                    create.columns = new JSONArray<>();
                }
                else if(create != null && token.equals(")"))
                {
                    tables.add(create);
                    
                    create = null;
                }
                else if(create != null && create.columns != null)
                {
                    column = new CreateColumn();
                    
                    column.name = token;
                    column.type = tokens.remove(0);
                    column.nullable = "";
                    
                    if(tokens.get(0).startsWith("("))
                    {
                        while(!column.type.endsWith(")")) column.type += tokens.remove(0);
                    }
                    
                    if(tokens.get(0).startsWith("IDENTITY"))
                    {
                        column.identity = tokens.remove(0);
                        
                        while(!column.identity.endsWith(")")) column.identity += tokens.remove(0);
                    }
                    
                    while(!tokens.isEmpty() && !tokens.get(0).equals(",") && !tokens.get(0).equals(")"))
                    {
                        if(column.nullable.length() > 0) column.nullable += " ";
                        
                        column.nullable += tokens.remove(0);
                    }
                    
                    if(!tokens.isEmpty() && tokens.get(0).equals(",")) tokens.remove(0);

                    create.columns.add(column);
                }
            }
        }
        
        return tables;
    }
    
    public static JSONArray<CreateTable> getCreateTables()
    {
        var source = new FileSource("AdventureWorks/raw/instawdbdw.sql");
        var sql = source.readString("UTF-16");
        
        return parse(sql);
    }
    
    public static CreateTable getCreateTable(String name)
    {
        for(CreateTable table : getCreateTables())
        {
            if(table.getTableName().equalsIgnoreCase(name))
            {
                return table;
            }
        }
        
        return null;
    }
    
    public static class CreateTable
    {
        public String table;
        public List<CreateColumn> columns;
        
        public String getTableName()
        {
            var split = this.table.split("\\.");
            
            return split[split.length-1];
        }
        
        public String[] getColumnNames()
        {
            var names = new JSONArray<String>();
            
            for(CreateColumn column : columns) names.add(column.name);
            
            return names.toArray(String[]::new);
        }
        
        public JSONObject toJSONObject()
        {
            var record = new JSONObject();
            var array = new JSONArray<JSONObject>();
            
            for(CreateColumn column : columns)
            {
                array.add(column.toJSONObject());
            }
            
            record.put("table", table);
            record.put("columns", array);
            
            return record;
        }
        
        public String toSQL()
        {
            var buffer = new StringBuffer();
            var table = getTableName();
            var first = true;
            
            buffer.append("create table ").append(table).append(" (\n");
            
            for(CreateColumn column : columns)
            {
                if(!first) buffer.append(",\n");
                
                buffer.append("    ").append(column.toSQL());
                
                first = false;
            }
            
            buffer.append("\n);");
            
            return buffer.toString();
        }
        
        @Override
        public String toString()
        {
            return toSQL();
        }
    }
    
    public static class CreateColumn
    {
        public String name;
        public String type;
        public String identity;
        public String nullable;
        
        public JSONObject toJSONObject()
        {
            var record = new JSONObject();
            
            record.put("name", name);
            record.put("type", type);
            record.put("identity", identity);
            record.put("nullable", nullable);
            
            return record;
        }
        
        public String getType()
        {
            String type = this.type.toLowerCase();
            
            if(type.startsWith("nvarchar")) type = type.replace("nvarchar", "varchar");
            if(type.startsWith("tinyint")) type = type.replace("tinyint", "int");
            if(type.startsWith("smallint")) type = type.replace("smallint", "int");
            
            return type;
        }
        
        public String toSQL()
        {
            String type = getType();
            
            if(identity != null && identity.startsWith("IDENTITY"))
            {
                return name + " " + type + " Primary Key";
            }
            
            if(nullable == null || nullable.equals("NULL"))
            {
                return name + " " + type;
            }
            
            return name + " " + type + " " + nullable;
        }
        
        @Override
        public String toString()
        {
            return toSQL();
        }
    }
}
