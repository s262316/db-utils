package com.github.s262316.dbtools.ddlexport;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class TableDump
{

    /** Extract the schema
     * 
     * Taken from http://www.isocra.com/articles/db2sql.java, with additions for indexes and vendor
     * 
     * Copyright Isocra Ltd 2004
     * 
     * @param dbConn A database connection
     * @return A string representation of the schema
     */
    public static void dumpDB(Connection dbConn, File outputFile, String schema, File dependencyOutputFile) throws IOException
    {        
        // Default to not having a quote character
        String columnNameQuote = "";
        DatabaseMetaData dbMetaData = null;
    
        try {
        	dbMetaData = dbConn.getMetaData();
            String catalog = null;
            String tables = null;
            ListMultimap<String, String> dependencies=ArrayListMultimap.create();
            List<String> alterTableAddForeignKeySql=new ArrayList<>();
            
            String hostname = "";
            try {
                InetAddress addr = InetAddress.getLocalHost();
                hostname = addr.getHostName();
            } catch (UnknownHostException e) {
            }
            
            try( ResultSet rs = dbMetaData.getTables(catalog, schema, tables, null) ) {
                if (! rs.next()) 
                    System.err.println("Unable to find any tables matching: catalog="+catalog+" schema="+schema+" tables="+tables);
                else {
                    // Right, we have some tables, so we can go to work.
                    // the details we have are
                    // TABLE_CAT String => table catalog (may be null)
                    // TABLE_SCHEM String => table schema (may be null)
                    // TABLE_NAME String => table name
                    // TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
                    // REMARKS String => explanatory comment on the table
                    // TYPE_CAT String => the types catalog (may be null)
                    // TYPE_SCHEM String => the types schema (may be null)
                    // TYPE_NAME String => type name (may be null)
                    // SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
                    // REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
                    // We will ignore the schema and stuff, because people might want to import it somewhere else
                    // We will also ignore any tables that aren't of type TABLE for now.
                    // We use a do-while because we've already caled rs.next to see if there are any rows
                    do {
                        String tableName = rs.getString("TABLE_NAME");
                        String tableType = rs.getString("TABLE_TYPE");
                        if ("TABLE".equalsIgnoreCase(tableType)) {
                        	
                            System.out.println("table "+tableName);
                            
                            FileUtils.write(outputFile, "\n-- "+tableName, StandardCharsets.UTF_8, true);
                            FileUtils.write(outputFile, "\nCREATE TABLE "+tableName+" (\n", StandardCharsets.UTF_8, true);
                            try ( ResultSet tableMetaData = dbMetaData.getColumns(null, schema, tableName, "%") ) {
                                boolean firstLine = true;
                                while (tableMetaData.next()) {
                                    if (firstLine) {
                                        firstLine = false;
                                    } else {
                                        // If we're not the first line, then finish the previous line with a comma
                                    	FileUtils.write(outputFile, ",\n", StandardCharsets.UTF_8, true);
                                    }
                                    String columnName = tableMetaData.getString("COLUMN_NAME");
                                    String columnType = tableMetaData.getString("TYPE_NAME");
                                    // WARNING: this may give daft answers for some types on some databases (eg JDBC-ODBC link)
                                    int columnSize = tableMetaData.getInt("COLUMN_SIZE");
                                    String nullable = tableMetaData.getString("IS_NULLABLE");
                                    String nullString = " ";
                                    if ("NO".equalsIgnoreCase(nullable)) {
                                        nullString = "NOT NULL";
                                    }
                                    
                                    if(columnType.equalsIgnoreCase("DATE") || columnType.equalsIgnoreCase("TIMESTAMP") || columnType.equalsIgnoreCase("CLOB"))
                                    	FileUtils.write(outputFile, "    "+columnNameQuote+columnName+columnNameQuote+" "+columnType+" "+nullString, StandardCharsets.UTF_8, true);
                                    else
                                    	FileUtils.write(outputFile, "    "+columnNameQuote+columnName+columnNameQuote+" "+columnType+" ("+columnSize+")"+" "+nullString, StandardCharsets.UTF_8, true);
                                }
                            }

                            System.out.println("getting primary keys for "+tableName);
                            
                            ResultSet primaryKeys =null;
                            // Now we need to put the primary key constraint
                            try {
                            	primaryKeys=dbMetaData.getPrimaryKeys(catalog, schema, tableName);
                                // What we might get:
                                // TABLE_CAT String => table catalog (may be null)
                                // TABLE_SCHEM String => table schema (may be null)
                                // TABLE_NAME String => table name
                                // COLUMN_NAME String => column name
                                // KEY_SEQ short => sequence number within primary key
                                // PK_NAME String => primary key name (may be null)
                                String primaryKeyName = null;
                                StringBuffer primaryKeyColumns = new StringBuffer();
                                while (primaryKeys.next()) {
                                    String thisKeyName = primaryKeys.getString("PK_NAME");
                                    if ((thisKeyName != null && primaryKeyName == null)
                                        || (thisKeyName == null && primaryKeyName != null)
                                        || (thisKeyName != null && ! thisKeyName.equals(primaryKeyName))
                                        || (primaryKeyName != null && ! primaryKeyName.equals(thisKeyName))) {
                                        // the keynames aren't the same, so output all that we have so far (if anything)
                                        // and start a new primary key entry
                                        if (primaryKeyColumns.length() > 0) {
                                            // There's something to output
                                        	FileUtils.write(outputFile, ",\n    CONSTRAINT ", StandardCharsets.UTF_8, true);
                                            if (primaryKeyName != null) { FileUtils.write(outputFile, primaryKeyName, StandardCharsets.UTF_8, true); }
                                            FileUtils.write(outputFile, " PRIMARY KEY ", StandardCharsets.UTF_8, true);
                                            FileUtils.write(outputFile, "("+primaryKeyColumns.toString()+")", StandardCharsets.UTF_8, true);
                                        }
                                        // Start again with the new name
                                        primaryKeyColumns = new StringBuffer();
                                        primaryKeyName = thisKeyName;
                                    }
                                    // Now append the column
                                    if (primaryKeyColumns.length() > 0) {
                                        primaryKeyColumns.append(", ");
                                    }
                                    primaryKeyColumns.append(primaryKeys.getString("COLUMN_NAME"));
                                }
                                if (primaryKeyColumns.length() > 0) {
                                    // There's something to output
                                	FileUtils.write(outputFile, ",\n    CONSTRAINT ", StandardCharsets.UTF_8, true);
                                    if (primaryKeyName != null) { FileUtils.write(outputFile, primaryKeyName, StandardCharsets.UTF_8, true); }
                                    FileUtils.write(outputFile, " PRIMARY KEY ", StandardCharsets.UTF_8, true);
                                    FileUtils.write(outputFile, " ("+primaryKeyColumns.toString()+")", StandardCharsets.UTF_8, true);
                                }
                                
                            } catch (SQLException e) {
                                // NB you will get this exception with the JDBC-ODBC link because it says
                                // [Microsoft][ODBC Driver Manager] Driver does not support this function
                                System.err.println("Unable to get primary keys for table "+tableName+" because "+e);
                            }
                            finally
                            {
                            	if(primaryKeys!=null)
                            		primaryKeys.close();
                            }

                            FileUtils.write(outputFile, "\n);\n", StandardCharsets.UTF_8, true);

                            System.out.println("getting foreign keys for "+tableName);

                            final String addForeignKeySql="ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)";
                            String foreignKeyName=null;
                            String foreignKeyTableName=null;
                            List<String> pkColumnNames=new ArrayList<>();
                            List<String> fkColumnNames=new ArrayList<>();
                            int rowNumber=1;
                            
                        	ResultSet foreignKeys=dbMetaData.getImportedKeys(catalog, schema, tableName);
                        	while(foreignKeys.next())
                        	{
                        		if(foreignKeys.getInt("KEY_SEQ")==1)
                        		{
                        			if(rowNumber>1)
                        			{
	                                    String fkAlterSql=String.format(addForeignKeySql,
	                                    		tableName, foreignKeyName,
	                                    		Joiner.on(",").join(fkColumnNames),
	                                    		foreignKeyTableName,
	                                    		Joiner.on(",").join(pkColumnNames));
	
	    	                            alterTableAddForeignKeySql.add(fkAlterSql+";");
                        			}
                        			
	                                foreignKeyName=foreignKeys.getString("FK_NAME");
	                                foreignKeyTableName=foreignKeys.getString("PKTABLE_NAME");
	                                
	                                pkColumnNames.clear();
	                                fkColumnNames.clear(); 
	                                
	                                dependencies.put(tableName, foreignKeyTableName);                                
                        		}
                        		
                               	pkColumnNames.add(foreignKeys.getString("PKCOLUMN_NAME"));
                               	fkColumnNames.add(foreignKeys.getString("FKCOLUMN_NAME"));
                               	
                               	rowNumber++;
                        	}

                        	if(!pkColumnNames.isEmpty())
                        	{
	                            String fkAlterSql=String.format(addForeignKeySql,
	                            		tableName, foreignKeyName,
	                            		Joiner.on(",").join(fkColumnNames),
	                            		foreignKeyTableName,
	                            		Joiner.on(",").join(pkColumnNames));
	
	                            alterTableAddForeignKeySql.add(fkAlterSql+";");
                        	}
                        	
                            System.out.println("getting indexes keys for "+tableName);

                            /**
                             * Get the indexes for this table
                             */
                            ResultSet indexes=null;
                            
                            try
                            {
                                indexes = dbMetaData.getIndexInfo(null, schema, tableName, false, false);
                                Map<String, List<String>> index2col = new HashMap<String,List<String>>();
                                Map<String, Boolean> index2unique = new HashMap<String, Boolean>();
                                while (indexes.next())
                                {
                                    Boolean nonUnique = indexes.getBoolean("NON_UNIQUE");
                                    String name = indexes.getString("INDEX_NAME");
                                    String col = indexes.getString("COLUMN_NAME");
                                    if (!index2col.containsKey(name))
                                        index2col.put(name, new ArrayList<String>());
                                    index2col.get(name).add(col);
                                    index2unique.put(name, !nonUnique);
                                }
                                for (String index: index2col.keySet())
                                {
                                	if(index!=null && !index.endsWith("_PK"))
                                	{
	                                    List<String> cols = index2col.get(index);
	                                    FileUtils.write(outputFile, "CREATE ", StandardCharsets.UTF_8, true);
	                                    if (index2unique.get(index)) FileUtils.write(outputFile, "UNIQUE ", StandardCharsets.UTF_8, true);
	                                    FileUtils.write(outputFile, "INDEX " + index + " ON " + tableName + " (", StandardCharsets.UTF_8, true);
	                                    FileUtils.write(outputFile, String.join(", ", cols.toArray(new String[]{})), StandardCharsets.UTF_8, true);
	                                    FileUtils.write(outputFile, ");\n", StandardCharsets.UTF_8, true);
                                	}
                                }
                            }
                            catch (SQLException e)
                            {
                                System.err.println("Unable to get indexes for table "+tableName+" because "+e);
                            }
                            finally
                            {
                            	if(indexes!=null)
                            		indexes.close();
                            }
                        }
                    } while (rs.next());
                }
                
                FileUtils.writeLines(outputFile, alterTableAddForeignKeySql, true);

                FileUtils.write(dependencyOutputFile, dependencies.toString());
            }

        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use Options | File Templates.
        }
    }

}

