package com.github.s262316.dbtools.tableorder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.graph.Traverser;

/**
 * This utility class converts a table-dependencies.txt file into
 * an insert-order list of tables
 * 
 * Put that string in a file named <dbname>-table-dependencies.txt and a file named <dbname>-order.txt will be created
 * 
 * Ensure <dbname>-order.txt is empty because it will be appended to.
 * 
 * Run with the dbname as the argument, e.g. mydb1
 * 
 * 
 */
public class TableDependencies
{
	public static Table dependencies(String tableName, String databaseName) throws Exception
	{
		Map<String, Table> tables=read(databaseName);
		return tables.get(tableName);
	}
	
	public static void main(String args[]) throws Exception
	{	
		Map<String, Table> tables=read(args[0]);
		
		Traverser<Table> traverser = Traverser.forTree(node -> node.getDependentTables());
		for(Map.Entry<String, Table> e : tables.entrySet())
		{
			traverser.depthFirstPostOrder(e.getValue()).forEach(v -> printIfNotAlready(v.tableName, args[0]));
		}
	}
	
	static Set<String> printedTables=new HashSet<>(); 
	
	public static void printIfNotAlready(String tableName, String databaseName)
	{
		try
		{
			if(!printedTables.contains(tableName))
			{
				printedTables.add(tableName);
				FileUtils.writeStringToFile(new File(databaseName+"-table-order.txt"), tableName+"\r\n", true);
			}
		}
		catch(IOException ioe)
		{
			throw new RuntimeException(ioe);
		}
	}

	private static Map<String, Table> read(String databaseName) throws IOException
	{
		String tableDependencies=FileUtils.readFileToString(new File(databaseName+"-table-dependencies.txt"));
		
		Map<String, Table> tables=new HashMap<>();
		
		tableDependencies=StringUtils.remove(tableDependencies, "{");
		tableDependencies=StringUtils.remove(tableDependencies, "}");
		tableDependencies=StringUtils.remove(tableDependencies, " ");
		String pairs[]=StringUtils.splitByWholeSeparator(tableDependencies, "],");

		for(String p : pairs)
		{
			p=StringUtils.remove(p, "[");
			p=StringUtils.remove(p, "]");
			p=StringUtils.trim(p);
			
			String keyValue[]=StringUtils.split(p, "=");
			List<Table> dependentTables=Arrays.stream(StringUtils.split(keyValue[1], ","))
				.map(v -> tables.getOrDefault(v, new Table(v)))
				.collect(Collectors.toList());
			
			for(Table t : dependentTables)
				tables.putIfAbsent(t.tableName, t);
			
			tables.merge(keyValue[0], new Table(keyValue[0], dependentTables), (t1, t2) -> t1.merge(t2.dependentTables));
		}
		
		return tables;
	}
}
