package com.github.s262316.dbtools.tableexport;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.github.s262316.dbtools.tableorder.Table;
import com.github.s262316.dbtools.tableorder.TableDependencies;
import com.google.common.graph.Traverser;

/**
 * generates SQL inserts for tables
 * 
 * Specify tables to be extracted in <dbname>-filter.txt
 * 
 * Specify table order in <dbname>-table-order.txt
 * There can be more tables in <dbname>-table-order.txt than <dbname>-table-filter.txt
 *
 * Also creates inserts for dependent tables
 * 
 */
@SpringBootApplication
public class ExportTablesApplication implements CommandLineRunner
{
	@Autowired
	DataSource dataSource;
	@Autowired
	NamedParameterJdbcTemplate jdbcTemplate;
	@Autowired
	TableExporter tableExporter;
	int currentRow;
	@Value("${dbName}")
	String databaseName;
	
	@Override
	public void run(String... args) throws Exception
	{
		List<String> allTables=FileUtils.readLines(new File(databaseName+"-table-order.txt"));
		List<String> requiredTables=FileUtils.readLines(new File(databaseName+"-table-filter.txt"));
		
		for(String tableName : allTables)
		{
			System.out.println(tableName);
			if(requiredTables.contains(tableName))
			{
				Table tableDependencies=TableDependencies.dependencies(tableName, databaseName);
				if(tableDependencies!=null)
				{
					Traverser<Table> traverser = Traverser.forTree(node -> node.getDependentTables());
					traverser.depthFirstPostOrder(tableDependencies).forEach(v -> tableExporter.writeTableIfNotAlready(v.getTableName()));
				}
				
				tableExporter.writeTableIfNotAlready(tableName);
			}
		}
	}

	public static void main(String args[]) throws IOException
	{
		SpringApplication.run(ExportTablesApplication.class, args);		
	}
	
	
}
