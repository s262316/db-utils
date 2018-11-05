package com.github.s262316.dbtools.ddlexport;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.datasource.DataSourceUtils;

/**
 * Creates DDL from a database schema
 * Tables and primary/foreign keys
 * Indexes
 * writes to a file named <dbname>_<currenttime>.ddl
 * 
 * Also writes the table dependencies to a file named <dbname>-dependencies.txt
 * 
 * Run with 2 args : dbname schemaname
 * 
 *
 */
@SpringBootApplication
public class DdlWriterApplication implements CommandLineRunner
{
	@Autowired
	DataSource dataSource;
	@Value("${dbName}")
	String dbName;
	@Value("${spring.datasource.username}")
	String schemaName;
	
    @Override
	public void run(String... args) throws Exception
	{
		Connection c=DataSourceUtils.getConnection(dataSource);
		TableDump.dumpDB(c, new File(dbName+"_"+System.currentTimeMillis()+".ddl"), schemaName, new File(dbName+"-dependencies.txt"));
	}
	
	public static void main(String args[]) throws IOException
	{
		SpringApplication.run(DdlWriterApplication.class, args);		
	}
}
