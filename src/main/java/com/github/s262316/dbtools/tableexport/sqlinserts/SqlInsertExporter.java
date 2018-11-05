package com.github.s262316.dbtools.tableexport.sqlinserts;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.github.s262316.dbtools.tableexport.ColumnDescr;
import com.github.s262316.dbtools.tableexport.ColumnNamesAndTypes;
import com.github.s262316.dbtools.tableexport.TableExporter;

// exports all tables included to SQL insert statements in a single script
// writes to stdout
@Component
@ConditionalOnProperty("exportAsSqlInserts")
public class SqlInsertExporter implements TableExporter
{
	@Autowired
	NamedParameterJdbcTemplate jdbcTemplate;
	
	private Set<String> doneTables=new HashSet<>();
	
	@Override
	public void writeTableIfNotAlready(String tableName)
	{
		if(!doneTables.contains(tableName))
		{
			List<ColumnDescr> cols=jdbcTemplate.query("select * from "+tableName, new ColumnNamesAndTypes());
			
			SqlInsertRowMapper sqlMapping=new SqlInsertRowMapper(tableName, cols);
			List<String> sqlInserts=jdbcTemplate.query("select * from "+tableName, sqlMapping);
			sqlInserts.forEach(System.out::println);

			doneTables.add(tableName);
		}
	}
}
