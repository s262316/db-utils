package com.github.s262316.dbtools.tableexport.csv;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.github.s262316.dbtools.tableexport.ColumnDescr;
import com.github.s262316.dbtools.tableexport.ColumnNamesAndTypes;
import com.github.s262316.dbtools.tableexport.SqlOverride;
import com.github.s262316.dbtools.tableexport.TableExporter;
import com.google.common.collect.Iterables;

@Component
@ConditionalOnProperty("exportAsCsv")
public class CsvExporter implements TableExporter
{
	@Autowired
	NamedParameterJdbcTemplate jdbcTemplate;
	@Value("${outputFolder}")
	File outputFolder;
	@Autowired
	SqlOverride sqlOverride;
	
	private Set<String> doneTables=new HashSet<>();

	@Override
	public void writeTableIfNotAlready(String tableName)
	{
		if(!doneTables.contains(tableName))
		{
			jdbcTemplate.query(sqlOverride.sqlFor(tableName), new ResultSetExtractor<Void>()
			{
				@Override
				public Void extractData(ResultSet rs) throws SQLException, DataAccessException
				{
					try
					{
						List<ColumnDescr> cols=jdbcTemplate.query(sqlOverride.sqlFor(tableName), new ColumnNamesAndTypes());
						List<String> headerNames=cols.stream().map(ColumnDescr::getName).collect(Collectors.toList());

						CSVPrinter csvPrinter = CSVFormat.DEFAULT
								.withHeader(Iterables.toArray(headerNames, String.class))
								.print(new File(outputFolder, tableName+".csv"), StandardCharsets.UTF_8);

						OracleCsvDataFormatter myConverter=new OracleCsvDataFormatter(cols);
						
						MapHandler mh=new MapHandler();
						Map<String, Object> row=mh.handle(rs);
						if(row!=null)
							row.replaceAll(myConverter);
						
						while(row!=null)
						{
							csvPrinter.printRecord(row.values());
							row=mh.handle(rs);
							if(row!=null)
								row.replaceAll(myConverter);
						}

						csvPrinter.close();
						return null;
					}
					catch (IOException e)
					{
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			});

			doneTables.add(tableName);
		}
	}
}
