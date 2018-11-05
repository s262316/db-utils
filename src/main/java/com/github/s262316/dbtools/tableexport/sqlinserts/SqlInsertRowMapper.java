package com.github.s262316.dbtools.tableexport.sqlinserts;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.RowMapper;

import com.github.s262316.dbtools.tableexport.ColumnDescr;
import com.github.s262316.dbtools.tableexport.Formatting;

public class SqlInsertRowMapper implements RowMapper<String>
{
	private String tableName;
	private List<ColumnDescr> cols;
	
	public SqlInsertRowMapper(String tableName, List<ColumnDescr> cols)
	{
		this.tableName=tableName;
		this.cols=cols;
	}
	
	@Override
	public String mapRow(ResultSet rs, int rowNum) throws SQLException
	{
		String columnNames=cols.stream().map(ColumnDescr::getName).collect(Collectors.joining(","));
		String values=cols.stream().map(v -> doOneValue(v, rs)).collect(Collectors.joining(","));
		
		return String.format("insert into %s (%s) values (%s);", tableName, columnNames, values);
	}
	
	public String doOneValue(ColumnDescr cd, ResultSet rs)
	{
		try
		{
			switch(cd.getSqlType())
			{
				case Types.VARCHAR:
					return Formatting.format(rs.getString(cd.getName()));
				case Types.CHAR:
					return Formatting.format(rs.getString(cd.getName()));
				case Types.INTEGER:
					return Formatting.formatInt(rs, rs.getInt(cd.getName()));
				case Types.NUMERIC:
					return Formatting.formatLong(rs, rs.getLong(cd.getName()));
				case Types.DATE:
					return Formatting.format(rs.getDate(cd.getName()));
				case Types.TIMESTAMP:
					return Formatting.formatTimestamp(rs.getDate(cd.getName()));
				default:
					throw new RuntimeException(cd.getName()+" "+cd.getSqlType());
			}
		}
		catch(SQLException sqle)
		{
			throw new RuntimeException(sqle);
		}
	}
	
}
