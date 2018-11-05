package com.github.s262316.dbtools.tableexport.csv;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.github.s262316.dbtools.tableexport.ColumnDescr;
import com.github.s262316.dbtools.tableexport.Formatting;

import oracle.sql.TIMESTAMP;

public class OracleCsvDataFormatter implements BiFunction<String, Object, Object>
{
	private Map<String, ColumnDescr> cols;
	
	public OracleCsvDataFormatter(List<ColumnDescr> cols)
	{
		this.cols=cols.stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
	}
	
	@Override
	public Object apply(String mapKey, Object mapValue)
	{
		try
		{
			switch(cols.get(mapKey).getSqlType())
			{
				case Types.VARCHAR:
				case Types.CHAR:
					return Formatting.formatforCsv((String)mapValue);
				case Types.INTEGER:
				case Types.NUMERIC:
					return Formatting.formatInt((BigDecimal)mapValue);
				case Types.DATE:
					return Formatting.formatDateForCsv((Timestamp)mapValue);
				case Types.TIMESTAMP:
					return Formatting.formatTimestampForCsv(((TIMESTAMP)mapValue).timestampValue());
				default:
					throw new RuntimeException(mapKey+" "+mapValue);
			}
		}
		catch(SQLException sqle)
		{
			sqle.printStackTrace();
			throw new RuntimeException(sqle);
		}
	}
}