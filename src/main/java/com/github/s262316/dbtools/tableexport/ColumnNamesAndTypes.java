package com.github.s262316.dbtools.tableexport;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

public class ColumnNamesAndTypes implements ResultSetExtractor<List<ColumnDescr>>
{
	@Override
	public List<ColumnDescr> extractData(ResultSet rs) throws SQLException, DataAccessException
	{
		List<ColumnDescr> cols=new ArrayList<>();
		ResultSetMetaData rsmd=rs.getMetaData();
		
		for(int i=1; i<=rsmd.getColumnCount(); i++)
		{
			String colName=rsmd.getColumnName(i);
			int colType=rsmd.getColumnType(i);
			
			if(colType==Types.TIMESTAMP)
			{
				// all dates are returned as timestamps
				// this property seems to be the only way to distinguish a date from a timestamp
				if(rsmd.getColumnDisplaySize(i)==7)
				{
					colType=Types.DATE;
				}
			}
			

			cols.add(new ColumnDescr(colName, colType));
		}

		return cols;
	}	
}
