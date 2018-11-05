package com.github.s262316.dbtools.tableexport;

import java.util.List;

public interface TableExporter
{
	public void writeTableIfNotAlready(String tableName);
}
