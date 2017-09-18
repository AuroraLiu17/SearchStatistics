package stat;

import java.util.Map;

import stat.DataContract.ActiveUser;

public class ActiveUserReader extends DataReaderBase {
	
	private static final String RAW_DATA_FOLDER_PATH = "raw/active_user_data";
	private static final String COLUMN_DATE = "date";
	private static final String COLUMN_UUID = "uuid";
	private static final String COLUMN_LOG_COUNT = "logCount";
	private static final int DATA_TYPE = DataContract.DataDBInfo.DATA_TYPE_ACTIVE_USER;
	private static final String INSERT_FORMAT = "INSERT INTO %s (" 
			+ ActiveUser.DATE + "," + ActiveUser.UUID + "," + ActiveUser.LOG_COUNT
			+ ") VALUES ('%s', '%s', %s)";

	@Override
	public String getRawDataFolderPath() {
		return RAW_DATA_FOLDER_PATH;
	}

	@Override
	public int getDataType() {
		return DATA_TYPE;
	}

	@Override
	protected String getCreateTableSql(String tableName) {
		return "CREATE TABLE " + tableName + " ("
				+ ActiveUser.ID + " integer PRIMARY KEY,"
				+ ActiveUser.DATE + " text not null,"
				+ ActiveUser.UUID + " text not null,"
				+ ActiveUser.LOG_COUNT + " integer"
				+ ")";
	}

	@Override
	protected String getDateInfo(Map<String, String> rowValues) {
		return rowValues.get(COLUMN_DATE);
	}

	@Override
	protected String getInsertRowSql(String tableName, Map<String, String> rowData) {
		return String.format(INSERT_FORMAT, tableName,
				rowData.get(COLUMN_DATE), rowData.get(COLUMN_UUID), rowData.get(COLUMN_LOG_COUNT));
	}
	
}
