package stat;

import java.util.Map;

import stat.DataContract.NewUser;

public class NewUserReader extends DataReaderBase {
	
	private static final String RAW_DATA_FOLDER_PATH = "raw/new_user_data";
	private static final String COLUMN_DATE = "date";
	private static final String COLUMN_UUID = "uuid";
	private static final String COLUMN_REQUEST_COUNT = "requestCount";
	private static final int DATA_TYPE = DataContract.DataDBInfo.DATA_TYPE_NEW_USER;
	private static final String INSERT_FORMAT = "INSERT INTO %s (" 
			+ NewUser.DATE + "," + NewUser.UUID + "," + NewUser.REQUEST_COUNT
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
				+ NewUser.ID + " integer PRIMARY KEY,"
				+ NewUser.DATE + " text not null,"
				+ NewUser.UUID + " text not null,"
				+ NewUser.REQUEST_COUNT + " integer"
				+ ")";
	}

	@Override
	protected String getDateInfo(Map<String, String> rowValues) {
		return rowValues.get(COLUMN_DATE);
	}

	@Override
	protected String getInsertRowSql(String tableName, Map<String, String> rowData) {
		return String.format(INSERT_FORMAT, tableName, rowData.get(COLUMN_DATE), rowData.get(COLUMN_UUID), rowData.get(COLUMN_REQUEST_COUNT));
	}
	
}
