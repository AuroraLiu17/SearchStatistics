package stat;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import stat.RawDataReader.ReaderCallback;
import utils.TextUtils;

public abstract class DataReaderBase {
	
	public abstract String getRawDataFolderPath();

	public abstract int getDataType();
	
	protected abstract String getCreateTableSql(String tableName);
	
	protected abstract String getDateInfo(Map<String, String> rowValues);
	
	protected abstract String getInsertRowSql(String tableName, Map<String, String> rowData);

	protected static final int BATCH_UPDATE_COUNT = 100;
	private Map<String, String> dateTableNameMap;
	private Connection connection;
	private File[] files;

	public boolean readAll() {
		if (!prepare()) {
			return false;
		}
		
		try {
			for (File file : files) {
				if (file.isHidden() || file.isDirectory()) {
					continue;
				}
				if (!new DataFileReaderBase(file).read()) {
					return false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			SqliteHelper.silentlyClose(connection);
		}
		return true;
	}
	
	private boolean prepare() {
		File folder = new File(getRawDataFolderPath());
		if (!folder.exists() || !folder.isDirectory() || (files = folder.listFiles()) == null) {
			System.out.println("Nothing to read about under " + getRawDataFolderPath());
			return true;
		}

		try {
			connection = DataDBHelper.getDBConnectionForDataType(getDataType());
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		dateTableNameMap = new HashMap<>();
		return true;
	}
	
	private String getTableForDate(String date) {
		String tableName = null;
		if (!dateTableNameMap.containsKey(date)) {
			tableName = getTable(date);
			if (TextUtils.isEmpty(tableName)) {
				return null;
			}
			dateTableNameMap.put(date, tableName);
		} else {
			tableName = dateTableNameMap.get(date);
		}
		return tableName;
	}
	
	private String getTable(String date) {
		String tableName = DataDBHelper.getTableNameFor(getDataType(), date);
		if (!DataDBHelper.doTableExist(connection, tableName)) {
			boolean result = SqliteHelper.executeUpdate(connection, getCreateTableSql(tableName));
			if (!result) {
				System.err.println("Something wrong happened while creating table " + tableName);
				return null;
			} else {
				System.out.println("Successfully create table " + tableName);
			}
			result = Main.onRecordTable(getDataType(), date, DataDBHelper.getDBNameForDataType(getDataType()), tableName);
			if (!result) {
				System.err.println("Something wrong happened while recording table " + tableName);
				return null;
			}
		}
		return tableName;
	}
	
	private class DataFileReaderBase {
		private Statement statement;
		private int batchCount = 0;
		private File file;
		private boolean errorOccurred = false;
		
		public DataFileReaderBase(File file) {
			this.file = file;
		}
		
		public boolean read() {
			System.out.println("==== Start reading file " + file.getPath() + " ======");
			try {
				statement = connection.createStatement();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			
			ReaderCallback callback = new ReaderCallback() {
				private String date = null;
				private String sql = null;
				private int result = 0;
				
				@Override
				public boolean onReadRow(int rowIndex, Map<String, String> rowData) {
					date = getDateInfo(rowData);
					String tableName = getTableForDate(date);
					if (TextUtils.isEmpty(tableName)) {
						errorOccurred = true;
						return false;
					}
					
					try {
						sql = getInsertRowSql(tableName, rowData);
						result = statement.executeUpdate(sql);
						System.out.println(String.format("On insert row %d into %s result %d", rowIndex,  tableName, result));
						if (result > 0) {
							batchCount++;
						}
						if (batchCount >= BATCH_UPDATE_COUNT) {
							connection.commit();
						}
					} catch (SQLException e) {
						e.printStackTrace();
						errorOccurred = true;
						return false;
					}
					return true;
				}
			};
			
			RawDataReader.read(file.getAbsolutePath(), callback);
			
			SqliteHelper.silentlyClose(statement);
			System.out.println("==== Done reading file " + file.getPath() + " " + errorOccurred + " ======");
			return !errorOccurred;
		}
	}

}
