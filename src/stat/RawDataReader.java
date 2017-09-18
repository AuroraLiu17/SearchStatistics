package stat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import utils.TextUtils;

/**
 * Given a file path and read data by line and return it throw callback
 * @author liuxiaohui
 */
public class RawDataReader {
	
	public interface ReaderCallback {
		/**
		 * Callback method when read to row
		 * @param rowIndex
		 * @param rowData
		 * @return true if reader should continue to read next line
		 * 		   false if reader should stop reading
		 */
		boolean onReadRow(int rowIndex, Map<String, String> rowData);
	}
	
	public static void read(String filePath, ReaderCallback callback) {
		if (TextUtils.isEmpty(filePath) || callback == null) {
			System.out.println("Empty file path to read or empty callback!");
			return;
		}
		
		File file = new File(filePath);
		if (!file.exists()) {
			System.out.println("Invalid file path, no file found!");
			return;
		}
		
		FileInputStream fileInputStream = null;
		InputStreamReader inputStreamReader = null;
		BufferedReader bufferedReader = null;
		try {
			fileInputStream = new FileInputStream(file);
			inputStreamReader = new InputStreamReader(fileInputStream);
			bufferedReader = new BufferedReader(inputStreamReader);
			
			String line = null;
			String[] columnTitles = null;
			String[] columnValues = null;
			
			// First line is redundant
			bufferedReader.readLine();
			// read second line first to read column titles
			line = bufferedReader.readLine();
			if (TextUtils.isEmpty(line)) {
				System.out.println("Empty file, no column title found!");
				return;
			}
			
			columnTitles = line.split("\t");
			if (columnTitles == null || columnTitles.length <= 0) {
				System.out.println("Wrong column title format!");
				return;
			}

			System.out.println(String.format("On reading column titles %d, %s", columnTitles.length, line));
			
			Map<String, String> rowValueMap = new HashMap<String, String>(columnTitles.length);
			for (int lineCount = 0; !TextUtils.isEmpty(line = bufferedReader.readLine()); lineCount++) {
				rowValueMap.clear();
				columnValues = line.split("\t");
				System.out.print("\nOn read " + lineCount + " line: ");
				for (int i = 0; i < columnValues.length; i++) {
					rowValueMap.put(columnTitles[i], columnValues[i]);
					System.out.print(columnTitles[i] + ":" + columnValues[i] + " ");
				}
				if (!callback.onReadRow(lineCount, rowValueMap)) {
					break;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println("Something wrong happened while reading file " + filePath);
			e.printStackTrace();
		} finally {
			try {
				bufferedReader.close();
				inputStreamReader.close();
				fileInputStream.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
