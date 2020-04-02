package database_hw;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class dbload {

	public static void main(String[] args) {

		int pageSize = Integer.parseInt(args[1]);
		int cencusYearSize = 4;
		int blockIDSize = 4;
		int propertyIDSize = 4;
		int basePropertyIDSize = 4;
		int buildingNameSize = 50;
		int streetAddressSize = 50;
		int clueSmallAreaSize = 50;
		int constructionYearSize = 4;
		int refurbishedYearSize = 4;
		int numberOfFloorsSize = 4;
		int predominantSpaceUseSize = 50;
		int accessibilityTypeSize = 50;
		int accessibilityTypeDescriptionSize = 100;
		int accessibilityRatingSize = 4;
		int bicycleSpacesSize = 4;
		int hasShowersSize = 4;
		int xCoordinateSize = 8;
		int yCoordinateSize = 8;

		int recordSize = cencusYearSize + blockIDSize + propertyIDSize + basePropertyIDSize + buildingNameSize
				+ streetAddressSize + clueSmallAreaSize + constructionYearSize + refurbishedYearSize
				+ numberOfFloorsSize + predominantSpaceUseSize + accessibilityTypeSize
				+ accessibilityTypeDescriptionSize + accessibilityRatingSize + bicycleSpacesSize + hasShowersSize
				+ xCoordinateSize + yCoordinateSize;

		int recordPerPage = pageSize / recordSize;
		int remainderPage = pageSize % recordSize;
		long startTime = 0;
		long endTime = 0;

		if (pageSize < recordSize) {
			System.out.println("Minimum page size: " + recordSize);
			System.exit(0);
		}

		BufferedReader reader = null;
		DataOutputStream output = null;
		
		String line;
		String cencusYear;
		String blockID;
		String propertyID;
		String basePropertyID;
		String buildingName;
		String streetAddress;
		String clueSmallArea;
		String constructionYear;
		String refurbishedYear;
		String numberOfFloors;
		String predominantSpaceUse;
		String accessibilityType;
		String accessibilityTypeDescription;
		String accessibilityRating;
		String bicycleSpaces;
		String hasShowers;
		String xCoordinate;
		String yCoordinate;
		
		startTime = System.currentTimeMillis();
		
		try {
			reader = new BufferedReader(new FileReader(args[2]));
			output = new DataOutputStream(new FileOutputStream("heap." + pageSize));
			reader.readLine();
			int currRec = 0;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",");
				if (split.length != 20) {
					continue;
				}

				cencusYear = split[0];
				blockID = split[1];
				propertyID = split[2];
				basePropertyID = split[3];
				buildingName = split[4];
				streetAddress = split[5];
				clueSmallArea = split[6];
				constructionYear = split[7];
				refurbishedYear = split[8];
				numberOfFloors = split[9];
				predominantSpaceUse = split[10];
				accessibilityType = split[11];
				accessibilityTypeDescription = split[12];
				accessibilityRating = split[13];
				bicycleSpaces = split[14];
				hasShowers = split[15];
				xCoordinate = split[16];
				yCoordinate = split[17];

			}
			reader.close();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (endTime - startTime) / 1000.0 + "s");
	}
}
