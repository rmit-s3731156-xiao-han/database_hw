package database_hw;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

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
				try {
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

					if (cencusYear.isEmpty()) {
						output.write(ByteBuffer.allocate(cencusYearSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(cencusYearSize).putInt(Integer.parseInt(cencusYear)).array());
					}
					if (blockID.isEmpty()) {
						output.write(ByteBuffer.allocate(blockIDSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(blockIDSize).putInt(Integer.parseInt(blockID)).array());
					}
					if (propertyID.isEmpty()) {
						output.write(ByteBuffer.allocate(propertyIDSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(propertyIDSize).putInt(Integer.parseInt(propertyID)).array());
					}
					if (basePropertyID.isEmpty()) {
						output.write(ByteBuffer.allocate(basePropertyIDSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(basePropertyIDSize).putInt(Integer.parseInt(basePropertyID))
								.array());
					}

					output.write(Arrays.copyOf(buildingName.getBytes(), buildingNameSize));
					output.write(Arrays.copyOf(streetAddress.getBytes(), streetAddressSize));
					output.write(Arrays.copyOf(clueSmallArea.getBytes(), clueSmallAreaSize));
					if (constructionYear.isEmpty()) {
						output.write(ByteBuffer.allocate(constructionYearSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(constructionYearSize)
								.putInt(Integer.parseInt(constructionYear)).array());
					}
					if (refurbishedYear.isEmpty()) {
						output.write(ByteBuffer.allocate(refurbishedYearSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(refurbishedYearSize).putInt(Integer.parseInt(refurbishedYear))
								.array());
					}
					if (numberOfFloors.isEmpty()) {
						output.write(ByteBuffer.allocate(numberOfFloorsSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(numberOfFloorsSize).putInt(Integer.parseInt(numberOfFloors))
								.array());
					}
					output.write(Arrays.copyOf(predominantSpaceUse.getBytes(), predominantSpaceUseSize));
					output.write(Arrays.copyOf(accessibilityType.getBytes(), accessibilityTypeSize));
					output.write(
							Arrays.copyOf(accessibilityTypeDescription.getBytes(), accessibilityTypeDescriptionSize));
					if (accessibilityRating.isEmpty()) {
						output.write(ByteBuffer.allocate(accessibilityRatingSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(accessibilityRatingSize)
								.putInt(Integer.parseInt(accessibilityRating)).array());
					}
					if (bicycleSpaces.isEmpty()) {
						output.write(ByteBuffer.allocate(bicycleSpacesSize).putInt(-1).array());
					} else {
						output.write(
								ByteBuffer.allocate(bicycleSpacesSize).putInt(Integer.parseInt(bicycleSpaces)).array());
					}
					if (hasShowers.isEmpty()) {
						output.write(ByteBuffer.allocate(hasShowersSize).putInt(-1).array());
					} else {
						output.write(ByteBuffer.allocate(hasShowersSize).putInt(Integer.parseInt(hasShowers)).array());
					}
					if (xCoordinate.isEmpty()) {
						output.write(ByteBuffer.allocate(xCoordinateSize).putDouble(0).array());
					} else {
						output.write(ByteBuffer.allocate(xCoordinateSize).putDouble(Double.parseDouble(xCoordinate))
								.array());
					}
					if (yCoordinate.isEmpty()) {
						output.write(ByteBuffer.allocate(yCoordinateSize).putDouble(0).array());
					} else {
						output.write(ByteBuffer.allocate(yCoordinateSize).putDouble(Double.parseDouble(yCoordinate))
								.array());
					}
					currRec++;
					if (currRec == recordPerPage) {
						currRec = 0;
						for (int i = 0; i < remainderPage; i++) {
							output.write(0);
						}
					}
				} catch (IndexOutOfBoundsException e) {
				}
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
