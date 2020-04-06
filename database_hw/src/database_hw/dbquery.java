package database_hw;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class dbquery{
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

		int pageOffset = 0;
		int recordPerPage = pageSize / recordSize;

		byte[] byteQuery = args[0].getBytes();
		byte[] paddedQuery = Arrays.copyOf(byteQuery, buildingNameSize);

		int numberRecordFromPage = 0;
		long startTime = 0;

		RandomAccessFile in = null;
		try {
			startTime = System.nanoTime();
			in = new RandomAccessFile("heap." + pageSize, "r");
			startTime = System.currentTimeMillis();
			while (true) {
				int currOffset = numberRecordFromPage * recordSize + pageOffset * pageSize;
				if (currOffset > in.length()) {
					break;
				}

				in.seek(currOffset + cencusYearSize + blockIDSize + propertyIDSize + basePropertyIDSize);
				byte[] readByte = new byte[buildingNameSize];
				in.read(readByte);

				int cencusYear = -1;
				int blockID = -1;
				int propertyID = -1;
				int basePropertyID = -1;
				String buildingName = "";
				String streetAddress = "";
				String clueSmallArea = "";
				int constructionYear = -1;
				int refurbishedYear = -1;
				int numberOfFloors = -1;
				String predominantSpaceUse = "";
				String accessibilityType = "";
				String accessibilityTypeDescription = "";
				int accessibilityRating = -1;
				int bicycleSpaces = -1;
				int hasShowers = -1;
				double xCoordinate = 0;
				double yCoordinate = 0;
				if (Arrays.equals(readByte, paddedQuery)) {
					int tempOffset = currOffset;

					in.seek(tempOffset);
					byte[] cencusYearByte = new byte[cencusYearSize];
					in.read(cencusYearByte);
					cencusYear = ByteBuffer.wrap(cencusYearByte).getInt();

					tempOffset += cencusYearSize;
					in.seek(tempOffset);
					byte[] blockIDByte = new byte[blockIDSize];
					in.read(blockIDByte);
					blockID = ByteBuffer.wrap(blockIDByte).getInt();

					tempOffset += blockIDSize;
					in.seek(tempOffset);
					byte[] propertyIDByte = new byte[propertyIDSize];
					in.read(propertyIDByte);
					propertyID = ByteBuffer.wrap(propertyIDByte).getInt();

					tempOffset += propertyIDSize;
					in.seek(tempOffset);
					byte[] basePropertyIDByte = new byte[basePropertyIDSize];
					in.read(basePropertyIDByte);
					basePropertyID = ByteBuffer.wrap(basePropertyIDByte).getInt();

					tempOffset += basePropertyIDSize;
					in.seek(tempOffset);
					byte[] buildingNameByte = new byte[buildingNameSize];
					in.read(buildingNameByte);
					buildingName = new String(buildingNameByte);

					tempOffset += buildingNameSize;
					in.seek(tempOffset);
					byte[] streetAddressByte = new byte[streetAddressSize];
					in.read(streetAddressByte);
					streetAddress = new String(streetAddressByte);

					tempOffset += streetAddressSize;
					in.seek(tempOffset);
					byte[] clueSmallAreaByte = new byte[clueSmallAreaSize];
					in.read(clueSmallAreaByte);
					clueSmallArea = new String(clueSmallAreaByte);

					tempOffset += clueSmallAreaSize;
					in.seek(tempOffset);
					byte[] constructionYearByte = new byte[constructionYearSize];
					in.read(constructionYearByte);
					constructionYear = ByteBuffer.wrap(constructionYearByte).getInt();

					tempOffset += constructionYearSize;
					in.seek(tempOffset);
					byte[] refurbishedYearByte = new byte[refurbishedYearSize];
					in.read(refurbishedYearByte);
					refurbishedYear = ByteBuffer.wrap(refurbishedYearByte).getInt();

					tempOffset += refurbishedYearSize;
					in.seek(tempOffset);
					byte[] numberOfFloorsByte = new byte[numberOfFloorsSize];
					in.read(numberOfFloorsByte);
					numberOfFloors = ByteBuffer.wrap(numberOfFloorsByte).getInt();

					tempOffset += numberOfFloorsSize;
					in.seek(tempOffset);
					byte[] predominantSpaceUseByte = new byte[predominantSpaceUseSize];
					in.read(predominantSpaceUseByte);
					predominantSpaceUse = new String(predominantSpaceUseByte);

					tempOffset += predominantSpaceUseSize;
					in.seek(tempOffset);
					byte[] accessibilityTypeByte = new byte[accessibilityTypeSize];
					in.read(accessibilityTypeByte);
					accessibilityType = new String(accessibilityTypeByte);

					tempOffset += accessibilityTypeSize;
					in.seek(tempOffset);
					byte[] accessibilityTypeDescriptionByte = new byte[accessibilityTypeDescriptionSize];
					in.read(accessibilityTypeDescriptionByte);
					accessibilityTypeDescription = new String(accessibilityTypeDescriptionByte);

					tempOffset += accessibilityTypeDescriptionSize;
					in.seek(tempOffset);
					byte[] accessibilityRatingByte = new byte[accessibilityRatingSize];
					in.read(accessibilityRatingByte);
					accessibilityRating = ByteBuffer.wrap(accessibilityRatingByte).getInt();

					tempOffset += accessibilityRatingSize;
					in.seek(tempOffset);
					byte[] bicycleSpacesByte = new byte[bicycleSpacesSize];
					in.read(bicycleSpacesByte);
					bicycleSpaces = ByteBuffer.wrap(bicycleSpacesByte).getInt();

					tempOffset += bicycleSpacesSize;
					in.seek(tempOffset);
					byte[] hasShowersByte = new byte[hasShowersSize];
					in.read(hasShowersByte);
					hasShowers = ByteBuffer.wrap(hasShowersByte).getInt();

					tempOffset += hasShowersSize;
					in.seek(tempOffset);
					byte[] xCoordinateByte = new byte[xCoordinateSize];
					in.read(xCoordinateByte);
					xCoordinate = ByteBuffer.wrap(xCoordinateByte).getDouble();

					tempOffset += xCoordinateSize;
					in.seek(tempOffset);
					byte[] yCoordinateByte = new byte[yCoordinateSize];
					in.read(yCoordinateByte);
					yCoordinate = ByteBuffer.wrap(yCoordinateByte).getDouble();

					System.out.println("Record found at " + (System.currentTimeMillis() - startTime) / 1000.0 + "s");
					if (cencusYear > -1)
						System.out.println("cencusYear: " + cencusYear);
					if (blockID > -1)
						System.out.println("blockID: " + blockID);
					if (propertyID > -1)
						System.out.println("propertyID: " + propertyID);
					if (basePropertyID > -1)
						System.out.println("basePropertyID: " + basePropertyID);
					if (!buildingName.isEmpty())
						System.out.println("buildingName: " + buildingName);
					if (!streetAddress.isEmpty())
						System.out.println("streetAddress: " + streetAddress);
					if (!clueSmallArea.isEmpty())
						System.out.println("clueSmallArea: " + clueSmallArea);
					if (constructionYear > -1)
						System.out.println("constructionYear: " + constructionYear);
					if (refurbishedYear > -1)
						System.out.println("refurbishedYear: " + refurbishedYear);
					if (numberOfFloors > -1)
						System.out.println("numberOfFloors: " + numberOfFloors);
					if (!predominantSpaceUse.isEmpty())
						System.out.println("predominantSpaceUse: " + predominantSpaceUse);
					if (!accessibilityType.isEmpty())
						System.out.println("accessibilityType: " + accessibilityType);
					if (!accessibilityTypeDescription.isEmpty())
						System.out.println("accessibilityTypeDescription: " + accessibilityTypeDescription);
					if (accessibilityRating > -1)
						System.out.println("accessibilityRating: " + accessibilityRating);
					if (bicycleSpaces > -1)
						System.out.println("bicycleSpaces: " + bicycleSpaces);
					if (hasShowers > -1)
						System.out.println("hasShowers: " + hasShowers);
					if (xCoordinate != 0)
						System.out.println("xCoordinate: " + xCoordinate);
					if (yCoordinate != 0)
						System.out.print("yCoordinate: " + yCoordinate);
					System.out.println("\n");
				}
				numberRecordFromPage++;
				if (numberRecordFromPage == recordPerPage) {
					numberRecordFromPage = 0;
					pageOffset++;
				}
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
