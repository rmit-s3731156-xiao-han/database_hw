package database_hw;

import java.io.RandomAccessFile;

public class dbquery {
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
		int numberRecordFromPage = 0;
		RandomAccessFile in = null;
		try {
			in = new RandomAccessFile("heap." + pageSize, "r");
			while (true) {
				int currOffset = numberRecordFromPage * recordSize + pageOffset * pageSize;
				if (currOffset > in.length()) {
					break;
				}

				in.seek(currOffset + cencusYearSize + blockIDSize + propertyIDSize + basePropertyIDSize);
				byte[] readByte = new byte[buildingNameSize];
				in.read(readByte);
			}
		}catch(Exception e){
		}
	}
}

