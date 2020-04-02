package database_hw;

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

		if (pageSize < recordSize) {
			System.out.println("Minimum page size: " + recordSize);
			System.exit(0);
		}
		
		
	}

}
