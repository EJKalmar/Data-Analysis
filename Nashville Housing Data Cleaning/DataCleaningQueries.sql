/*
Cleaning Data in SQL

Nashville Housing Dataset used: https://www.kaggle.com/code/tmthyjames/nashville-housing-data-1/data

*/

---------------------------------------------------------------------------------------------------------------

-- Standardize Date Format

Select SaleDate, CONVERT(date, SaleDate)
From DataCleaningProject..NashvilleHousing

Update NashvilleHousing
SET SaleDate = CONVERT(date, SaleDate)

-- For whatever reason, the above update statement is not updating our table. Creating a new column will get the job done.
ALTER TABLE NashvilleHousing
Add SaleDateConverted Date

Update NashvilleHousing
Set SaleDateConverted = CONVERT(Date, SaleDate)

Select SaleDate, SaleDateConverted
From DataCleaningProject..NashvilleHousing


---------------------------------------------------------------------------------------------------------------

-- Filling in Null Property Address Data
-- Missing data retrieved from other entries with the same parcel ID.

Select ParcelId, PropertyAddress
From DataCleaningProject..NashvilleHousing
order by ParcelID

select t1.ParcelID, t1.PropertyAddress, t2.ParcelID, t2.PropertyAddress, ISNULL(t1.PropertyAddress, t2.PropertyAddress)
From DataCleaningProject..NashvilleHousing t1
JOIN DataCleaningProject..NashvilleHousing t2
	on t1.ParcelID = t2.ParcelID
	and t1.[UniqueID ] <> t2.[UniqueID ]
Where t1.PropertyAddress is null

Update t1
SET PropertyAddress = ISNULL(t1.PropertyAddress, t2.PropertyAddress)
From DataCleaningProject..NashvilleHousing t1
JOIN DataCleaningProject..NashvilleHousing t2
	on t1.ParcelID = t2.ParcelID
	and t1.[UniqueID ] <> t2.[UniqueID ]
Where t1.PropertyAddress is null


---------------------------------------------------------------------------------------------------------------

-- Separating Address Into Individual Columns (Address, City, State) using SUBSTRING and CHARINDEX

Select PropertyAddress
From DataCleaningProject..NashvilleHousing

Select
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1) as Address,
SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) +1, LEN(PropertyAddress)) as City
From DataCleaningProject..NashvilleHousing

ALTER TABLE DataCleaningProject..NashvilleHousing
Add PropertySplitCity Nvarchar(255);

ALTER TABLE DataCleaningProject..NashvilleHousing
Add PropertySplitAddress Nvarchar(255);

Update DataCleaningProject..NashvilleHousing
Set PropertySplitCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) +1, LEN(PropertyAddress))


Update DataCleaningProject..NashvilleHousing
Set PropertySplitAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1)


---------------------------------------------------------------------------------------------------------------

-- Separating Owner Address Into Address, City, State using PARSENAME and REPLACE

Select OwnerAddress
From DataCleaningProject..NashvilleHousing

Select 
PARSENAME(REPLACE(OwnerAddress, ',','.'), 3),
PARSENAME(REPLACE(OwnerAddress, ',','.'), 2),
PARSENAME(REPLACE(OwnerAddress, ',','.'), 1)
From DataCleaningProject..NashvilleHousing

ALTER TABLE DataCleaningProject..NashvilleHousing
Add OwnerSplitCity Nvarchar(255);

ALTER TABLE DataCleaningProject..NashvilleHousing
Add OwnerSplitAddress Nvarchar(255);

ALTER TABLE DataCleaningProject..NashvilleHousing
Add OwnerSplitState Nvarchar(255);

Update DataCleaningProject..NashvilleHousing
Set OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress, ',','.'), 2)

Update DataCleaningProject..NashvilleHousing
Set OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress, ',','.'), 3)

Update DataCleaningProject..NashvilleHousing
Set OwnerSplitState = PARSENAME(REPLACE(OwnerAddress, ',','.'), 1)


---------------------------------------------------------------------------------------------------------------

-- Change Y and N to Yes and No in "SoldAsVacant" field

select Distinct SoldAsVacant, count(SoldAsVacant)
From DataCleaningProject..NashvilleHousing
Group by SoldAsVacant

Select SoldAsVacant,
CASE
When SoldAsVacant = 'Y' THEN 'Yes'
When SoldAsVacant = 'N' THEN 'No'
Else SoldAsVacant
END
From DataCleaningProject..NashvilleHousing

Update DataCleaningProject..NashvilleHousing
Set SoldAsVacant =
CASE
When SoldAsVacant = 'Y' THEN 'Yes'
When SoldAsVacant = 'N' THEN 'No'
Else SoldAsVacant
END


---------------------------------------------------------------------------------------------------------------

-- Remove Duplicates

With RowNumCTE as 
	(
	Select *, 
		ROW_NUMBER() OVER (
		PARTITION BY
			ParcelID,
			PropertyAddress,
			SalePrice,
			SaleDate,
			LegalReference
		ORDER By
		UniqueID
		) row_num
	From DataCleaningProject..NashvilleHousing
	-- Order by ParcelID
	)
Select *
From RowNumCTE
Where row_num > 1
--Order by PropertyAddress

With RowNumCTE as 
	(
	Select *, 
		ROW_NUMBER() OVER (
		PARTITION BY
			ParcelID,
			PropertyAddress,
			SalePrice,
			SaleDate,
			LegalReference
		ORDER By
		UniqueID
		) row_num
	From DataCleaningProject..NashvilleHousing
	-- Order by ParcelID
	)
DELETE
From RowNumCTE
Where row_num > 1
--Order by PropertyAddress


---------------------------------------------------------------------------------------------------------------

-- Delete Old Columns

Select *
From DataCleaningProject..NashvilleHousing

ALTER TABLE DataCleaningProject..NashvilleHousing
DROP COLUMN OwnerAddress, PropertyAddress, SaleDate

---------------------------------------------------------------------------------------------------------------