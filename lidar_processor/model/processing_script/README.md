# LAZ file processing workflow

Required Python libraries for running the scripts:
* `numpy`
* `pyproj`
* `laspy`
* `pdal`

Install them in your `micromamba` (or `conda`) environment with the following command:

```bash
micromamba install -c conda-forge numpy pyproj laspy pdal
```

## Fix LAZ files with missing CRS

Python script `fix_laz_file.py` takes an input LAZ file and performs the following steps:
1. Remove overlapping points
2. Assign correct CRS (EPSG:3301)
3. Writes out the fixed file in LAZ version 1.4

```bash
# fix_laz_file.py input_file output_file
python fix_laz_file.py 447696_2019_tava.laz 447696_2019_tava_fixed.laz
```

## Reclassify points based on conditions

Script `reclassify_laz_file.py` takes the fixed LAZ file and performs the following steps based on a PDAL pipeline:
1. Detect points within ETAK sea polygons (layer `E_201_meri_a`). The attribute `WithinSea` is added to LAZ points, indicating whether the points are within (1) sea or not (0).
2. Detect points within 13 m buffers of ETAK overhead (`nimipinge` is 110, 220 or 330) powerlines (layer `E_601_elektriliin_j`). The attribute `WithinPowerline` is added to LAZ points, indicating whether the points are within (1) powerline buffers or not (0).
3. Detect points within polygons of ETAK water bodies (layers `E_202_seisuveekogu_a` and `E_203_vooluveekogu_a`). The attribute `WithinWaterBody` is added to LAZ points, indicating whether the points are within (1) them or not (0).
4. Detect points within polygons of ETAK buildings (layer `E_401_hoone_ka`) and other structures (layer `E_403_muu_rajatis_ka`). The attribute `WithinBuilding` is added to LAZ points, indicating whether the points are within (1) them or not (0).
5. Calculate NDVI for each point using the following workflow:
    1. Use the NDVI raster as input for `filters.hag_dem` to calculate temporary `HeightAboveGround` (HAG) attribute by subtracting NDVI from `Z`.
    2. Copy this temporary HAG into new attribute `NDVI`.
    3. Extract actual `NDVI` by subtracting `NDVI` from `Z`.
6. Calculate actual HAG based on the corresponding DEM file. This overwrites the existing temporary HAG derived from the NDVI raster. VRT also works as input. Use 0 as HAG where the points are within sea or elevation raster had missing values (-9999).
7. Assign new classification values to attribute `Classification` based on conditions (NDVI, HAG, location within buildings etc.). The original values are retained in attribute `OriginalClassification`.

```bash
# reclassify_laz_file.py input_file output_file dem_file etak_file ndvi_file
python reclassify_laz_file.py 447696_2019_tava_fixed.laz 447696_2019_tava_reclassified.laz 54494_dem_1m_2017-2020.tif ETAK_EESTI_GPKG_2020_01_04/ETAK_EESTI_GPKG.gpkg dcube_pub_estonia_sentinel2_ndvi_2019_est_s2_ndvi_2019-06-01_2019-08-31_cog.tif
```