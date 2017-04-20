import os

out_dir = "/home/roman/Downloads/california_dem/all"
suf = ".tif"

for layer in iface.mapCanvas().layers():
    provider = layer.dataProvider()

    input_file = provider.dataSourceUri()
    lname = layer.name()
    # lname = "bbbb"
    output_file = os.path.join(out_dir, lname + suf)

    cmd_convert = """
    gdal_translate -a_srs EPSG:4267 -a_nodata 0 -of USGSDEM {input_file} {output_file}
    """.format(
        input_file=input_file,
        output_file=output_file
    )

    cmd_reproject = """
    gdalwarp -overwrite -s_srs EPSG:4267 -t_srs EPSG:4326 -of GTiff {input} {output}
    """.format(
        input=input_file,
        output=output_file
    )

    cmd_hillshade = """
    gdaldem hillshade -z 5 -s 111120 {input} {output} -combined
    """.format(
        input=input_file,
        output=output_file
    )

    print(cmd_hillshade)
    os.system(cmd_hillshade)

    # xMin, yMin - 123.089, 32.6282: xMax, yMax - 114.558, 41.9589


    import os

    input_layer = "/mnt/hdd/pycharm_projects/AmbienceDataGIS/shp/test_points.shp"

    cmd = """
        gdal_grid
        -zfield wd
        -l test_points
        - a linear
        -of GTiff
        {input}
        /mnt/hdd/pycharm_projects/AmbienceDataGIS/raster/wd4.tif
        """.format(
        input=input_layer
    )

    cmd2 = """
    gdal_grid -zfield wd -l test_points -txe -123.089 -114.558 -tye 41.9589 32.6282 -of GTiff /mnt/hdd/pycharm_projects/AmbienceDataGIS/shp/test_points.shp /mnt/hdd/pycharm_projects/AmbienceDataGIS/raster/wd6.tif
    """
    print(cmd2)
    os.system(cmd2)