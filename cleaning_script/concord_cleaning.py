import findspark
findspark.init()
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, explode,from_json,expr,trim,when
from pyspark.sql import functions as f


def concord_cleaning(concord_df):
    # Assuming 'genre' column is currently a string type
    concord_df = concord_df.withColumn("genre_array", f.split(f.col("genre"), ", "))

    concord_df = concord_df.withColumn("genre", 
        f.concat(f.expr("transform(genre_array, x -> concat('[', x, ']'))")).cast("string"))

    #Replacing Extra [] from each genre
    concord_df = concord_df.withColumn("genre", f.regexp_replace(f.col("genre"), "\\[\\[", "\\[").alias("genre"))\
                            .withColumn("genre", f.regexp_replace(f.col("genre"), "\\]\\]", "\\]").alias("genre"))

    #Replacing [NaN] with NaN
    concord_df = concord_df.withColumn('genre', f.regexp_replace('genre', r'\[NaN\]', ""))

    #Target Audience clean
    concord_df = concord_df.withColumn("target_audience", f.split(f.col("target_audience"), ",\n"))
    concord_df=concord_df.withColumn('target_audience',f.col('target_audience').cast("string"))
    concord_df = concord_df.withColumn('target_audience', f.regexp_replace('target_audience', r'\[NaN\]', ""))

    #Seperating Male and Female Cast
    concord_df = concord_df.withColumn("male_cast", regexp_extract(col("cast_size"), r"(\d+)m", 1)).withColumn('female_cast',regexp_extract(col("cast_size"), r"(\d+)w", 1))

    #Seperating Authors Name only#
    #schema for the JSON-like structure
    authors_schema = "array<struct<author_id: int, fullname: string>>"
    concord_df=concord_df.withColumn("parsed_authors", from_json(col("authors"), authors_schema))
    # Extract full names from the parsed structure
    concord_df=concord_df.withColumn("author_names", expr("transform(parsed_authors, x -> x.fullname)")).drop('parsed_authors')


    #Seperate Image urls
    image_url_schema = "array<struct<image_id:string, image_type:string, image_height:int, image_url:string, image_credit:string>>"
    concord_df = concord_df.withColumn("parsed_images", from_json(col("image_urls"), image_url_schema)).drop('image_urls')

    # Extract image URLs from the parsed structure
    concord_df = concord_df.withColumn("image_urls", expr("transform(parsed_images, x -> x.image_url)")).drop('parsed_images')


    #Trim Columns
    concord_df = concord_df.withColumn("show_type",trim(concord_df.show_type))\
                    .withColumn("duration",trim(concord_df.duration))\
                    .withColumn("target_audience",trim(concord_df.target_audience))\
                    .withColumn("history",trim(concord_df.history))
    
    #Show Duration in minutes
    pattern = r'(\d+)\s*minutes'
    concord_df = concord_df.withColumn("duration", when(concord_df["duration"].rlike(pattern), regexp_extract(concord_df["duration"], pattern, 1)).otherwise(None))

    #Calculationg overall Cast numbers
    concord_df = concord_df.withColumn("overall_cast_number",concord_df.male_cast + concord_df.female_cast)

    #Trim new line in summary
    concord_df = concord_df.withColumn('summary', f.regexp_replace('summary', '\n', ' '))

    concord_df = concord_df.select('sid','show_title','show_url','male_cast','female_cast','overall_cast_number','show_type','genre','author_names','themes','duration','target_audience','awards','apply_for_license','summary','history')

    return concord_df

def rename_parquet_files(directory, new_name):
    for filename in os.listdir(directory):
        if filename.endswith('.parquet'):
            old_path = os.path.join(directory, filename)
            new_path = os.path.join(directory, new_name)
            os.rename(old_path, new_path)

def delete_files_except(directory, filename_to_keep):
    for filename in os.listdir(directory):
        if filename != filename_to_keep:
            file_path = os.path.join(directory, filename)
            os.remove(file_path)


def cleaning_script_main():
    spark = SparkSession.builder.appName("concord-data-cleaning").getOrCreate()
    concord_raw_data = spark.read.parquet("data/raw_data/concord/scraped_data.parquet")
    concord_clean_data = concord_cleaning(concord_raw_data)
    try:
        print("Writing data in parquet format")
        concord_clean_data.coalesce(1).write.parquet('data/clean_data/concord/', mode='overwrite')
    except Exception as e:
        print("could not save the data in parquet format.", e)

    directory = "data/clean_data/concord"
    new_filename = "concord_clean_data.parquet"

    rename_parquet_files(directory, new_filename)
    delete_files_except(directory, new_filename)


if __name__ == '__main__':
    cleaning_script_main()