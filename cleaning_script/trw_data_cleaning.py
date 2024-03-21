import findspark
findspark.init()
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import col, regexp_extract, explode,from_json,expr,trim,when, udf, regexp_replace

def extract_author(book_by):
        authors = []
        for item in eval(book_by):
            if item.startswith("Book:") or "Film written by" in item or "Book &" in item or "Book" in item:
                authors.append(item.split(":")[1].strip())
        return authors

def extract_musician(music_by):
    musicians = []
    for item in eval(music_by):
        if "Music"in item or "Music," in item or "Music &" in item:
            musicians.append(item.split(":")[1].strip())
    return musicians

def extract_lyrician(lyrics_by):
    lyrician = []
    for item in eval(lyrics_by):
        if "Lyrics"in item or "Lyrics by," in item or "Lyrics &" in item:
            lyrician.append(item.split(":")[1].strip())
    return lyrician

def filter_and_slice_cast_list(cast_list):
    # Convert string representation of list to actual list
    cast_list = eval(cast_list)
    filtered_cast_list = [item for item in cast_list if len(item.split()) <= 3]
    sliced_cast_list = filtered_cast_list[1:-1]
    return sliced_cast_list

def cleaning_trw_main():
    spark = SparkSession.builder.appName("trw-cleaning").getOrCreate()
    trw_df = spark.read.format("parquet").option("header","true").option("inferSchema", "true") \
          .option("mode", "FAILFAST").load("/home/fm-pc-lt-342/Documents/Fusemachines/BLG/raw_data/trw/theatricalrights.parquet")
    

    extract_author_udf = udf(extract_author, StringType())

    filter_values_udf = udf(filter_and_slice_cast_list, ArrayType(StringType()))

    #Extracting authors from book_and_music_by
    trw_df = trw_df.withColumn("author", extract_author_udf(col("book_and_music_by")))

    #Extracting Music by from book_and_music_by
    trw_df = trw_df.withColumn("music_by",extract_musicain_udf(col("book_and_music_by")))

    #Extracting Lyrics by from book_and_music_by
    trw_df = trw_df.withColumn("lyrics_by",extract_lyrician_udf(col("book_and_music_by")))

    trw_df = trw_df.withColumn("characters", filter_values_udf(col("cast_list")))

    #Extracting male and female cast numners
    trw_df = trw_df.withColumn("male_cast", 
        when(col("casting_info").rlike("M|MALE|Men"), 1)
        .otherwise(regexp_extract(col("casting_info"), r"(\d+) male", 1))
    ).withColumn("female_cast", when(col("casting_info").rlike("W|F|FEMALE|WoMen"), 1)
        .otherwise(regexp_extract(col("casting_info"), r"(\d+) female", 1)))
    
    # Removing \xa0 and \\\\u2028 from the synopsis
    trw_df = trw_df.withColumn("synopsis",regexp_replace(trw_df.synopsis,"\[|\]", ""))
    trw_df = trw_df.withColumn("synopsis",regexp_replace(trw_df.synopsis,"\\\\xa0", ""))
    trw_df = trw_df.withColumn("synopsis",regexp_replace(trw_df.synopsis,"\\\\u2028",""))

    #Removing new line in show_credits
    trw_df = trw_df.withColumn('show_credits', f.trim(f.regexp_replace('show_credits', '^\n+|\n+$', ''))).\
                withColumn('show_credits', f.regexp_replace('show_credits', '\n', ','))

    trw_df = trw_df.withColumn('synopsis', f.trim(f.regexp_replace('synopsis', '\n', ' ')))
    
    trw_df = trw_df.withColumn("overall_cast_number",trw_df.male_cast + trw_df.female_cast).withColumnRenamed("related_shows_urls","similar_shows")

    trw_df = trw_df.select('show_title','show_type','show_url','author','music_by','lyrics_by','male_cast','female_cast','overall_cast_number','characters','apply_for_license','show_credits','songs_list','similar_shows','upcoming_productions','resources','synopsis')

    trw_df.write.format("parquet").mode("overwrite")\
                .save(".parquet")
    
if __name__ == "__main__":
    cleaning_trw_main()