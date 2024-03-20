import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import pandas as pd

def cast_size_clean(df):
    mti_cleaning_df = df.withColumn("size_category", f.regexp_extract("cast_size", r"\((.*?)\)", 1)) \
        .withColumn("cast_size_detail", f.regexp_replace("cast_size", r"\(.*?\)", "")) \
        .withColumn("cast_size_detail", f.split("cast_size", "\\s+")) \
        .withColumn("size_category", f.when(f.col("size_category") == "", None).otherwise(f.col("size_category"))) \
        .withColumn("cast_size_detail", f.when(f.size(f.col("cast_size_detail")) > 0, f.col("cast_size_detail")[0]).otherwise(None))

    df = mti_cleaning_df.withColumnRenamed("cast_size_detail", "cast_size_category")
    size_cleaned_df = df.withColumnRenamed("size_category", "cast_size_detail")
    return size_cleaned_df

def extract_names(billing):
    """
    Extracts names from a billing dictionary into separate lists based on keywords.
    Skips rows containing "NaN" values.

    Args:
        billing (str): A JSON string representing the billing dictionary.

    Returns:
        tuple: A tuple containing four lists: music, lyrics, book, and others.
    """

    music = []
    lyrics = []
    book = []
    others = []

    if billing is None:  
        return None, None, None, None
    
    try:
        billing_dict = eval(billing)
    except (NameError, SyntaxError):
        return None, None, None, None

    for key, value in billing_dict.items():
        if all(val != None for val in value):  # Check if all values are not NaN
            if "music" in key.lower() and "lyrics" in key.lower() and "book" in key.lower():
                music.extend(value)
                lyrics.extend(value)
                book.extend(value)
            elif "music" in key.lower() and "lyrics" in key.lower():
                music.extend(value)
                lyrics.extend(value)
            elif "music" in key.lower() and "book" in key.lower():
                music.extend(value)
                book.extend(value)
            elif "lyrics" in key.lower() and "book" in key.lower():
                lyrics.extend(value)
                book.extend(value)
            elif "music" in key.lower():
                music.extend(value)
            elif "lyrics" in key.lower():
                lyrics.extend(value)
            elif "book" in key.lower():
                book.extend(value)
            else:
                others.extend(value)
        else:
            music = None
            lyrics = None
            book = None
            others = None
    return music if music else None, lyrics if lyrics else None, book if book else None, others if others else None  # Replace empty lists with None


def count_genders(genders):
    male_count = 0
    female_count = 0
    other_count = 0

    if genders is None:
        return [male_count, female_count, other_count]

    for gender in genders:
        if gender is not None:
            if gender.lower() == "male":
                male_count += 1
            elif gender.lower() == "female":
                female_count += 1
            else:
                other_count += 1

    return [male_count, female_count, other_count]


def remove_duplicates(tags):
    if tags is None:
        return None
    return list(set(tags))

def mti_data_cleaning_main(mti_raw_data): 
    schema = "array<struct<Name:string, Age:string, Gender:string, Description:string>>"
    mti_parsed_df = mti_raw_data.withColumn("parsed_characters", f.from_json(f.col("characters"), schema))
    
    size_cleaned_df = cast_size_clean(mti_parsed_df)

    extract_names_udf = f.udf(extract_names, returnType=ArrayType(StringType()))

    author_cleaned_df = size_cleaned_df.withColumn("music_by", extract_names_udf("Billing")[0]) \
        .withColumn("lyrics_by", extract_names_udf("Billing")[1]) \
        .withColumn("book_by", extract_names_udf("Billing")[2]) \
        .withColumn("other_authors", extract_names_udf("Billing")[3])

    count_genders_udf = f.udf(count_genders, ArrayType(IntegerType()))

    mti_character_count_added_df = author_cleaned_df.withColumn("CharacterGenderCount", count_genders_udf(f.col("parsed_characters.Gender")))

    mti_cleaned_df = mti_character_count_added_df.withColumn("male_character_count", f.col("CharacterGenderCount")[0]) \
                                                 .withColumn("female_character_count", f.col("CharacterGenderCount")[1]) \
                                                 .withColumn("other_character_count", f.col("CharacterGenderCount")[2])

    mti_cleaned_df = mti_cleaned_df.drop("CharacterGenderCount", "parsed_characters")

    remove_duplicates_udf = f.udf(remove_duplicates, ArrayType(StringType()))

    tags_split_df = mti_cleaned_df.withColumn("tags_array", f.split(f.regexp_replace(f.col("tags"), "[\\[\\]']", ""), ", "))

    mti_final_cleaned_df = tags_split_df.withColumn("tags", remove_duplicates_udf("tags_array"))

    mti_final_cleaned_df = mti_final_cleaned_df.drop("tags_array")

    return mti_cleaned_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("mti-data-cleaning").getOrCreate()
    mti_raw_data = spark.read.parquet("/home/fm-pc-lt-302/find_a_play_etl/data/raw_data/mti/mti_raw_data.csv")
    mti_clean_data = mti_data_cleaning_main(mti_raw_data)
    try:
        print("Writing data in parquet format")
        mti_clean_data.coalesce(1).write.parquet('../data/clean_data/test.parquet')
    except:
        print("could not save the data in parquet format.")
    
    try:
        print("writing data in csv format.")
        mti_clean_data.coalesce(1).write.csv('../data/clean_data/test.csv', header=True, quote='"', escape='"')
        print("Writing data completed.")
    except:
        print("could not write the data in csv foramt.")
