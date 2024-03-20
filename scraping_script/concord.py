import os
import math
import time
import requests
import pandas as pd
from logger import setup_logger
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("concord_scraping_script").getOrCreate()

# Get response from url
def get_response(endpoint,params):
    try:
        if params !="":
            response = requests.get(endpoint,params=params)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            return response.json()
        else:
             response = requests.get(endpoint)
             soup = BeautifulSoup(response.content, "html.parser")
             return soup

    except requests.RequestException as e: 
            print(f"Request failed: {e}")
            return None
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# Raise exception
def return_exception(e):
    return f"Exception Occured as: {e}"


# Replace HTML entities
def clean_text(text):
    if text is None:
        return ""  

    else:
        soup = BeautifulSoup(text, 'html.parser')
        cleaned_text = soup.get_text(separator=' ', strip=True)
        return cleaned_text


# Get data from each plays
def scrape_plays(plays_json_content):
    data_collection=[]
    
    #Show id
    try:
        sid = plays_json_content.get("Id")
    except:
        sid = plays_json_content.get("ProductVariantModels")[0].get("")

    #Duration
    duration = plays_json_content.get('TitleAttributeDisplayModel').get('DescriptionDuration')

    #Target Audience
    target_audience = plays_json_content.get('TitleAttributeDisplayModel').get('DescriptionTargetAudience')

    #awards
    awards = {}
    awards_data = plays_json_content.get('TitleAttributeDisplayModel').get('DescriptionAwards')
    # clean = re.compile('<.*?>')
    # awards_data_clean =re.sub(clean,'',awards_data)
    awards_data_clean = clean_text(awards_data)
    awards_data = awards_data_clean.split(", ")
    awards ={'awards':awards_data}

    # apply for liscense
    apply_for_liscense = f'''https://license.concordtheatricals.com/performance-license/license?productId={sid}'''

    #Brief Synopsis
    brief_synop = plays_json_content.get('ShortDescription')
    brief_synopsis = clean_text(brief_synop)

    #Summary
    summary_raw = plays_json_content.get('FullDescription')
    summary = clean_text(summary_raw)

    #History
    history_raw = plays_json_content.get('TitleAttributeDisplayModel').get('DescriptionProductionInformation')
    history = clean_text(history_raw)

    #Peromrance Group
    performance_group = []
    performance_group_list = plays_json_content.get('TitleAttributeDisplayModel').get('PerformanceGroups')
    for item in performance_group_list:
        performance_group.append(item.get('Name'))
    
    #minimum liscense fee
    minimum_license_fee = plays_json_content.get('LicensingFeeDisplay')
    minimum_license_fee = minimum_license_fee.replace('<strong>', "").replace("</strong>","").split(':')
    # minimum_license_fee = minimum_license_fee[1].strip()
    if len(minimum_license_fee) >= 2:
        minimum_license_fee = minimum_license_fee[1].strip()
    else:
         minimum_license_fee = None 


    data = {
        'show_id':sid,
        'duration': duration,
        'target_audience': target_audience,
        'awards':awards,
        'apply_for_license':apply_for_liscense,
        'minimum_license_fee': minimum_license_fee,
        'brief_synopsis':brief_synopsis,
        'summary': summary,
        'history':history,
        'performance_groups':performance_group,
         }
    data_collection.append(data)
    return data_collection


# Get Data from landing page
def scrape_page(json_content):
    data_collection = []
    
    for item in json_content:

        #Show id
        sid = item.get('Id')

        #Show Title
        Name = item.get('Name')
        # show_title.append(Name)

        #Show url
        show_base_url = f"https://www.concordtheatricals.com/p/{sid}/{item.get('SeName')}"
        

        #Cast Size
        TitleCasting = item.get('TitleCasting')        

        #Show type
        show_type = item.get('TitleTypeOfPlayAndGenre')
        show_type = show_type.split(", ")
        type_of_show = show_type[0]

        #Genre
        genre_key = item.get('TitleTypeOfPlayAndGenre')
        genre_key = genre_key.split(", ")

        if len(genre_key)>1:
            genre = genre_key[1]
        else:
            genre=""
        
        # #Author
        author_key = item.get('Authors')

        author_details = []
        for author in author_key:
            author_id = author.get('Id')
            full_name = author.get('FirstName') + ' ' + author.get('LastName')
            author_details.append({'author_id': author_id, 'fullname': full_name})

        #Themes
        theme_key = item.get('Tags')
        themes = [theme for theme in theme_key]

        #Images url
        image_key = item.get('ProductImages')
        image_urls = []
        for image in image_key:
            image_id = image.get('Id')
            image_type = image.get('Type')
            image_height = image.get('Height')
            image_url = image.get('ImageUrl')
            image_credit = image.get('PhotoCredit')
            image_urls.append({'image_id':image_id, 'image_type':image_type,'image_height':image_height,'image_url':image_url,'image_credit':image_credit})

        #Short Description
        short_description = item.get('ShortDescription')

        data = {
        'sid': sid,
        'show_title': Name,
        'show_url': show_base_url,
        'cast_size':TitleCasting,
        'show_type':type_of_show,
        'genre':genre,
        'authors':author_details,
        'themes':themes,
        'image_urls':image_urls,
        'short_description':short_description


         }
        data_collection.append(data)
    return data_collection

# Filter new plays and shows
def filter_new_shows_only(df_left, df_right, join_col):
    merged_df = df_right.merge(df_left[[join_col]], how='left', on=join_col, indicator=True)
    merged_df = merged_df[merged_df['_merge'] == 'left_only']
    merged_df.drop('_merge', axis=1, inplace=True)
    return merged_df

# Start Scraping
def start_concord():

    #### INITIAL PART ###
    global df # Declare df as global
    
    concord_log = setup_logger("concord_scraping", f"../logs/concord_scraping.log")
    concord_log.info("\n ******Concord Scraping Script Started*****")


    # dir = r'../data/raw_data/concord/'
    dir = r'/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/concord/'
    directory = os.path.dirname(dir)

    if not os.path.exists(directory):
        print(f"{dir}  iscreated.")
        os.makedirs(directory)


    page_size = 150
    concord_url= f"https://www.concordtheatricals.com/api/v1/search"
    params = {
    "licensable": "true",
    "pageSize": 18
    }

    #For total product
    get_json_res = get_response(concord_url,params = params)
    totalproduct=get_json_res.get('ProductTotal')

    total_page = math.ceil(totalproduct/page_size)
    print("toatl plays are:",totalproduct)
    print("total page to scrape:",total_page)
    
    concord_log.info(f"\n ******Total Plays: {totalproduct}*****")
    concord_log.info(f"\n ******Total Page to Scrape: {total_page}*****")


    file_path  = f"{dir}initial_scrapped.csv"
    file_exist = os.path.isfile(file_path)
    if file_exist:
        df_old = pd.read_csv(file_path)
    else:
        df_old = pd.DataFrame(columns=['sid','show_title', 'show_url','cast_size', 'show_type', 'genre', 'authors', 'themes','image_urls','short_description'])
    pd.DataFrame(columns=['sid','show_title', 'show_url','cast_size', 'show_type', 'genre', 'authors', 'themes','image_urls','short_description']).to_csv(file_path, index = False)
    i=0
    while i<total_page:
        json_content = get_response(concord_url,params={"licensable": "true","pageSize":page_size,"pageNumber":{i}})
        scrape_data = scrape_page(json_content.get('Products')) 
        print(f'''page_number: {i}''')
        df = pd.DataFrame(scrape_data)
        df.to_csv(file_path, mode='a', header=False, index=False)
        i = i + 1
    df_new = pd.read_csv(file_path)
    print(df_old.count(), df_new.count())
    df_new_only = filter_new_shows_only(df_old, df_new,'sid')
    print(df_new_only.count())
    
    concord_log.info("\n ******Scrapped total Product*****")


    ### SECOND PART ###
    # read_csv = pd.read_csv(f'../{dir}initial_scrapped.csv')

    if not df_new_only.empty:
        read_csv_df = df_new_only.iloc[:]

        show_url=read_csv_df[['show_url','sid']]
        global df_v2
        df_v2 = pd.DataFrame(columns=['show_id','duration','target_audience', 'awards','apply_for_license', 'minimum_license_fee', 'brief_synopsis', 'summary', 'history', 'performance_group'])


        itetration_count = 0
        all_data = []
        for index, row in show_url.iterrows():
            play_url = f'''https://www.concordtheatricals.com/api/v1/products/{row['sid']}?'''
            params = {"includeAuthorTitles":False}
            plays_json_content = get_response(play_url,params=params)
            get_data = scrape_plays(plays_json_content)
            scrape_data_df = pd.DataFrame(get_data)
            all_data.append(scrape_data_df)  
            itetration_count +=1
            print(f'''{itetration_count}: scrapped_complete for {play_url}''')

        df_v2 = pd.concat(all_data, ignore_index=True)

        initial_df = spark.createDataFrame(read_csv_df)
        final_df = spark.createDataFrame(df_v2)

        final_df = initial_df.join(final_df,final_df.show_id == initial_df.sid,"inner")

        final_df = final_df.toPandas()

        # scrapped_data_dir = '../data/raw_data/concord/scraped_data.csv'
        scrapped_data_dir = '/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/concord/scraped_data.csv'

        if not os.path.isfile(scrapped_data_dir):
            # final_df.to_csv('../data/raw_data/concord/scraped_data.csv',mode='w', index=False)
            final_df.to_csv('/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/concord/scraped_data.csv',mode='w', index=False)

            print("Write Scrapped")

        else:
            # final_df.to_csv('../data/raw_data/concord/scraped_data.csv',mode='a',header=False, index=False)
            final_df.to_csv('/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/concord/scraped_data.csv',mode='a',header=False, index=False)
            
            print("Scrapped append")
        
        spark.stop()

    print("No new data")



if __name__ == '__main__':
    start_concord()