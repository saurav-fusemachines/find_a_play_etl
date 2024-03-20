import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


def get_response(endpoint):
    try:
        response = requests.get(endpoint)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup
    except requests.RequestException as e:
        
        print(f"Request failed: {e}")
        return None
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    
def return_exception(e):
    return f"Exception Occured as: {e}"

def generate_urls_to_scrape(html_content,filter):
    url_to_scrape = []
    shows_root_thumbnail = html_content.find_all('div',{'class':'col-md-4 col-sm-6 col-xs-12 views-col'})

    for shows_link in shows_root_thumbnail:
        show_link = shows_link.find('div',{'class':'list-title'})
        if show_link:
            # Access further information or perform actions with the list-title element
            h3_element = show_link.find('h3')
            if h3_element:
                title_text = h3_element.text.strip()
                # print("Title:", title_text)
            url = shows_link.find('a').get('href')
            url_to_scrape.append({'url': url, 'filter': filter})
    return url_to_scrape


def scrape_page(show_endpoint,show_filter):
    html_content = get_response(show_endpoint)
    data_collection = []
    
    # Show title
    try:
        show_title = html_content.find(
            'h1', {'class': 'single-title custom-post-type-title'}).text
    except Exception as e:
        return_exception(e)
        show_title = "None"

    #Show type
    show_type = show_filter

    #Book and Music by
    book_and_music_by = []
    try:
        book_and_music = html_content.find('div', {'id': 'show-overview'}).find(
            'ul', {'class': 'list-unstyled'}).find_all('li', {'author-info'})
        for entry in book_and_music:
            entry_text = entry.text.strip()
            if entry_text:
                book_and_music_by.append(entry_text)
            else:
                book_and_music_by = ""
                
    except Exception as e:
        book_and_music_by = "None"
        return_exception(e)

    #Male and Female Casting Number
    try:
        Casting_info_div = html_content.find('div',{'id':'show-casting-information'})
        casting_info = Casting_info_div.find_all('p')
        
        if casting_info:
            casting_info_text = casting_info[0].text.strip()
            if any(char.isdigit() for char in casting_info_text):
                casting_info = casting_info_text
            else:
                casting_info = ""
        else:
            casting_info = ""
    
    except Exception as e:       
        casting_info = ""
        return_exception(e)

    #Cast List
    Casting_div = html_content.find('div',{'id':'show-casting-information'})
    Casting_div= Casting_div.find_all('p')
    cast_list_detail = []
    try:
        for cast_list in Casting_div:
            cast_list = cast_list.text.strip()
            cast_list_detail.append(cast_list)
    except Exception as e:
        cast_list_detail = []
        return_exception(e)

    # Image URL
    try:
        image_url = ""
        image_h = html_content.find('header',{'class':'article-header show-header'})
        if image_h:
            image_url = image_h.find('img',{'class':'img-responsive'}).get("src")
        else:
            image_url = ""
    except:
        image_url = ""

    # Apply for License
    try:
        apply_for_license = html_content.find('div', {'class': 'pull-right extra-links'})
        
        if apply_for_license.text:
            license_links = apply_for_license.find_all('a', {'class': 'btn btn-success btn-lg'})
            
            # Check if the list is not empty before accessing the second element
            if len(license_links) > 1:
                apply_for_license = license_links[1].get('href')
            else:
                apply_for_license = "None"
        else:
            apply_for_license = "None"
    except Exception as e:
        apply_for_license = "None"
        return_exception(e)


    # Show Credits
    try:
        show_credits = html_content.find('div', {'id': 'show-credits'}).text
    except AttributeError:
        show_credits = "None"
        return_exception(e)
    
    #Show orchestral-information
    orchestral_information_list = []
    try:
        orchestral_information = html_content.find('div',{'id':'show-orchestral-information'})
        if orchestral_information:
            orchestral_information = orchestral_information.find_all('p')
            for item in orchestral_information:
                orchestral_information_list.append(item.text)
        else: 
            orchestral_information_list =""
    except Exception as e:
        orchestral_information_list = []
        return_exception(e)


    #Show-Synopsis
    try:
        show_synopsis_texts = []

        show_synopsis_elements = html_content.find('div', {'id': 'show-synopsis'}).find_all('p')

        if show_synopsis_elements:
            for all_p in show_synopsis_elements:
                show_synopsis_texts.append(all_p.text)
    except Exception as e:
        show_synopsis_texts = []    

    #resources
    resources_list = []
    try:
        resources_div = html_content.find('div',{'class':'sidebar-section additional-products'})
        if resources_div:
            resources_ul = resources_div.find('ul',{'class':'list-unstyled'}).find_all('li')
            for li_tag in resources_ul:
                resource_name = li_tag.text.strip()
                resource_url = li_tag.a['href']
                
                resource_info = {
                    'resource_name': resource_name,
                    'resource_url': resource_url
                }
                
                resources_list.append(resource_info)
        else:
            resources_list = ""

    except Exception as e:
        resources_list = ""
    
    #Songs Used
    songs_list = []
    try:
        songs_div = html_content.find('div',{'class':'audio-wrapper'})
        if songs_div:
            songs_li = songs_div.find('ol',{'class':'audiojs-playlist'}).find_all('li')
            if songs_li:
                for i in songs_li:
                    songs_list_txt = i.text.strip()
                    songs_url = i.a.get('data-src')
                    songs_dict = {
                        'song_name':songs_list_txt,
                        'song_url':songs_url
                    }
                    songs_list.append(songs_dict)
            else: songs_list = ""
        else:
            songs_list = ""
    except Exception as e:
        songs_list = ""
        return_exception(e)

    #Related Shows
    related_shows_links = []
    try:
        show_overview_div = html_content.find('div', {'id': 'show-overview'})
        if show_overview_div:
            h3_tag = show_overview_div.find('h3', string='Related Shows')
            if h3_tag:
                a_tag = h3_tag.find_next('a')
                if a_tag and a_tag.find_next_sibling('a'):
                    related_shows_links.append(a_tag['href'])
                    related_shows_links.append(a_tag.find_next_sibling('a')['href'])
                
            else:
                related_shows_links = ""
        else:
            related_shows_links = ""

    except Exception as e:
        related_shows_links = ""
        return_exception(e)
    
    #upcoming productions
    upcoming_productions=[]
    try:
        upcoming_productions_content = get_response(f"{show_endpoint}/upcoming/")
        upcoming_productions_div = upcoming_productions_content.find_all('div',{'class':'upcoming'})
        if upcoming_productions_div:
            for i in upcoming_productions_div:
                if i:
                    from_date = i.find('span',{'class':'date-display-start'}).text.strip()
                    to_date = i.find('span',{'class':'date-display-end'}).text.strip()
                    production_date = f"{from_date} to {to_date}"
                    location = i.find('h5').text.strip()
                    current_production_dict = {
                        'production_date':production_date,
                        'location':location
                    }
                    upcoming_productions.append(current_production_dict)
                else:
                    upcoming_productions_productions = ""
        else:
            upcoming_productions = ""

    except Exception as e:
        upcoming_productions = ""
        return_exception(e)

    #Community
    community= {}
    try:
        community_div = html_content.find('div',{'id':'show-community'}).find('div',{'class':'resource clearfix'})
        community_div = '\n'.join([p.get_text() for p in community_div.find_all('p')])

        sets = []
        costumes = []
        props = []
        current_category = None

        lines = community_div.split('\n')

        # Iterate each line
        for line in lines:
            line = line.strip()
            
            # Check if the line contains a category keyword
            if line.startswith('Sets'):
                current_category = 'sets'
            elif line.startswith('Costumes'):
                current_category = 'costumes'
            elif line.startswith('Props'):
                current_category = 'props'
            elif current_category:

                if current_category == 'sets':
                    sets.append(line)
                elif current_category == 'costumes':
                    costumes.append(line)
                elif current_category == 'props':
                    props.append(line)

        community= {
            'sets': sets,
            'costumes': costumes,
            'props': props
        }
    except Exception as e:
        community= ""
        return_exception(e)


    data = {
        'show_title': show_title,
        'show_type':show_type,
        'show_url': show_endpoint,
        'book_and_music_by':book_and_music_by,
        'logo_url':image_url,
        'casting_info':casting_info,
        'cast_list':cast_list_detail,
        'apply_for_license': apply_for_license,
        'show_credits':show_credits,
        'orchestral_information_list':orchestral_information_list,
        'upcoming_productions':upcoming_productions,
        'songs_list':songs_list,
        'related_shows_urls':related_shows_links,
        'resources':resources_list,
        'synopsis':show_synopsis_texts,
        'community':community
    }
    data_collection.append(data)
    return data_collection

# Filter new plays and shows
def filter_new_shows_only(df_left, df_right, join_col):
    merged_df = df_right.merge(df_left[[join_col]], how='left', on=join_col, indicator=True)
    merged_df = merged_df[merged_df['_merge'] == 'left_only']
    merged_df.drop('_merge', axis=1, inplace=True)
    return merged_df

def start_trw():
    filters = ['broadway','off-broadway','school-edition','young-audience','young-part','younger-part','symphonic-experience']

    dir = r'/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/trw/'
    directory = os.path.dirname(dir)

    if not os.path.exists(directory):
        print(f"{dir}  iscreated.")
        os.makedirs(directory)

    file_path  = f"{dir}trw_urls.csv"
    file_exist = os.path.isfile(file_path)

    if file_exist:
        df_old = pd.read_csv(file_path)
    else:
        df_old = pd.DataFrame(columns=['url','filter'])

    pd.DataFrame(columns=['url','filter']).to_csv(file_path, index = False)

    shows_endpoints = []
    for filter in filters:
        print(f"***Getting urls of : {filter} ***")
        theatricalrights_url = f"https://www.theatricalrights.com/show-type/{filter}/?lang=en"
        html_content = get_response(theatricalrights_url)
        urls_to_scrape = generate_urls_to_scrape(html_content,filter)
        shows_endpoints.extend(urls_to_scrape)
        df = pd.DataFrame(urls_to_scrape)
        df.to_csv(file_path,mode='a',header=False,index=False)

    df_new = pd.read_csv(file_path)

    df_new_only = filter_new_shows_only(df_old, df_new,'url')

    if not df_new_only.empty:
        read_new_only_df = df_new_only.iloc[:]
        show_url = read_new_only_df[['url','filter']]
        global df_v2

        df_v2 = pd.DataFrame(columns=['show_title','show_type', 'show_url','book_and_music_by', 'logo_url', 'casting_info','cast_list','apply_for_license','show_credits','orchestral_information_list','resources','songs_list','related_shows_urls','upcoming_productions','synopsis','community'])

        itetration_count = 0
        all_data = []
        for index,row in show_url.iterrows():
            scrape_data =scrape_page(row['url'],row['filter'])
            scrape_data_df = pd.DataFrame(scrape_data)
            all_data.append(scrape_data_df)
            itetration_count +=1
            print(f"{itetration_count}: Scraping Completed for {row['url']}")
        
        df_v2 = pd.concat(all_data,ignore_index=True)
        scrapped_data_dir = '/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/trw/scraped_data.csv'

        if not os.path.isfile(scrapped_data_dir):
                df_v2.to_csv('/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/trw/scraped_data.csv',mode='w', index=False)
                print("Write Scrapped")

        else:
            df_v2.to_csv('/home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/data/raw_data/trw/trw.csv',mode='a',header=False, index=False)
                
            print("Scrapped append")

    else:
        print("No New Show is Added in TRW")    


if __name__ == '__main__':
    start_trw()