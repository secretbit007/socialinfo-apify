import re
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import datetime
from math import ceil

def get_slugs() -> list:
    is_first = True
    page_limit = 0
    page_index = 0
    slugs = []
    
    while True:
        api = 'https://meilisearch.sozialinfo.ch/multi-search'
        headers = {
            'Authorization': 'Bearer 2368d253b290cdda9ad6196a170b158d1157e18390d1003aabd40ba495e7df35'
        }
        payload = {
            "queries": [
                {
                    "indexUid": "job_portal_joboffer",
                    "q": "",
                    "facets": [
                        "employment",
                        "location_cantons",
                        "location_city",
                        "percent_min",
                        "position",
                        "qualifications",
                        "working_areas"
                    ],
                    "filter": ["status = published"],
                    "attributesToHighlight": ["*"],
                    "highlightPreTag": "__ais-highlight__",
                    "highlightPostTag": "__/ais-highlight__",
                    "limit": 1000,
                    "offset": page_index * 1000,
                    "sort": ["sorting_time:desc"]
                }
            ]
        }

        try:
            resp = requests.post(api, headers=headers, json=payload)
        except:
            continue
        
        result = resp.json()['results'][0]
        
        if is_first:
            total_count = result['estimatedTotalHits']
            page_limit = ceil(total_count / 1000)
            is_first = False
            
        hits = resp.json()['results'][0]['hits']
        
        for hit in hits:
            info = {}
            
            try:
                sorting_time = datetime.fromtimestamp(hit['sorting_time']).date()

                
                info['sourced_published_date'] = sorting_time.strftime('%Y-%m-%d')
            except:
                pass
            
            try:
                info['sourced_uid'] = hit['id']
            except:
                pass
            
            try:
                info['sourced_email'] = hit['application_email']
            except:
                pass
            
            try:
                soup = BeautifulSoup(hit['description'], 'html.parser')
                info['sourced_description'] = soup.text.strip()
            except:
                pass
            
            try:
                info['sourced_state'] = ','.join(hit['location_cantons'])
            except:
                pass
            
            try:
                info['sourced_city'] = hit['location_city']
            except:
                pass
            
            try:
                info['sourced_address'] = hit['location_street']
            except:
                pass
            
            try:
                info['sourced_zip'] = hit['location_zip']
            except:
                pass
            
            try:
                info['sourced_organisation'] = hit['organisation']['name']
            except:
                pass
            
            try:
                info['sourced_employment'] = hit['employment']
            except:
                pass
            
            try:
                info['sourced_percentage_lower'] = hit['percent_min']
            except:
                pass
            
            try:
                info['sourced_percentage_upper'] = hit['percent_max']
            except:
                pass
            
            try:
                info['sourced_position'] = hit['position']
            except:
                pass
            
            try:
                info['sourced_title'] = hit['title']
            except:
                pass
            
            try:
                info['slug'] = hit['slug']
            except:
                pass
            
            try:
                info['sourced_domain'] = str(hit['organisation']['website']).replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.','')
            except:
                pass
            
            try:
                name = hit['author']['name']
                info['sourced_firstname'] = name.split(' ')[0]
                info['sourced_lastname'] = name.split(' ')[-1]
            except:
                pass
        
            slugs.append(info)
        page_index += 1
        
        if page_index == page_limit:
            break
    return slugs

def get_email(text: str) -> str:
    email_re = re.search(r'[\w\.-]+@([\w-]+\.)+[\w-]{2,4}', text)
    
    if email_re:
        email = email_re.group(0).strip()
        email = re.sub(r'\.ch.+', '.ch', email)
        
        return email
    else:
        return ''
        
def get_phone(text: str) -> str:
    phone_re = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', text)
            
    if phone_re:
        return phone_re.group(0).strip()
    else:
        return ''

def get_detail(slug: dict) -> dict:
    info = slug
    url = f'https://www.sozialinfo.ch/arbeitsmarkt/stellenportal/{info["slug"]}'
    info['sourced_url'] = url
    info['sourced_source'] = url.replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '').replace('.', '_')

    while True:
        try:
            resp = requests.get(url)
        except:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        
        main_text = soup.find('main').text
        
        if 'email' in info.keys():
            if info['sourced_email'] == '':
                info['sourced_email'] = get_email(main_text)
        else:
            info['sourced_email'] = get_email(main_text)
            
        info['sourced_phone'] = get_phone(main_text)
        info.pop('slug')
        break
    
    return info
    
async def scrape_sozialinfo_data(dataset):
    slugs = get_slugs()
    
    jobs = []
    with ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(get_detail, slugs)

        jobs.extend(results)

    for job in jobs:
        if job:
            await dataset.push_data(job)
