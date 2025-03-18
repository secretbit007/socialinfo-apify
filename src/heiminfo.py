import re
import requests
from math import ceil
from bs4 import BeautifulSoup, Tag
from typing import List
from concurrent.futures import ThreadPoolExecutor

def get_detail(article):
    respDetail = requests.get(article)
    row = {}
    row['sourced_url'] = article
    row['sourced_source'] = str(article).replace('https://', '').replace('http://', '').split('/')[0].strip().replace('www.', '').replace('.', '_')
    if respDetail.status_code == 200:
        soupDetail = BeautifulSoup(respDetail.text, 'html.parser')
        
        try:
            row['sourced_domain'] = soupDetail.find('a', class_='website').get('href').replace('https://', '').replace('http://', '').split('/')[0].strip().replace('www.', '')
        except:
            pass
        
        detail = soupDetail.find('div', class_='institution-detail')
        row['sourced_uid'] = detail['data-id']
        sections: List[Tag] = detail.find_all('section')
    
        for section in sections:
            sectionId = section.get('id')
            
            match sectionId:
                case 'portrait':
                    row['sourced_title'] = section.find('div', class_='head').text.strip()
                    
                    descObj = section.find('div', class_='description')
                    
                    if descObj:
                        row['sourced_description'] = descObj.text.strip()
                case 'kontakt':
                    mapContainer = section.find('div', 'map-and-contact-container')
                    
                    if mapContainer:
                        contactObj = mapContainer.find('div', class_='introduction')
                        
                        elements = contactObj.find_all()
                        
                        for element in elements:
                            emailObj = re.search(r'[\w\.-]+@([\w-]+\.)+[\w-]{2,4}', element.text.strip())
                            
                            if emailObj:
                                break
                        
                        if emailObj:
                            email = emailObj.group(0).strip()
                            email = re.sub(r'\.ch.+', '.ch', email)
                            row['sourced_email'] = email
                        
                        plzObj = contactObj.find('span', class_='plz')
                        cityObj = contactObj.find('span', class_='city')
                        cantonObj = contactObj.find('span', class_='canton')
                        addressObj = contactObj.find('span', class_='address1')
                        phoneObj = contactObj.find('p', class_='phone')
                        
                        if cantonObj:
                            row['sourced_state'] = cantonObj.text.strip()
                            
                        if plzObj:
                            row['sourced_zip'] = plzObj.text.strip()
                            
                        if cityObj:
                            row['sourced_city'] = cityObj.text.strip()
                            
                        if addressObj:
                            row['sourced_address'] = addressObj.text.strip()
                            
                        if phoneObj:
                            phone = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', phoneObj.text.strip())
                            
                            if phone:
                                row['sourced_phone'] = phone.group(0).strip()
                        
                case _:
                    continue
        
    return row

async def scrape_heiminfo_data(dataset):
    articleLinks = []
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://www.heiminfo.ch/institutionen/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    s = requests.Session()
    response = s.get('https://www.heiminfo.ch/institutionen/?sort=score&searchterm=&distance=&platform=&mandatory=&optional=&livingtype=&freeplaces=', headers=headers)
    metaData = response.json()
    pageSize = metaData['PAGESIZE']
    total = metaData['TOTAL']
    pageLimit = ceil(total / pageSize)
    
    def getUrl(page):
        url = f'https://www.heiminfo.ch/institutionen/page/{page}'
        resp = s.get(url, headers=headers)
        
        if resp.status_code == 200:
            data = resp.json()
            
            soup = BeautifulSoup(data['HTML'], 'html.parser')
            articles: List[Tag] = soup.find_all('article', class_='institution')
            
            for article in articles:
                link = 'https://www.heiminfo.ch' + article.find('a').get('href')
                articleLinks.append(link)
                
    with ThreadPoolExecutor(max_workers=50) as executor:
        executor.map(getUrl, range(1, pageLimit + 1))
    
    jobs = []
    with ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(get_detail, articleLinks))

        jobs.extend(results)


    for job in jobs:
        await dataset.push_data(job)

