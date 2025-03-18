import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

def getDetail(info: str):
    jobId = info['i']
    url = f'https://www.sozjobs.ch/job/{jobId}'
    result = {
        'sourced_uid': jobId
    }
    resp = requests.get(url)
    
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, 'html.parser')
        add_info = soup.find('div', class_='additional-data-content').find('div', class_='content')
        
        result['sourced_title'] = add_info.h2.text.strip()
        
        try:
            result['sourced_organisation'] = add_info.find('p', class_='ort').text.strip()
        except:
            pass
        
        result['sourced_url'] = url
        result['sourced_source'] = url.replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '').replace('.', '_')
        result['sourced_description'] = soup.find('article', id='inhalt').text.strip()
        
        try:
            result['sourced_email'] = soup.find('div', id='kontakt').find('a', attrs={'href': re.compile('mailto:')})['href'].replace('mailto:', '')
        except:
            emailObj = re.search(r'[\w\.-]+@([\w-]+\.)+[\w-]{2,4}', soup.find('div', id='kontakt').text.strip())
            
            if emailObj:
                email = emailObj.group(0).strip()
                email = re.sub(r'\.ch.+', '.ch', email)
                result['sourced_email'] = email
            
        phoneObj = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', soup.find('div', id='kontakt').text.strip())
        
        if phoneObj:
            result['sourced_phone'] = phoneObj.group(0).strip()
        
        website_obj = soup.find('div', id='kontakt').find('a', attrs={'target': '_blank'})
        
        if website_obj:
            result['sourced_domain'] = website_obj.get('href').replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '')
            
        person = soup.find('div', class_='person')
        
        if person:
            name = person.find('p').text.strip()
            result['sourced_firstname'] = name.split(' ')[0]
            result['sourced_lastname'] = name.split(' ')[-1]
        
        divisions = add_info.h2.find_next_siblings('div')
        
        for div in divisions:
            heading = div.h4.text.strip()
            
            match heading:
                case 'Arbeitspensum':
                    workload = re.search(r'(\d+%?\s?-?\s?)?\d+\s?%', div.p.text.strip()).group(0)
                    percents = workload.replace('%', '').split('-')
                    
                    result['sourced_percentage_lower'] = percents[0].strip()
                    result['sourced_percentage_upper'] = percents[-1].strip()
                case 'Funktion':
                    result['sourced_position'] = div.p.text.strip()
                case 'Datum':
                    result['sourced_published_date'] = div.p.text.strip()
                case 'Arbeitsort':
                    address = re.search(r'(\d+) (.+) \((.+)\),', div.p.text.strip())
                    
                    if address:
                        result['sourced_address'] = address.group(0)
                        result['sourced_state'] = address.group(3)
                        result['sourced_zip'] = address.group(1)
                        result['sourced_city'] = address.group(2)
                    
    return result
    
async def scrape_sozjobs_data(dataset):
    jobs = []
    resp = requests.get('https://www.sozjobs.ch/')

    if resp.status_code == 200:
        html = resp.text
        info = json.loads(re.search(r'aJobs = (.+)</script>', html).group(1))
            
        with ThreadPoolExecutor(max_workers=100) as executor:
            results = executor.map(getDetail, info)

            jobs.extend(results)

    for job in jobs:
        if job:
            await dataset.push_data(job)
                
