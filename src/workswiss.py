import os
import requests
import pandas as pd
import xlsxwriter
from datetime import datetime
from sqlalchemy.orm import Session

from models.orders import Order

def getCategories():
    url = 'https://www.job-room.ch/referenceservice/api/_search/occupations/label?prefix=sozial&types=AVAM,CHISCO3,CHISCO5&resultSize=50&_ng=ZGU='
    cookies = {
        'NG_TRANSLATE_LANG_KEY': 'de'
    }
    resp = requests.get(url, cookies=cookies)
    categories = []
    
    if resp.status_code == 200:
        data = resp.json()
        
        categories = data['classifications']
        
    return categories

def getJobs(categories: list, jobItems: list):
    payload = {
        "workloadPercentageMin":10,
        "workloadPercentageMax":100,
        "permanent": None,
        "companyName": None,
        "onlineSince":60,
        "displayRestricted": False,
        "professionCodes":[],
        "keywords":[],
        "communalCodes":[],
        "cantonCodes":[]
    }
    
    for category in categories:
        payload["professionCodes"].append(
            {
                "type": category["type"],
                "value": category["code"]
            }
        )
        
    page = 0
    
    while True:
        url = f'https://www.job-room.ch/jobadservice/api/jobAdvertisements/_search?page={page}&size=20&sort=date_desc&_ng=ZW4='
        resp = requests.post(url, json=payload)
        
        if resp.status_code == 200:
            jobs = resp.json()
            
            for job in jobs:
                row = {}

                row['sourced_uid'] = job['jobAdvertisement']['externalReference']
                row['sourced_title'] = job['jobAdvertisement']['jobContent']['jobDescriptions'][0]['title']
                
                try:
                    row['sourced_percentage_lower'] = job['jobAdvertisement']['jobContent']['employment']['workloadPercentageMin']
                except:
                    pass
                
                try:
                    row['sourced_percentage_upper'] = job['jobAdvertisement']['jobContent']['employment']['workloadPercentageMax']
                except:
                    pass
                
                try:
                    row['sourced_organisation'] = job['jobAdvertisement']['jobContent']['company']['name']
                except:
                    pass
                
                try:
                    row['sourced_published_date'] = job['jobAdvertisement']['updatedTime']
                except:
                    pass
                
                try:
                    row['sourced_state'] = job['jobAdvertisement']['jobContent']['location']['cantonCode']
                except:
                    pass
                
                try:
                    row['sourced_zip'] = job['jobAdvertisement']['jobContent']['location']['postalCode']
                except:
                    pass
                
                try:
                    row['sourced_city'] = job['jobAdvertisement']['jobContent']['location']['city']
                except:
                    pass
                
                row['sourced_url'] = f"https://www.job-room.ch/job-search/{job['jobAdvertisement']['id']}"
                row['sourced_source'] = row['sourced_url'].replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '').replace('.', '_')
                row['sourced_description'] = job['jobAdvertisement']['jobContent']['jobDescriptions'][0]['description']
                
                try:
                    row['sourced_email'] = job['jobAdvertisement']['jobContent']['company']['email']
                except:
                    pass
                
                try:
                    row['sourced_phone'] = job['jobAdvertisement']['jobContent']['company']['phone']
                except:
                    pass
                
                try:
                    row['sourced_address'] = job['jobAdvertisement']['jobContent']['company']['street']
                except:
                    pass
                
                try:
                    row['sourced_domain'] = str(job['jobAdvertisement']['jobContent']['company']['website']).replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '')
                except:
                    pass
                
                try:
                    firstname = job['jobAdvertisement']['jobContent']['publicContact']['firstName']
                    lastname = job['jobAdvertisement']['jobContent']['publicContact']['lastName']
                    row['sourced_firstname'] = firstname
                    row['sourced_lastname'] = lastname
                except:
                    pass
                
                jobItems.append(row)
        elif resp.status_code == 412:
            break
        
        page += 1
    
def scrape_data(start_date, end_date, order_id, session: Session):
    try:
        if not os.path.exists('data'):
            os.mkdir('data')
            
        results = []
        
        categories = getCategories()
        getJobs(categories, results)
        
        jobs = []
        
        for result in results:
            try:
                sortingDate = datetime.fromisoformat(result['sourced_published_date']).date()
                        
                if sortingDate >= start_date and sortingDate <= end_date:
                    jobs.append(result)
            except:
                pass
        
        df = pd.DataFrame(jobs, columns=['sourced_uid', 'sourced_title', 'sourced_percentage_lower', 'sourced_percentage_upper', 'sourced_position', 'sourced_organisation', 'sourced_employment', 'sourced_published_date', 'sourced_address', 'sourced_state', 'sourced_zip', 'sourced_city', 'sourced_url', 'sourced_description', 'sourced_email', 'sourced_phone', 'sourced_domain', 'sourced_firstname', 'sourced_lastname', 'sourced_source'])
        df.to_excel(f"data/{order_id}.xlsx", engine="xlsxwriter")
        
        session.query(Order).filter(Order.id == order_id).update({"error": False})
    except:
        session.query(Order).filter(Order.id == order_id).update({"error": True})
    
    session.query(Order).filter(Order.id == order_id).update({"finished": True})
    session.commit()
