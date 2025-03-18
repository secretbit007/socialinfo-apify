import os
import pandas as pd
import requests
import xlsxwriter
from datetime import datetime
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

from models.orders import Order

def getIds(category):
    page = 1
    isFirst = True
    jobIds = []

    while True:
        url = f'https://www.jobs.ch/api/v1/public/search?category-ids%5B%5D={category}&page={page}&rows=20'
        resp = requests.get(url)

        if resp.status_code == 200:
            data = resp.json()
        
            if isFirst:
                pageLimit = data['num_pages']
                
                if pageLimit > 100:
                    pageLimit = 100
                
                isFirst = False

            documents = data['documents']
            
            for document in documents:
                jobIds.append(document['datapool_id'])
                    
        page += 1
        
        if page > pageLimit:
            break
        
    return jobIds

def getDetail(id: str):
    proxies = {
        'http': 'http://spmdri6ewh:SOk=a1gy64ybvFzH5w@dc.smartproxy.com:10000',
        'https': 'http://spmdri6ewh:SOk=a1gy64ybvFzH5w@dc.smartproxy.com:10000',
    }
    
    url = f'https://www.jobs.ch/api/v1/public/search/job/{id}'
    resp = requests.get(url, proxies=proxies)
    job = {
        'sourced_uid': id,
        'sourced_url': f'https://www.jobs.ch/en/vacancies/?term=&jobid={id}'
    }
    
    job['sourced_source'] = job['sourced_url'].replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '').replace('.', '_')
    
    if resp.status_code == 200:
        data = resp.json()
        
        job['sourced_title'] = data['title']
        
        desc_soup = BeautifulSoup(data['template_text'], 'html.parser')
        job['sourced_description'] = desc_soup.text.strip()
        
        try:
            job['sourced_organisation'] = data['company_name']
        except:
            pass
        
        try:
            job['sourced_email'] = data['application_email']
        except:
            pass
        
        try:
            job['sourced_percentage_lower'] = min(data['employment_grades'])
            job['sourced_percentage_upper'] = max(data['employment_grades'])
        except:
            pass
        
        try:
            positions = ['Executive position', 'Employee', 'Specialist']
            position = [positions[i - 1] for i in data['employment_position_ids']]
            
            job['sourced_position'] = ', '.join(position)
        except:
            pass
        
        try:
            job['sourced_city'] = data['locations'][0]['city']
        except:
            pass
        
        try:
            job['sourced_zip'] = data['locations'][0]['postalCode']
        except:
            pass
        
        try:
            job['sourced_published_date'] = data['publication_date']
        except:
            pass
        
        try:
            job['sourced_state'] = data['locations'][0]['cantonCode']
        except:
            pass
        
        try:
            job['sourced_address'] = data['locations'][0]['street']
        except:
            pass
        
        try:
            job['sourced_phone'] = data['contact_person']['phone']
            job['sourced_firstname'] = data['contact_person']['firstName']
            job['sourced_lastname'] = data['contact_person']['lastName']
        except:
            pass
        
        try:
            company_id = data['company_id']
            comp_resp = requests.get(f'https://www.jobs.ch/api/v1/public/company/{company_id}', proxies=proxies)
        
            job['sourced_domain'] = str(comp_resp.json()['url']).replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '')
        except:
            pass
            
    return job

def scrape_data(start_date, end_date, order_id, session: Session):
    try:
        if not os.path.exists('data'):
            os.mkdir('data')
            
        jobs = []
        jobIds = []
        
        jobIds += getIds(107)
        jobIds += getIds(112)
        jobIds += getIds(113)
        
        with ThreadPoolExecutor(max_workers=100) as executor:
            results = executor.map(getDetail, jobIds)
        
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
        