import os
import re
import json
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from time import sleep

from models.orders import Order

def get_urls():
    while True:
        try:
            service = Service('/usr/lib/chromium-browser/chromedriver')
            options = Options()
            options.add_argument('--headless=new')
            driver = webdriver.Chrome(service=service, options=options)

            driver.get('https://www.publicjobs.ch/jobs/Sozialwesen+-+Sozialarbeit/~cat37')

            jobUrls = []

            while True:
                nextBtnObjs = driver.find_elements(By.CLASS_NAME, 'jobs_next_btn')
                
                if len(nextBtnObjs) == 1:
                    if not 'pull-right' in nextBtnObjs[0].get_attribute('class'):
                        break
                
                jobResultsObj = driver.find_element(By.CLASS_NAME, 'job-results')
                jobObjs = jobResultsObj.find_elements(By.CLASS_NAME, 'list-group-item')
                
                for jobObj in jobObjs:
                    jobUrl = jobObj.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    jobUrls.append(jobUrl)
                    
                if len(nextBtnObjs) == 0:
                    break
                
                driver.execute_script("arguments[0].click();", nextBtnObjs[-1])
                sleep(3)

            driver.quit()
            
            break
        except:
            pass
    
    return jobUrls

def get_detail(jobUrl: str):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    }
    
    resp = requests.get(jobUrl, headers=headers)
    
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        try:
            json_data = soup.find('script', attrs={'type': 'application/ld+json'}).text.strip()
        except:
            return {}
        
        description = re.search(r'"description": "(.+)",\s', json_data).group(1)
        json_data = re.sub(r'"description": "(.+)",\s', '', json_data)
            
        try:
            parsed_json = json.loads(json_data)
        except:
            return {}
        
        info = {}
        
        info['sourced_uid'] = parsed_json['identifier']
        info['sourced_title'] = parsed_json['title']
        
        try:
            workload = re.search(r'(\d+%?\s?-?(bis)?\s?)?\d+\s?%', info['title']).group(0)
            percents = workload.replace('%', '').split('-')
            
            if len(percents) == 1:
                percents = percents[0].split('bis')
            
            info['sourced_percentage_lower'] = percents[0].strip()
            info['sourced_percentage_upper'] = percents[-1].strip()
        except:
            pass
        
        try:
            info['sourced_position'] = parsed_json['occupationalCategory']
        except:
            pass
        
        try:
            info['sourced_organisation'] = parsed_json['hiringOrganization']['name']
        except:
            pass
        
        try:
            info['sourced_published_date'] = parsed_json['datePosted']
        except:
            pass
        
        try:
            info['sourced_state'] = parsed_json['jobLocation']['address']['addressRegion']
        except:
            pass
        
        try:
            info['sourced_zip'] = parsed_json['jobLocation']['address']['postalCode']
        except:
            pass
        
        try:
            info['sourced_city'] = parsed_json['jobLocation']['address']['addressLocality']
        except:
            pass
        
        try:
            info['sourced_address'] = parsed_json['jobLocation']['address']['streetAddress']
        except:
            pass
        
        info['sourced_url'] = jobUrl
        info['sourced_source'] = jobUrl.replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '').replace('.', '_')
        
        try:
            desc_soup = BeautifulSoup(description, 'html.parser')
            
            try:
                links = desc_soup.find_all('a')
                
                if len(links) > 0:
                    info['sourced_domain'] = links[-1].get('href').replace('\\\"', '').replace('http://', '').replace('https://', '').strip().split('/')[0].replace('www.', '')
            except:
                pass
            
            info['sourced_description'] = desc_soup.text.strip()
            
            emailObj = re.search(r'[\w\.-]+@([\w-]+\.)+[\w-]{2,4}', desc_soup.text.strip())
            
            if emailObj:
                info['sourced_email'] = emailObj.group(0).strip()
                
            phoneObj = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', desc_soup.text.strip())
            
            if phoneObj:
                info['sourced_phone'] = phoneObj.group(0).strip()
        except:
            pass
        
        return info

def scrape_data(start_date, end_date, order_id, session: Session):
    if not os.path.exists('data'):
        os.mkdir('data')
        
    jobUrls = get_urls()
    jobs = []

    with ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(get_detail, jobUrls)
        
    for result in results:
        try:
            sortingDate = datetime.strptime(result['sourced_published_date'], '%Y-%m-%d').date()
            
            if sortingDate >= start_date and sortingDate <= end_date:
                jobs.append(result)
        except:
            pass
    
    df = pd.DataFrame(jobs, columns=['sourced_uid', 'sourced_title', 'sourced_percentage_lower', 'sourced_percentage_upper', 'sourced_position', 'sourced_organisation', 'sourced_employment', 'sourced_published_date', 'sourced_address', 'sourced_state', 'sourced_zip', 'sourced_city', 'sourced_url', 'sourced_description', 'sourced_email', 'sourced_phone', 'sourced_domain', 'sourced_firstname', 'sourced_lastname', 'sourced_source'])
    df.to_excel(f"data/{order_id}.xlsx", engine="xlsxwriter")
    
    session.query(Order).filter(Order.id == order_id).update({"error": False})
        
    session.query(Order).filter(Order.id == order_id).update({"finished": True})
    session.commit()
