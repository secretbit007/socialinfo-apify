import os
import re
import pandas as pd
import asyncio
from selenium_driverless import webdriver
from selenium_driverless.types.by import By
from time import sleep
from sqlalchemy.orm import Session

from models.orders import Order

async def get_jobs():
    result = []
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    async with webdriver.Chrome(options=options) as driver:
        await driver.maximize_window()
        await driver.get("https://www.google.com/search?q=sozialberufe%20stellen%20near%20Z%C3%BCrich&rlz=1C5CHFA_enCH1099CH1099&oq=Sozialberufe+stellen&gs_lcrp=EgZjaHJvbWUyBggAEEUYOdIBCDUzNTVqMGo3qAIAsAIA&sourceid=chrome&ie=UTF-8&jbr=sep:0&udm=8&llpgabe=CggvbS8wODk2Ng&ved=2ahUKEwjcmL23h4qLAxVx_rsIHVFdI3kQ5ZgEKAB6BAgHEAA", wait_load=True)
        
        while True:
            await driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            sleep(1)
            
            message = 'No more jobs match your exact search. Try changing your terms or filters.'
            
            if message in await driver.page_source:
                break
            
        jobs = await driver.find_elements(By.XPATH, '//div[@jscontroller="b11o3b"]')
        
        for job in jobs:
            await job.click(move_to=True)
            await driver.sleep(1)
            
            info = {}
            dialog = await driver.find_element(By.XPATH, '//div[@data-hveid="2"]', timeout=10)
            
            org_elem = await dialog.find_element(By.CLASS_NAME, 'VTMWGb')
            organisation = await org_elem.text
            
            info['sourced_organisation'] = organisation
            
            title_elem = await dialog.find_element(By.TAG_NAME, 'h1')
            title = await title_elem.text
            
            info['sourced_title'] = title
            
            workload_re = re.search(r'(\d+%?\s?-?(bis)?\s?)?\d+\s?%', title)
            
            if workload_re:
                percents = workload_re.group(0).replace('%', '').split('-')
                
                if len(percents) == 1:
                    percents = percents[0].split('bis')
                
                info['sourced_percentage_lower'] = percents[0].strip()
                info['sourced_percentage_upper'] = percents[-1].strip()
                
            location_elem = await title_elem.find_element(By.XPATH, './following-sibling::div')
            location = await location_elem.text
            
            info['sourced_address'] = location
            
            desc_elem = await dialog.find_element(By.CLASS_NAME, 'NgUYpe')
            desc = await desc_elem.text
            
            info['sourced_description'] = desc.replace('Job description', '')
            
            emailObj = re.search(r'[\w\.-]+@([\w-]+\.)+[\w-]{2,4}', desc)
            
            if emailObj:
                info['sourced_email'] = emailObj.group(0).strip()
                info['sourced_domain'] = info['sourced_email'].split('@')[-1]
                
            phoneObj = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', desc)
            
            if phoneObj:
                info['sourced_phone'] = phoneObj.group(0).strip()
            
            result.append(info)
        
        await driver.quit()
        
    return result

async def run(order_id):
    jobs = await get_jobs()
    df = pd.DataFrame(jobs, columns=['sourced_uid', 'sourced_title', 'sourced_percentage_lower', 'sourced_percentage_upper', 'sourced_position', 'sourced_organisation', 'sourced_employment', 'sourced_published_date', 'sourced_address', 'sourced_state', 'sourced_zip', 'sourced_city', 'sourced_url', 'sourced_description', 'sourced_email', 'sourced_phone', 'sourced_domain', 'sourced_firstname', 'sourced_lastname', 'sourced_source'])
    df.to_excel(f"data/{order_id}.xlsx", engine="xlsxwriter")

def scrape_data(start_date, end_date, order_id, session: Session):
    if not os.path.exists('data'):
        os.mkdir('data')
    
    asyncio.run(run(order_id))

    session.query(Order).filter(Order.id == order_id).update({"error": False})

    session.query(Order).filter(Order.id == order_id).update({"finished": True})
    session.commit()
        