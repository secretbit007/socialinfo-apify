import re
from selenium_driverless import webdriver
from selenium_driverless.types.by import By
from time import sleep
from apify import Actor

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
                email = emailObj.group(0).strip()
                email = re.sub(r'\.ch.+', '.ch', email)
                info['sourced_email'] = email
                info['sourced_domain'] = info['sourced_email'].split('@')[-1]
                
            phoneObj = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', desc)
            
            if phoneObj:
                info['sourced_phone'] = phoneObj.group(0).strip()

            result.append(info)
        
        await driver.quit()
        
    return result

async def scrape_google_data():
    jobs = await get_jobs()
    
    async with Actor:
        dataset = await Actor.open_dataset(name='socialinfo')

        for job in jobs:
            await dataset.push_data(job)
        