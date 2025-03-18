import re
import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

def getUrls():
    jobUrls = []
    
    api = 'https://www.gemeindestellen.ch/jobsearch'

    headers = {
        'content-type': 'multipart/form-data; boundary=----WebKitFormBoundary3pUMDbwYt91olgDl',
        'x-requested-with': 'XMLHttpRequest'
    }
    
    page = 0
    while True:
        payload = f"""------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="searchquery-jobTitle"


------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="jom_typeahead_selected_id"

0
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="searchquery-location"


------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="searchquery-jobType"

Alterspflege, Kindertagesstätten, Krankenpflege, Spitex und andere, Kindergarten / Basisstufe (1. Zyklus), Primarstufe (1. und 2. Zyklus), Sekundarstufe I (3. Zyklus), Sekundarstufe II, Weiterbildungen / Kurswesen, Universitäten / Fachhochschulen, Erwachsenenbildung, Sonderschulung, Musikschulen, Weitere Schulen, Fachlehrperson, Integrative Förderung / Sonderschulung (IF, IS), Klassenlehrperson, Klassenassistenz, Logopädie, Vikariate / Stellvertretungen, Schulische Heilpädagogik, Schulpsychologie, Psychomotorik, Schulleitung, Tagesstrukturen, Deutsch als Zweitsprache (DaZ), Vorschulangebote, Sozialpädagogik, Diverse, Justiz / Justizvollzug / Recht, Einwohnerdienste, Betreibungsamt, Verwaltung / Kanzlei, Einwohnerdienste / Stadtbüro, Präsidiales / Zentrale Dienste / Verwaltung allgemein, Schulverwaltung, Zivilstandswesen, Erziehung / Miterziehung, Fachpersonen Betreuung Kind, Gruppenleitung / Hortleitung, Diverse, Ärzte / Fachärzte, Praxispersonal / Assistenz, Klinische Sekretariate / MPA, Akademische Berufe, Med. Technische Berufe / Med. Therapeutische Berufe, Pflegefachpersonen, Fachpersonen Gesundheit (FaGe) und Betreuung (FaBe), Pflegefachpersonen mit Zusatzausbildung (IPS, OPS, Notfall, Anästhesie), Spitex, Diverse, Erziehung / Familienberatung, Jugendarbeit, Alters- und Seniorenarbeit, Sozialarbeit / Schulsozialarbeit, Asylwesen / Migrationsarbeit, Kindes- / Erwachsenenschutz KESB, Behindertenbereich, Zusatzleistungen / Sozialversicherungen, Andere, Fundraising, Campaigning, Politik, Entwicklungszusammenarbeit, Freiwilligenarbeit, Diverse
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

31
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

30
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

32
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

33
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

6
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

7
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

8
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

664
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

4
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

9
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

665
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

666
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

667
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

668
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

670
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

671
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

672
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

673
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

62
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

60
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

64
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

63
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

65
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

66
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

674
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

61
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

675
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

676
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

677
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

36
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

16
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

70
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

15
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

71
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

72
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

73
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

74
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

98
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

99
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

100
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

101
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

24
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

25
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

75
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

76
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

26
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

77
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

78
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

601
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

79
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

80
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

38
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

39
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

81
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

40
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

82
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

83
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

84
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

85
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

41
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

589
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

590
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

591
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

592
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

593
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="listitem_option_1[]"

594
------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="searchquery-jobCategoryType"


------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="searchquery-jobContractType"


------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="editHash"


------WebKitFormBoundary3pUMDbwYt91olgDl
Content-Disposition: form-data; name="from"

{page * 20}
------WebKitFormBoundary3pUMDbwYt91olgDl--"""
        resp = requests.post(api, headers=headers, data=payload)
        soup = BeautifulSoup(resp.text, 'html.parser')

        items = soup.find_all('li', class_='list-group-item')
        
        if len(items) == 0:
            break

        for item in items:
            jobUrls.append('https://www.gemeindestellen.ch' + item.a['href'])
            
        page += 1
        
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
            return
        
        description = re.search(r'"description": "(.+)",\s', json_data).group(1)
        json_data = re.sub(r'"description": "(.+)",\s', '', json_data)
            
        parsed_json = json.loads(json_data)
        
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
        info['sourced_source'] = jobUrl.replace('https://', '').replace('http://', '').split('/')[0].strip().replace('www.', '').replace('.', '_')
        
        try:
            desc_soup = BeautifulSoup(description, 'html.parser')
            
            links = desc_soup.find_all('a')
            if len(links) > 0:
                info['sourced_domain'] = links[-1].get('href').replace('\\\"', '').replace('https://', '').replace('http://', '').split('/')[0].strip().replace('www.', '')
            
            info['sourced_description'] = desc_soup.text.strip()
            
            emailObj = re.search(r'[\w\.-]+@([\w-]+\.)+[\w-]{2,4}', desc_soup.text.strip())
            
            if emailObj:
                email = emailObj.group(0).strip()
                email = re.sub(r'\.ch.+', '.ch', email)
                info['sourced_email'] = email
                
            phoneObj = re.search(r'[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[\s0-9-]{9,14}', desc_soup.text.strip())
            
            if phoneObj:
                info['sourced_phone'] = phoneObj.group(0).strip()
        except:
            pass
        
        return info

async def scrape_gemeindestellen_data(dataset):
    jobs = []
    jobUrls = getUrls()

    with ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(get_detail, jobUrls))
        
        jobs.extend(results)

    for job in jobs:
        if job:
            await dataset.push_data(job)
