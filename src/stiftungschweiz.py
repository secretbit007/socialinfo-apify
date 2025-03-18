import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from sqlalchemy.orm import Session

from models.orders import Order

def get_slugs():
    is_first = True
    page_limit = 0
    page_index = 0
    results = []

    while True:

        api = 'https://sdykx2foz0-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.73.4)%3B%20Vue%20(2.7.16)%3B%20Vue%20InstantSearch%20(4.19.2)%3B%20JS%20Helper%20(3.22.3)%3B%20autocomplete-core%20(1.17.4)%3B%20autocomplete-js%20(1.17.4)&x-algolia-api-key=796065d64062e373e20d1f9311e77e81&x-algolia-application-id=SDYKX2FOZ0'
        payload = {
            "requests": [
                {
                    "indexName": "prod_Organizations",
                    "params": f"clickAnalytics=true&facetFilters=%5B%5B%22nks_2%3A114%22%2C%22nks_2%3A115%22%5D%5D&facets=%5B%22effective_type%22%2C%22nks_2%22%5D&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=1000&maxValuesPerFacet=300&page={page_index}&query="
                }
            ]
        }
        resp = requests.post(api, json=payload)
        data = resp.json()['results'][0]
        
        if is_first:
            is_first = False
            page_limit = data['nbPages']

        for hit in data['hits']:
            info = {
                'sourced_uid': hit['id'],
                'sourced_title': str(hit['title']).strip(),
                'slug': hit['slug'],
                'sourced_state': hit['address']['canton'],
                'sourced_zip': hit['address']['postal_code'],
                'sourced_city': hit['address']['city'],
                'sourced_address': f"{hit['address']['street']} {hit['address']['house_number']}, {hit['address']['address_line_2']}",
                'sourced_published_date': datetime.fromtimestamp(hit['updated_at']).strftime('%Y-%m-%d'),
                'sourced_description': hit['purpose_zefix']
            }

            results.append(info)

        page_index += 1

        if page_index >= page_limit:
            break

    return results

def get_detail(info):
    url = f'https://stiftungen.stiftungschweiz.ch/organisation/{info["slug"]}'
    row = info
    row['sourced_url'] = url
    row['sourced_source'] = url.replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '').replace('.', '_')

    cookies = {
        'stiftungschweiz_session': 'eyJpdiI6IlY0akZJdkJwTVEydlgyUnk2eDhRTmc9PSIsInZhbHVlIjoiTjJsUHdqVmFudmx0dDdWT2JjVTNmSzRIcnhUSHZzNEFycm9BcVN3amVsRmlIZXFsUFFJTG5vNlpYNURsU3NLOTJrQ3dPOFVzRFBjU0crZlYraU1CQVRmaDBUVjRCS25SWFFRQXVxa1FaT2lzY3o4dk5ZWU5wOHJYWDB0a2oyKzYiLCJtYWMiOiI3YzMxMDhjM2FkMmVkMmI2ZTMxOGNiYTU3MDVlMTZkMzE0NmFmNDI3ZDYwMmY1MmVjOWVlMjBjNDAzZjhkZmQ3IiwidGFnIjoiIn0%3D'
    }
    resp = requests.get(url, cookies=cookies)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    modal = soup.find('organization-donation-cta-modal')
    
    if modal:
        try:
            organization = modal.get(':organization')
            data = json.loads(organization)
            
            row['sourced_domain'] = str(data['site']).replace('https://', '').replace('http://', '').strip().split('/')[0].replace('www.', '')
            row['sourced_phone'] = data['phone']
            row['sourced_email'] = data['email']
            row['sourced_firstname'] = data['contact_first_name']
            row['sourced_lastname'] = data['contact_last_name']
        except:
            pass

    return row

def scrape_data(start_date, end_date, order_id, session: Session):
    try:
        if not os.path.exists('data'):
            os.mkdir('data')

        slugs = get_slugs()
        
        with ThreadPoolExecutor(max_workers=100) as executor:
            jobs = executor.map(get_detail, slugs)
        
        df = pd.DataFrame(jobs, columns=['sourced_uid', 'sourced_title', 'sourced_percentage_lower', 'sourced_percentage_upper', 'sourced_position', 'sourced_organisation', 'sourced_employment', 'sourced_published_date', 'sourced_address', 'sourced_state', 'sourced_zip', 'sourced_city', 'sourced_url', 'sourced_description', 'sourced_email', 'sourced_phone', 'sourced_domain', 'sourced_firstname', 'sourced_lastname', 'sourced_source'])
        df.to_excel(f"data/{order_id}.xlsx", engine="xlsxwriter")

        session.query(Order).filter(Order.id == order_id).update({"error": False})
    except:
        session.query(Order).filter(Order.id == order_id).update({"error": True})

    session.query(Order).filter(Order.id == order_id).update({"finished": True})
    session.commit()
