from .sozialinfo import scrape_sozialinfo_data
from .gemeindestellen import scrape_gemeindestellen_data
from .google import scrape_google_data
from .heiminfo import scrape_heiminfo_data
from .jobs import scrape_jobs_data
from .publicjobs import scrape_publicjobs_data
from .sozjobs import scrape_sozjobs_data
from .stiftungschweiz import scrape_stiftungschweiz_data
from .workswiss import scrape_workswiss_data
from apify import Actor

async def main() -> None:
    async with Actor:
        Actor.log.info('Dropping socialinfo dataset...')
        dataset = await Actor.open_dataset(name='socialinfo')
        await dataset.drop()

        Actor.log.info('Creating socialinfo dataset...')
        dataset = await Actor.open_dataset(name='socialinfo')

        Actor.log.info('Scraping results...')

        Actor.log.info('sozialinfo')
        await scrape_sozialinfo_data(dataset)

        Actor.log.info('gemeindestellen')
        await scrape_gemeindestellen_data(dataset)

        Actor.log.info('heiminfo')
        await scrape_heiminfo_data(dataset)

        Actor.log.info('jobs')
        await scrape_jobs_data(dataset)

        Actor.log.info('sozjobs')
        await scrape_sozjobs_data(dataset)

        Actor.log.info('stiftungschweiz')
        await scrape_stiftungschweiz_data(dataset)

        Actor.log.info('workswiss')
        await scrape_workswiss_data(dataset)

        Actor.log.info('publicjobs')
        await scrape_publicjobs_data(dataset)

        Actor.log.info('google')
        await scrape_google_data(dataset)
