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
        dataset = await Actor.open_dataset(name='socialinfo')
        await dataset.drop()

        dataset = await Actor.open_dataset(name='socialinfo')

        await scrape_sozialinfo_data(dataset)
        await scrape_gemeindestellen_data(dataset)
        await scrape_heiminfo_data(dataset)
        await scrape_jobs_data(dataset)
        await scrape_sozjobs_data(dataset)
        await scrape_stiftungschweiz_data(dataset)
        await scrape_workswiss_data(dataset)
        await scrape_publicjobs_data(dataset)
        await scrape_google_data(dataset)
