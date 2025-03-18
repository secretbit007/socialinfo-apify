from .sozialinfo import scrape_sozialinfo_data
from .gemeindestellen import scrape_gemeindestellen_data
from .google import scrape_google_data
from .heiminfo import scrape_heiminfo_data
from .jobs import scrape_jobs_data
from .publicjobs import scrape_publicjobs_data
from .sozjobs import scrape_sozjobs_data
from .stiftungschweiz import scrape_stiftungschweiz_data
from .workswiss import scrape_workswiss_data

async def main() -> None:
    await scrape_sozialinfo_data()
    await scrape_gemeindestellen_data()
    await scrape_heiminfo_data()
    await scrape_jobs_data()
    await scrape_sozjobs_data()
    await scrape_stiftungschweiz_data()
    await scrape_workswiss_data()
    await scrape_publicjobs_data()
    await scrape_google_data()
