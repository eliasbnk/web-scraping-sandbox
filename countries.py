import asyncio
from content_fetcher import ContentFetcher


async def fetch_country_data(url, html_fetcher):
    """
    Extracts country information from the HTML content fetched from the specified URL
    using the given ContentFetcher.

    :param url: The URL to fetch country data from.
    :param html_fetcher: An instance of ContentFetcher for fetching HTML content.
    :return: A list containing country data extracted from the HTML content.
    """
    try:
        html_content = await html_fetcher.fetch_content(url, params={})
        countries_html = html_content.find_all('div', class_="col-md-4 country")
        country_list = []

        # Fetch country information concurrently
        tasks = [extract_country_info(country, html_fetcher) for country in countries_html]
        country_list = await asyncio.gather(*tasks)

        return country_list

    except Exception as e:
        print("Error:", e)


async def extract_country_info(country, html_fetcher):
    """
    Extracts information for a single country.

    :param country: The HTML content of a country.
    :param html_fetcher: An instance of ContentFetcher for fetching HTML content.
    :return: A dictionary containing country information.
    """
    try:
        country_info = {
            'country_name': country.find('h3', class_='country-name').text.strip(),
            'country_capital': country.find('span', class_='country-capital').text.strip(),
            'country_population': country.find('span', class_='country-population').text.strip(),
            'country_area': f"{country.find('span', class_='country-area').text.strip()} km2"
        }

        return country_info

    except Exception as e:
        print("Error extracting country info:", e)


async def main():
    """
    The main entry point of the program.
    """
    try:
        url = "http://www.scrapethissite.com/pages/simple/"
        html_fetcher = ContentFetcher('country_cache.json')

        # Fetch and extract country data and print the result
        country_list = await fetch_country_data(url, html_fetcher)
        print(country_list)

    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    # Run the main function asynchronously
    asyncio.run(main())

# Web Scraping Sandbox found on https://www.scrapethissite.com/