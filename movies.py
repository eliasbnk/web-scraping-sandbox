import asyncio
from content_fetcher import ContentFetcher


async def fetch_movie_data(base_url, html_fetcher):
    """
    Extracts Oscar-winning films data from the HTML and JSON content fetched from the specified base URL
    using the given ContentFetcher.

    :param base_url: The base URL to fetch movie data from.
    :param html_fetcher: An instance of ContentFetcher for fetching HTML and JSON content.
    :return: A list containing movie data extracted from the base URL.
    """
    try:
        # Fetch HTML content from the base URL
        html_content = await html_fetcher.fetch_content(base_url, params={})

        # Extract movie years from HTML content
        movie_year_list = [link.text.strip() for link in html_content.select('.year-link')]

        # Fetch JSON content for each movie year concurrently
        tasks = [fetch_movies_for_year(base_url, html_fetcher, movie_year) for movie_year in movie_year_list]
        movies = await asyncio.gather(*tasks)

        # Flatten the list of lists into a single list
        return [movie for sublist in movies for movie in sublist]

    except Exception as e:
        print("Error:", e)


async def fetch_movies_for_year(base_url, html_fetcher, movie_year):
    """
    Fetches JSON content for a specific movie year.

    :param base_url: The base URL to fetch movie data from.
    :param html_fetcher: An instance of ContentFetcher for fetching HTML and JSON content.
    :param movie_year: The year for which to fetch movie data.
    :return: A list containing movie data for the specified year.
    """
    try:
        json_content = await html_fetcher.fetch_content(base_url, params={"ajax": "true", "year": movie_year})
        return json_content

    except Exception as e:
        print(f"Error fetching data for year {movie_year}: {e}")
        return []


async def main():
    """
    The main entry point of the program.
    """
    try:
        base_url = "https://www.scrapethissite.com/pages/ajax-javascript/"
        html_fetcher = ContentFetcher('movie_cache.json')

        # Fetch movie data and print the result
        movies = await fetch_movie_data(base_url, html_fetcher)
        print(movies)

    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    # Run the main function asynchronously
    asyncio.run(main())