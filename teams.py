import asyncio
from content_fetcher import ContentFetcher


async def fetch_team_data(base_url, html_fetcher, page_number):
    """
    Extracts hockey team data from the HTML content fetched from the specified base URL
    using the given ContentFetcher.

    :param base_url: The base URL to fetch team data from.
    :param html_fetcher: An instance of ContentFetcher for fetching HTML content.
    :param page_number: The page number to fetch data from.
    :return: A list containing hockey team data extracted from the base URL.
    """
    try:
        params = {"page_num": page_number}
        html_content = await html_fetcher.fetch_content(base_url, params=params)
        teams_html = html_content.find_all('tr', class_="team")
        team_list = []

        # Extract hockey team information from HTML content
        for team in teams_html:
            team_info = {
                "team_name": team.find('td', class_="name").text.strip(),
                "team_year": team.find('td', class_="year").text.strip(),
                "team_wins": team.find('td', class_="wins").text.strip(),
                "team_losses": team.find('td', class_="losses").text.strip(),
                "team_ot_losses": team.find('td', class_="ot-losses").text.strip(),
                "team_goals_for": team.find('td', class_="gf").text.strip(),
                "team_goals_against": team.find('td', class_="ga").text.strip(),
                "team_diff": team.find('td', class_="diff").text.strip()
            }
            team_list.append(team_info)

        return team_list

    except Exception as e:
        print("Error:", e)


async def main():
    """
    The main entry point of the program.
    """
    try:
        base_url = "https://www.scrapethissite.com/pages/forms/"
        html_fetcher = ContentFetcher('team_cache.json')
        num_pages = 24

        # Fetch team data concurrently for all pages
        tasks = [fetch_team_data(base_url, html_fetcher, page_number) for page_number in range(1, num_pages + 1)]
        all_teams = await asyncio.gather(*tasks)

        # Flatten the list of lists into a single list
        teams = [team for sublist in all_teams for team in sublist]

    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    # Run the main function asynchronously
    asyncio.run(main())