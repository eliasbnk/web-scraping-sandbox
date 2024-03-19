import aiohttp
import asyncio
import logging
import time
import json
from typing import Any, Dict, Union
from aiohttp import ClientSession
from bs4 import BeautifulSoup as bs

Content = Union[bs, Dict[str, Any]]

class ContentFetcher:
    def __init__(self, cache_file_path: str):
        """
        Initializes the ContentFetcher with a cache file path.

        :param cache_file_path: The file path to the cache file.
        """
        self.logger = logging.getLogger(__name__)
        self.cache_file_path = cache_file_path
        self.successful_cache = {}
        self.bad_request_cache = {}
        self.load_cache_from_file()

    async def fetch_content(self, url: str, params: dict, session: ClientSession = None,
                            force_refresh: bool = False, ignore_bad_request: bool = False) -> Content:
        """
        Fetches content from the given URL with parameters.

        :param url: The URL to fetch content from.
        :param params: The parameters to be sent with the request.
        :param session: Optional aiohttp ClientSession for making HTTP requests.
        :param force_refresh: If True, ignores cache and fetches fresh content.
        :param ignore_bad_request: If True, ignores previous failed requests.
        :return: Parsed content, either BeautifulSoup object (for HTML), list, or parsed JSON object.
        """
        if session is None:
            if self.successful_cache is None and self.bad_request_cache is None:
                self.load_cache_from_file()
            async with aiohttp.ClientSession() as session:
                return await self._fetch_with_session(url, params, session, force_refresh, ignore_bad_request)
        else:
            if self.successful_cache is None and self.bad_request_cache is None:
                self.load_cache_from_file()
            return await self._fetch_with_session(url, params, session, force_refresh, ignore_bad_request)

    async def _fetch_with_session(self, url: str, params: dict, session: ClientSession,
                                  force_refresh: bool, ignore_bad_request: bool) -> Content:
        """
        Internal method to fetch content with a specified session.

        :param url: The URL to fetch content from.
        :param params: The parameters to be sent with the request.
        :param session: aiohttp ClientSession for making HTTP requests.
        :param force_refresh: If True, ignores cache and fetches fresh content.
        :param ignore_bad_request: If True, ignores previous failed requests.
        :return: Parsed content, either BeautifulSoup object (for HTML), list, or parsed JSON object.
        """
        cache_key = json.dumps((url, sorted(params.items())))  # Unique cache key for each request

        # Check if the request is in the successful cache and return the cached data if present
        if not force_refresh and cache_key in self.successful_cache:
            entry = self.successful_cache[cache_key]
            entry['last_request'] = time.time()
            return bs(entry['data'], 'html.parser') if isinstance(entry['data'], str) else entry['data']

        try:
            # Skip checking bad request cache if ignore_bad_request is True
            if not ignore_bad_request and cache_key in self.bad_request_cache:
                entry = self.bad_request_cache[cache_key]
                entry['last_request'] = time.time()  # Update last request time
                self.logger.error("Ignoring request due to previous failure: %s", cache_key)
                raise ValueError(f"Request was previously marked as bad: {cache_key}")

            async with session.get(url, params=params, timeout=10) as response:
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type.lower():
                    html_content = await response.text()
                    if html_content:
                        html_string = str(html_content)
                        entry = {'url': url, 'params': params, 'first_request': time.time(),
                                 'last_request': time.time(), 'data': html_string}
                        # Update cache with the successful response
                        self.successful_cache[cache_key] = entry
                        # Remove URL and params from bad request cache if present
                        if cache_key in self.bad_request_cache:
                            del self.bad_request_cache[cache_key]
                        self.save_cache_to_file()  # Save cache data to file after successful fetch
                        return bs(html_content, 'html.parser')
                    else:
                        # Handle empty HTML content
                        self._handle_error_and_cache(cache_key, url, params, f"Empty HTML content received from {url}",
                                                     ignore_bad_request)
                elif 'application/json' in content_type.lower():
                    json_content = await response.json()
                    entry = {'url': url, 'params': params, 'first_request': time.time(),
                             'last_request': time.time(), 'data': json_content}
                    # Update cache with the successful response
                    self.successful_cache[cache_key] = entry
                    # Remove URL and params from bad request cache if present
                    if cache_key in self.bad_request_cache:
                        del self.bad_request_cache[cache_key]
                    self.save_cache_to_file()  # Save cache data to file after successful fetch
                    return json_content  # Return parsed JSON content
                else:
                    # Handle unexpected content type
                    self._handle_error_and_cache(cache_key, url, params,
                                                 f"Unexpected content type '{content_type}' received from {url}",
                                                 ignore_bad_request)
        except aiohttp.ClientError as e:
            # Handle client errors
            error_message = f"An error occurred during HTTP request: {e} received from {url}"
            self._handle_error_and_cache(cache_key, url, params, error_message, ignore_bad_request)
        except asyncio.TimeoutError:
            # Handle timeout errors
            error_message = f"Request timed out for URL: {url}"
            self._handle_error_and_cache(cache_key, url, params, error_message, ignore_bad_request)
        except Exception as e:
            # Handle unexpected errors
            error_message = f"An unexpected error occurred: {e} received from {url}"
            self._handle_error_and_cache(cache_key, url, params, error_message, ignore_bad_request)

    def _handle_error_and_cache(self, cache_key: Any, url: str, params: Dict[str, Any],
                                error_message: str, ignore_bad_request: bool) -> None:
        """
        Handles errors and caches bad requests.

        :param cache_key: The cache key.
        :param url: The URL of the request.
        :param params: The parameters of the request.
        :param error_message: The error message.
        :param ignore_bad_request: If True, bad requests will not be cached.
        :return: None
        """
        if not ignore_bad_request:
            entry: Dict[str, Any] = {'url': url, 'params': params, 'last_request': time.time(), 'error': error_message}
            if cache_key in self.bad_request_cache:
                entry['first_request'] = self.bad_request_cache[cache_key]['first_request']
            else:
                entry['first_request'] = time.time()
            self.bad_request_cache[cache_key] = entry
            self.save_cache_to_file()  # Save cache data to file when encountering an error
            raise ValueError(error_message)
        else:
            self.logger.error(error_message)

    def load_cache_from_file(self):
        """
        Loads cache data from the cache file.
        """
        try:
            with open(self.cache_file_path, 'r') as file:
                cache_data = json.load(file)
            self.successful_cache = cache_data.get('successful_cache', {})
            self.bad_request_cache = cache_data.get('bad_request_cache', {})
        except FileNotFoundError:
            self.logger.warning("Cache file not found. Starting with empty cache.")
        except json.JSONDecodeError:
            self.logger.warning("Error decoding cache file. Starting with empty cache.")
        except Exception as e:
            self.logger.error("An error occurred while loading cache from file: %s", e)

    def save_cache_to_file(self):
        """
        Saves cache data to the cache file.
        """
        try:
            cache_data = {
                'successful_cache': self.successful_cache,
                'bad_request_cache': self.bad_request_cache
            }
            with open(self.cache_file_path, 'w') as file:
                json.dump(cache_data, file, indent=4)
        except Exception as e:
            self.logger.error("An error occurred while saving cache to file: %s", e)

