import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.genius_yield import genius_yield_constants as CONSTANTS, genius_yield_web_utils as web_utils
from hummingbot.connector.exchange.genius_yield.genius_yield_auth import GeniusYieldAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.genius_yield.genius_yield_exchange import GeniusYieldExchange


class GeniusYieldAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: GeniusYieldAuth,
                 trading_pairs: List[str],
                 connector: 'GeniusYieldExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: GeniusYieldAuth = auth
        self._domain = domain
        self._api_factory = api_factory

    async def _request_user_stream(self):
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(path_url=CONSTANTS.GENIUS_YIELD_USER_STREAM_PATH_URL, domain=self._domain),
                method=RESTMethod.POST,
                throttler_limit_id=CONSTANTS.GENIUS_YIELD_USER_STREAM_PATH_URL,
                headers=self._auth.header_for_authentication()
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            raise IOError(f"Error fetching user stream. Error: {exception}")

        return data

    async def _listen_for_user_stream(self):
        """
        This method listens to the user stream.
        """
        while True:
            try:
                data = await self._request_user_stream()
                # Process the user stream data
                await self._process_user_stream_data(data)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error while listening to user stream. Error: {str(e)}")
                await self._sleep(5.0)
