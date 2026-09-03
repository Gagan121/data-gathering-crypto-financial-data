from core.complex_exchanges.exchanges_with_expiry import ExchangeWithExpiry
from core.manager.manage_subscription import ManageSubscription
from core.complex_exchanges.deribit_options_adapter import ExchangeWithExpiry

class DeribitOptionManager(ManageSubscription):
    def __init__(self, pipelines: list, limit_of_number_of_channels:int):
        super().__init__(pipelines=pipelines, limit_of_number_of_channels=limit_of_number_of_channels)

    def find_instruments(self):
        if len(self.pipelines) <= 0:
            return []

        exchange_adapter_with_expiry = self.pipelines[0].get_exchange_adapter()

        if not isinstance(exchange_adapter_with_expiry, ExchangeWithExpiry):
            return []

        websocket_url = exchange_adapter_with_expiry.get_websocket_url()
        exchange_info = exchange_adapter_with_expiry.get_exchange_info()
        information = exchange_adapter_with_expiry.get_instruments(base_url=websocket_url, exchange_info=exchange_info)
        list_of_instruments = exchange_adapter_with_expiry.sort_data_form_new_requests(information)

        return list_of_instruments

