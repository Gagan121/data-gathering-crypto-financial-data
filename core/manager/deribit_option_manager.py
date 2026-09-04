from core.complex_exchanges.exchanges_with_expiry import ExchangeWithExpiry
from core.manager.manage_subscription import ManageSubscription
import copy
from core.complex_exchanges.deribit_options_adapter import DeribitOptionsAdapter, DeribitOptionsConfig

class DeribitOptionManager(ManageSubscription[DeribitOptionsConfig]):
    def __init__(self, pipelines: list, limit_of_number_of_channels:int):
        super().__init__(pipelines=pipelines, limit_of_number_of_channels=limit_of_number_of_channels)

    @classmethod
    def generate_multiple_adapters(cls, deribit_options_config:DeribitOptionsConfig) -> list:
        data = {
            'currency': deribit_options_config.currency,
            'expired': deribit_options_config.expired,
        }
        information = DeribitOptionsAdapter.get_instruments(config=deribit_options_config)
        # true if information is there
        if not (bool(information)):
            return []

        list_of_instruments_dict = DeribitOptionsAdapter.sort_data_form_new_requests(information)

        total_channels = cls.format_instruments_to_channels(list_of_instruments_dict=list_of_instruments_dict, config=deribit_options_config)

        list_of_lists_of_channels = [total_channels[x:x + deribit_options_config.limit_number_of_channels] for x in range(0, len(total_channels), deribit_options_config.limit_number_of_channels)]
        list_of_adapters = []
        for i in range(len(list_of_lists_of_channels)):
            # we have to create a copy here otherwise pass by reference would make all the msg the same
            adapter_msg = copy.deepcopy(deribit_options_config.msg)
            adapter_msg["params"]['channels'] = list_of_lists_of_channels[i]
            list_of_adapters.append(
                DeribitOptionsAdapter(
                    channels=list_of_lists_of_channels[i],
                    exchange_name=deribit_options_config.exchange_name,
                    websocket_url=deribit_options_config.websocket_url,
                    msg=deribit_options_config.msg,
                    ticker=deribit_options_config.currency,
                    heart_beat_msg=deribit_options_config.heart_beat_msg,
                    heart_beat_reply_msg=deribit_options_config.heart_beat_reply_msg,
                    base_url=deribit_options_config.base_url,
                    exchange_info=data,
                )
            )

        return list_of_adapters

    @staticmethod
    def convert_data_to_single_list_of_channels(data:list) -> list:
        list_of_instruments = []
        for instrument_dict in data:
            if "instrument_name" not in instrument_dict:
                continue
            instrument_name = instrument_dict["instrument_name"]
            list_of_instruments.append(instrument_name)
        return list_of_instruments

    @staticmethod
    def format_instruments_to_channels(list_of_instruments_dict: list, config: DeribitOptionsConfig):
        total_channels = []
        for item in list_of_instruments_dict:
            for types_of_data in config.data_types:
                total_channels.append(f"{types_of_data}.{item['instrument_name']}.{config.interval_type}")

        return total_channels


    def find_instruments(self, config:DeribitOptionsConfig) -> list:
        if len(self.pipelines) <= 0:
            return []

        exchange_adapter_with_expiry = self.pipelines[0].get_exchange_adapter()

        if not isinstance(exchange_adapter_with_expiry, ExchangeWithExpiry):
            return []

        information = exchange_adapter_with_expiry.get_instruments(config=config)
        list_of_instruments_dict = exchange_adapter_with_expiry.sort_data_form_new_requests(information)

        return list_of_instruments_dict


