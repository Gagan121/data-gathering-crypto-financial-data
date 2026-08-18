import os
import pandas as pd
import pytest
from core.exchanges.exchange_adapter import ExchangeAdapter

@pytest.fixture
def adapter_validate_message_is_false():

    class TestAdapter(ExchangeAdapter):
        def get_structure_of_data(self, data) -> dict:
            return data
        def validate_message(self, msg):
            return False
        def normalise_data(self, batch_list:list) -> list:
            return batch_list


    return TestAdapter(exchange_name="Test", url="ws:test", msg={}, ticker="TEST_TEST")


@pytest.fixture
def adapter():

    class TestAdapter(ExchangeAdapter):
        def get_structure_of_data(self, data) -> dict:
            return data
        def validate_message(self, msg):
            return True
        def normalise_data(self, batch_list:list) -> list:
            return batch_list

    return TestAdapter(exchange_name="Test", url="ws:test", msg={}, ticker="TEST_TEST")

def test_check_quotes_diff_true_case(adapter):
    msg = {'bid':1, 'ask':2}
    adapter.previous_ask_bid_value = {'bid':0, 'ask':0}
    # quotes do differ, in the init function the above is true the values are set to 0
    assert adapter.check_quotes_diff(msg) == True

def test_check_quotes_diff_false_case(adapter):
    msg = {'bid':1, 'ask':2}
    adapter.previous_ask_bid_value = {'bid':1, 'ask':2}
    # quotes do not differ, the previous quotes are the same as they are now
    assert adapter.check_quotes_diff(msg) == False

# -------------------------------------------------------------------

def test_valid_message_can_pass_non_duplicates_quotes(adapter):
    msg = {'bid':1, 'ask':2}
    adapter.previous_ask_bid_value = {'bid':0, 'ask':0}
    # quotes do differ, in the init function the above is true the values are set to 0
    assert adapter.valid_message_can_pass(msg) == True

def test_valid_message_can_pass_duplicates_quotes(adapter):
    msg = {'bid':1, 'ask':2}
    adapter.previous_ask_bid_value = {'bid':1, 'ask':2}
    # quotes do differ, in the init function the above is true the values are set to 0
    assert adapter.valid_message_can_pass(msg) == False

# -------------------------------------------------------------------------------

def test_valid_message_can_pass_non_duplicates_validate_false(adapter_validate_message_is_false):
    msg = {'bid': 1, 'ask': 2}
    adapter_validate_message_is_false.previous_ask_bid_value = {'bid': 0, 'ask': 0}
    # quotes do differ, in the init function the above is true the values are set to 0
    assert adapter_validate_message_is_false.valid_message_can_pass(msg) == False

def test_valid_message_can_pass_duplicates_validate_false(adapter_validate_message_is_false):
    msg = {'bid': 1, 'ask': 2}
    adapter_validate_message_is_false.previous_ask_bid_value = {'bid': 1, 'ask': 2}
    # quotes do differ, in the init function the above is true the values are set to 0
    assert adapter_validate_message_is_false.valid_message_can_pass(msg) == False


def test_write_data_to_file(adapter):
    path_to_folder = (adapter.PATH_DIR / adapter.exchange_name).resolve()
    if path_to_folder.exists():
        for file in os.listdir(path_to_folder):
            file_path = os.path.join(path_to_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

    data = [
        {
            "price":1000,
            "volume":2000,
            "index":0
        },
        {
            "price": 2000,
            "volume": 2000,
            "index": 1
        },
        {
            "price": 3000,
            "volume": 3000,
            "index": 2
        },
    ]
    original_df = pd.DataFrame(data)


    # ---------------------------------------------
    adapter.writer(data)

    path_to_newly_created_file = None
    for file in os.listdir(path_to_folder):
        file_path = os.path.join(path_to_folder, file)
        if os.path.isfile(file_path):
            path_to_newly_created_file = file_path
            break

    if path_to_newly_created_file is None:
        assert False

    new_df_from_parquet = pd.read_parquet(path_to_newly_created_file)


    pd.testing.assert_frame_equal(original_df.reset_index(drop=True), new_df_from_parquet.reset_index(drop=True))






