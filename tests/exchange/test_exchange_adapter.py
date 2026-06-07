import os
import pandas as pd
import pytest
from core.exchanges.exchange_adapter import ExchangeAdapter, create_base_folder

@pytest.fixture
def adapter():

    class TestAdapter(ExchangeAdapter):
        def validate_message(self, msg):
            return True
        def normalise_data(self, batch_list:list) -> list:
            return batch_list


    path_to_folder = r"D:\python_projects\data_gathering_using_websockets_finance_crypto\data"

    return TestAdapter(path_to_folder=path_to_folder, exchange_name="Test", url="ws:test", msg={})


def test_folder_creation(adapter):
    new_path_to_folder = create_base_folder(adapter.exchange_name, adapter.path_to_folder)

    assert os.path.exists(new_path_to_folder)
    assert os.path.isdir(new_path_to_folder)


def test_write_data_to_file(adapter):
    path_to_folder = os.path.join(adapter.path_to_folder, adapter.exchange_name)
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
    else:
        assert True

    new_df_from_parquet = pd.read_parquet(path_to_newly_created_file)


    pd.testing.assert_frame_equal(original_df.reset_index(drop=True), new_df_from_parquet.reset_index(drop=True))






