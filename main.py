'''
Run this file to get a list of the least correlated binance assets
'''

from src.data_preprocessing import PreProcessing
from src.correlation import Correlation
from loguru import logger


def main():
    mcap_cut_off = 20
    lookback_window = 1
    preprocessing = PreProcessing()
    all_data = preprocessing.get_all_ohlc(
        all_pairs=preprocessing.get_coin_list(mcap_cut_off),
        lookback=lookback_window,
        timeframe='15m')
    correlations = Correlation().get_data(df=all_data,
                                          list_return_length=15)
    logger.info(f"Inverse Correlation Table Top {mcap_cut_off} Market Cap")
    print(correlations)


if __name__ == '__main__':
    test = main()
