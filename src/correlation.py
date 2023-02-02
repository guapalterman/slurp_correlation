import pandas as pd
from loguru import logger


class Correlation():
    def __init__(self):
        logger.info("Start Correlation Calculations")

    def get_correlation(self, df):
        correlations = df.corr()
        correlation_row_btc = correlations['BTCUSDT']
        correlation_row_eth = correlations['ETHUSDT']
        correlation_row_btc = correlation_row_btc.round(3)
        correlation_row_eth = correlation_row_eth.round(3)
        sorted_corr = correlation_row_btc.sort_values()
        merged_df = pd.concat([sorted_corr, correlation_row_eth], axis=1)
        return merged_df

    def add_returns(self, corr, df):
        return_list = []
        for asset in corr.index:
            returns = str(round(((df[asset].iloc[-1] /
                                  df[asset].iloc[0])-1) * 100, 3)) + '%'
            return_list.append(returns)
        return return_list

    def add_price(self, corr, df):
        price_list = []
        for asset in corr.index:
            price = str(df[asset].iloc[-1])
            price_list.append(price)
        return price_list

    def get_data(self, df, list_return_length):
        corr = self.get_correlation(df)
        return_list = self.add_returns(corr, df)
        corr['3D Returns'] = return_list
        price_list = self.add_price(corr, df)
        corr['Price'] = price_list
        return corr[:list_return_length]
