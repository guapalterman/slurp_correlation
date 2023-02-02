import pandas as pd
from binance.client import Client
import requests
from binance.exceptions import BinanceAPIException
from loguru import logger


class PreProcessing():
    def __init__(self):
        logger.info("Aggregation Module Initiated")
        self.client = Client("", "")

    def get_coin_list(self, mcap_cut_off) -> list:
        black_list = ['USDT', 'USTC', 'USDC', 'USDN', 'BUSD',
                      'DAI', 'TUSD', 'USDP', "USDD", "FEI",
                      "WBTC", "WETH"]
        url = f'https://api.coinmarketcap.com/data-api/v3/map/all?listing_status=active,untracked&exchangeAux=is_active,status&cryptoAux=is_active,status&start=1&limit={mcap_cut_off}' # noqa
        top_assets = requests.get(url).json()
        all_pairs = []
        for entry in top_assets.get("data").get("cryptoCurrencyMap"):
            coin = entry.get("symbol")
            if (coin == "MIOTA"):
                coin = "IOTA"
            if not (coin in (black_list)):
                all_pairs.append(coin)
        print(all_pairs)
        return all_pairs
    
    def kline_mapper(self, kline):
        return [
            int(kline[0]),
            float(kline[1]),
            float(kline[2]),
            float(kline[3]),
            float(kline[4]),
            float(kline[5]),
            float(kline[7]),
            float(kline[8]),
            float(kline[9]),
            float(kline[10]),
        ]

    def to_dataframe(self, data):
        klines = [self.kline_mapper(d) for d in data]
        df = pd.DataFrame(klines, columns=['timestamp', 'Open', 'High', 'Low',
                                           'Close', 'TotalVolumeBase',
                                           'TotalVolumeQuote', 'NTrades',
                                           'TakerBuyBaseVolume',
                                           'TakerBuyQuoteVolume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp').sort_index(ascending=True)
        return df

    def get_binance_data(self, interval: str, pair: str):
        data = self.client.get_historical_klines(symbol=pair,
                                                 interval=interval)
        df_data = self.to_dataframe(data[:-1])
        df_data['volume_delta'] = \
            df_data['TakerBuyQuoteVolume'].mul(2).sub(
                df_data['TotalVolumeQuote'])
        df_data['cvd'] = df_data['volume_delta'].cumsum()
        df_data = df_data.drop(['TotalVolumeBase', 'NTrades',
                                'TakerBuyBaseVolume',
                                'TakerBuyQuoteVolume'], axis=1)
        return df_data

    def get_ohlc(self, coin, lookback, timeframe):
        """Desired OHLC data for BTC and ETH."""
        try:
            start_date = f"{lookback} days ago UTC"
            klines = self.client.get_historical_klines(coin,
                                                       timeframe,
                                                       start_date)
            asset_df = pd.DataFrame(klines)
            if len(asset_df) == 0:
                return None
            asset_df.columns = ['open_time', 'open', 'high', 'low', 'close',
                                'volume', 'close_time', 'qav',
                                'num_trades', 'taker_base_vol',
                                'taker_quote_vol', 'ignore']
            asset_df['close_time'] = pd.to_datetime(asset_df['close_time'],
                                                    unit="ms")
            asset_df.set_index(['close_time'], inplace=True)
            return asset_df
        except BinanceAPIException as e:
            logger.info(f"{e.message} : Asset = {coin}")

    def get_all_ohlc(self, all_pairs, lookback, timeframe):
        final_df = pd.DataFrame()
        all_pairs_usdt = [x+'USDT' for x in all_pairs]
        all_pairs_busd = [x+'BUSD' for x in all_pairs]
        for coin_usdt, coin_busd in zip(all_pairs_usdt, all_pairs_busd):
            try:
                asset_df_usdt = self.get_ohlc(coin_usdt, lookback, timeframe)
                asset_df_busd = self.get_ohlc(coin_busd, lookback, timeframe)
                if ((asset_df_usdt is not None) and (asset_df_busd is None)) or  ((asset_df_usdt is not None) and (asset_df_busd is not None)):
                    asset_df_usdt = asset_df_usdt.rename(columns={'close': coin_usdt}) # noqa
                    final_df = pd.concat([final_df, asset_df_usdt[coin_usdt]], axis=1) # noqa
                elif (asset_df_usdt is None) and (asset_df_busd is not None):
                    asset_df_busd = asset_df_busd.rename(columns={'close': coin_busd}) # noqa
                    final_df = pd.concat([final_df, asset_df_busd[coin_busd]], axis=1) # noqa
                else:
                    continue
            except BinanceAPIException as e:
                ticker = coin_usdt[:-3]
                logger.info(f"{e.message} : Asset = {ticker}")
        final_df = final_df.reset_index(drop=True)
        final_df = final_df.astype(float)
        return final_df
