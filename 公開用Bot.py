# coding: utf-8
from tornado import gen
import requests
import json
import pybitflyer
import numpy as np
import time
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub_tornado import PubNubTornado
from pubnub.pnconfiguration import PNReconnectionPolicy
import pandas as pd
from datetime import datetime, timezone, timedelta

#API_KEYとAPI_SECRETを記述
#my_order_size == 一度に買いたい量
my_order_size = 0.001
API_KEY = "Your_API_KEY"
API_SECRET = "Your_API_SECRET"


config = PNConfiguration()
config.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
config.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNubTornado(config)
api = pybitflyer.API(api_key=API_KEY, api_secret=API_SECRET)
df_all = pd.DataFrame(index=['datetime'],
                  columns=['id',
                           'side',
                           'price',
                           'size',
                           'exec_date',
                           'buy_child_order_acceptance_id',
                           'sell_child_order_acceptance_id'])

# entry buy or sell position
def entry(side, order_size):
    print('[' + side + ' Entry]')
    now_tick = api.ticker(product_code="FX_BTC_JPY")
    #現在の中間価格を代入
    if side == 'BUY':
        price = float(now_tick["best_bid"])
    elif side == 'SELL':
        price = float(now_tick["best_ask"])
    else:
        price = float(now_tick["best_bid"])

    callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='LIMIT', side=side, price=price, size=order_size)
    print(callback)
    if not(callback.get('status')):
        print('Order Complete!')
        return side
    else:
        #親注文キャンセル
        api.cancelallchildorders(product_code= 'FX_BTC_JPY')
        return 'NONE'

def close(side, order_size):
    api.cancelallchildorders(product_code= 'FX_BTC_JPY')
    tatedama = api.getpositions(product_code='FX_BTC_JPY')
    close_order_size = 0
    for i in tatedama:
        close_order_size += float(i["size"])
    order_size = close_order_size
    oposit_side = 'NONE'
    if side == 'BUY':
        oposit_side = 'SELL'
    elif side == 'SELL':
        oposit_side = 'BUY'
    bf_positions = pd.DataFrame(tatedama)
    if not(bf_positions.empty):
        bf_pos = bf_positions.ix[[0], ['side']].values.flatten()
        bf_pos_price = int(bf_positions.ix[[0], ['price']].values.flatten())
        if bf_pos == side:
            print('[' + side + ' Close]')
            callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='MARKET', side=oposit_side, size=order_size)
            print(callback)
            if not(callback.get('status')):
                print('Order Complete!')
                return 'NONE'
    else:
        return side
chumon_umu = 0
count = 0
my_position = "NONE"
my_last_position = "NONE"
#1 == buy, -1 == sell
buy_or_sell_flag = 0
#1 == buyVol>sellVol, -1 == sellVol>buyVol
buyVol_sellVol_balance = 0
#大きいのはどっち？
#BUY_1hour_VOL と Buy_15m_VOL と Buy_5sec_VOL が全てSELLよりも高ければ買いポジ
#買いポジ持っている時にBuy_15m_VOL と Buy_5sec_VOL がSELLよりも低くなったら買いポジ解消
#
#SELL_1hour_VOL と SELL_15m_VOL と SELL_5sec_VOL が全てBUYよりも高ければ売りポジ
#売りポジ持っている時にSELL_15m_VOL と SELL_5sec_VOL がBUYよりも低くなったら売りポジ解消
def buy_or_sell(buy_1h_vol,buy_15m_vol,buy_5s_vol,sell_1h_vol,sell_15m_vol,sell_5s_vol):
    margin = 1
    if buy_1h_vol > sell_1h_vol and buy_15m_vol > sell_15m_vol*margin and buy_5s_vol > sell_5s_vol*margin:
        return "BUY"
    elif buy_1h_vol < sell_1h_vol and buy_15m_vol*margin < sell_15m_vol and buy_5s_vol*margin < sell_5s_vol:
        return "SELL"
    else:
        return "NONE"
        api.cancelallchildorders(product_code= 'FX_BTC_JPY')

def close_or_dont_close(buy_15m_vol,buy_5s_vol,sell_15m_vol,sell_5s_vol,pos):
    print("cd")
    margin = 1
    if pos == "BUY":
        if buy_15m_vol*margin < sell_15m_vol and buy_5s_vol*margin < sell_5s_vol:
            return "CLOSE"
        else:
            return "DONT_CLOSE"
    elif pos == "SELL":
        if buy_15m_vol > sell_15m_vol*margin and buy_5s_vol > sell_5s_vol*margin:
            return "CLOSE"
        else:
            return "DONT_CLOSE"
    else:
        return "DONT_CLOSE"

@gen.coroutine #非同期処理
def main(channels):
    class BitflyerSubscriberCallback(SubscribeCallback):
        def presence(self, pubnub, presence):
            pass  # handle incoming presence data

        def status(self, pubnub, status):
            if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass  # This event happens when radio / connectivity is lost

            elif status.category == PNStatusCategory.PNConnectedCategory:
                # Connect event. You can do stuff like publish, and know you'll get it.
                # Or just use the connected event to confirm you are subscribed for
                # UI / internal notifications, etc
                pass
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
                # Happens as part of our regular operation. This event happens when
                # radio / connectivity is lost, then regained.
            elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
                pass
                # Handle message decryption error. Probably client configured to
                # encrypt messages and on live data feed it received plain text.

        def message(self, pubnub, message):
            # Handle new message stored in message.message
            try:
                task(message.channel, message.message)
            except Exception as e:
                print(e)

    listener = BitflyerSubscriberCallback()
    pubnub.add_listener(listener)
    pubnub.subscribe().channels(channels).execute()

def task(channel, message):
    #注文量
    global my_order_size
    global buy_or_sell_flag
    global buyVol_sellVol_balance
    global api
    for i in message:
        df_new = pd.DataFrame(message)
        df_new['exec_date'] = pd.to_datetime(df_new['exec_date'])
    global my_position
    global my_last_position
    global chumon_umu
    global df_all
    #どこかで配列の計算に使わない古い要素を消していかないとメモリー超過やばそう
    df_all = df_all.append(df_new)
    df_all.index = df_all['exec_date']
    date_now = df_all.index[len(df_all)-1]
    vol_df_5sec_sec = 5
    vol_df_1hour_sec = 3600
    vol_df_15min_sec = 900
    vol_df_all_sec = vol_df_1hour_sec *1.2
    df_5sec = df_all.ix[df_all.index >= (date_now - timedelta(seconds=vol_df_5sec_sec))]
    df_1hour = df_all.ix[df_all.index >= (date_now - timedelta(seconds=vol_df_1hour_sec))]
    df_15min = df_all.ix[df_all.index >= (date_now - timedelta(seconds=vol_df_15min_sec))]
    df_all = df_all[df_all.index >= (date_now - timedelta(seconds=vol_df_all_sec))]
    buy_vol = df_15min[df_15min.apply(lambda x: x['side'], axis=1) == "BUY"]['size'].sum(axis=0)
    sell_vol = df_15min[df_15min.apply(lambda x: x['side'], axis=1) == "SELL"]['size'].sum(axis=0)
    buy_5sec_vol = df_5sec[df_5sec.apply(lambda x: x['side'], axis=1) == "BUY"]['size'].sum(axis=0)
    sell_5sec_vol = df_5sec[df_5sec.apply(lambda x: x['side'], axis=1) == "SELL"]['size'].sum(axis=0)
    buy_1hour_vol = df_1hour[df_1hour.apply(lambda x: x['side'], axis=1) == "BUY"]['size'].sum(axis=0)
    sell_1hour_vol = df_1hour[df_1hour.apply(lambda x: x['side'], axis=1) == "SELL"]['size'].sum(axis=0)
    print(df_15min.index[0].strftime('%Y-%m-%d %H:%M:%S'),
          df_15min.index[len(df_15min)-1].strftime('%H:%M:%S'),
          "BUY_1h_VOL", format(buy_1hour_vol, '.2f'),
          "SELL_1h_VOL", format(sell_1hour_vol, '.2f'),
          "BUY_15m_VOL", format(buy_vol, '.2f'),
          "SELL_15m_VOL", format(sell_vol, '.2f'),
          "BUY_5s_VOL", format(buy_5sec_vol, '.2f'),
          "SELL_5s_VOL", format(sell_5sec_vol, '.2f'))
    try:
        if (df_all['exec_date'][-1]-df_all['exec_date'][1]).seconds > vol_df_1hour_sec:
            if chumon_umu == 0:
                my_position = buy_or_sell(buy_1hour_vol,buy_vol,buy_5sec_vol,sell_1hour_vol,sell_vol,sell_5sec_vol)
                #"BUY"か"SELL"posがあればentry
                if my_position != "NONE" and my_position != my_last_position:
                    my_position = entry(my_position,my_order_size)
                    my_last_position = my_position
                    print("POSITION: ",my_position)
                    chumon_umu = 1
            elif chumon_umu == 1:
                close_flag = close_or_dont_close(buy_vol,buy_5sec_vol,sell_vol,sell_5sec_vol,my_position)
                print(close_flag)
                if "CLOSE" == close_flag:
                    close_result = close(my_position,my_order_size)
                    if close_result == "NONE":
                        chumon_umu = 0
                    print("POSITION: ",close_result)
                    my_position = "NONE"
            else:
                print(my_position)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main(['lightning_executions_FX_BTC_JPY'])
    pubnub.start()
