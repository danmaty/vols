from time import strftime as stt
from deta import Deta
import streamlit as st
import pandas as pd
import zulip
import os


def deta_to_df():
    try:
        deta = Deta(os.environ.get('db_key'))
        db = deta.Base(os.environ.get('db_name'))

        res = db.fetch()
        all_items = res.items

        while res.last:
            res = db.fetch(last=res.last)
            all_items += res.items
    except Exception as e:
        print('deta 2 df 1', e)

    try:
        df0 = pd.DataFrame(all_items)
        df1 = df0['data'].tolist()
        df = pd.DataFrame(df1)
        df = df.replace('PALLET', '0')

        df['datetime'] = df0['datetime']
        df = df.sort_values('datetime')
    except Exception as e:
        print('deta 2 df 2', e)
        df = pd.DataFrame()

    try:
        df['ret-arr-it'] = df['ret-arr'].str[0]
        df['ret-arr-xf'] = df['ret-arr'].str[1]
        df['ast-arr-it'] = df['ast-arr'].str[0]
        df['ast-arr-xf'] = df['ast-arr'].str[1]

        df['ret-sch-it'] = df['ret-sch'].str[0]
        df['ret-sch-xf'] = df['ret-sch'].str[1]
        df['ast-sch-it'] = df['ast-sch'].str[0]
        df['ast-sch-xf'] = df['ast-sch'].str[1]

        df = df.apply(pd.to_numeric, errors='ignore')

        df['ret-arr-cf'] = df['ret-arr-it'] / df['ret-arr-xf']
        df['ret-sch-cf'] = df['ret-sch-it'] / df['ret-sch-xf']
        df['ast-arr-cf'] = df['ast-arr-it'] / df['ast-arr-xf']
        df['ast-sch-cf'] = df['ast-sch-it'] / df['ast-sch-xf']
    except Exception as e:
        print('deta2df 3', e)
        df = pd.DataFrame()

    try:
        cols = ['datetime',
                'ret-arr-it', 'ret-sch-it', 'ret-arr-xf', 'ret-sch-xf', 'ret-arr-cf', 'ret-sch-cf',
                'ast-arr-it', 'ast-arr-xf', 'ast-arr-cf', 'ast-sch-it', 'ast-sch-xf', 'ast-sch-cf',
                'jbb', 'jbb-it', 'gry', 'gry-it', 'clk', 'clk-it', 'rec', 'rec-it', 'crs', 'crs-it',
                '300', '300-it', 'blk', 'blk-it', 'grn', 'grn-it', 'orn', 'orn-it', 'red', 'red-it', 'yel', 'yel-it']

        df = df[cols]
        df.reset_index(drop=True, inplace=True)
        df['datetime'] = df['datetime'].str[5:16]
    except Exception as e:
        print('deta2df 4', e)
        df = pd.DataFrame()

    try:
        df = df.tail(2)
        df = df.reset_index()
        # df = df[['datetime',
        #          'ret-arr-it', 'ret-sch-it',
        #          'ret-arr-xf', 'ret-sch-xf',
        #          'ret-arr-cf', 'ret-sch-cf',
        #          'ast-arr-it', 'ast-sch-it',
        #          'ast-arr-xf', 'ast-sch-xf',
        #          'ast-arr-cf', 'ast-sch-cf']]
        df = df[['datetime',
                 'ret-arr-it', 'ret-sch-it',
                 'ast-arr-it', 'ast-sch-it']]
    except Exception as e:
        print('deta2df 5', e)
        df = pd.DataFrame()

    try:
        zulip.Client(api_key=os.environ.get('msg_key'),
                     email=os.environ.get('msg_mail'),
                     site=os.environ.get('msg_site')).send_message({"type": "private", "to": [int(os.environ.get('msg_to'))],
                                                                    "content": f"Vols checked at {stt('%H:%M:%S on %d-%m-%y')}"})
    except Exception as e:
        print('action_zulip', e)

    return df.copy()


st.set_page_config(
    page_title='Reverse Flow Volumes',
    page_icon='📶',
)

data = deta_to_df()
ar = data['ret-arr-it'].tolist()
sr = data['ret-sch-it'].tolist()
aa = data['ast-arr-it'].tolist()
sa = data['ast-sch-it'].tolist()

c1, c2 = st.columns(2)
c1.metric(label='Returns Arrived', value=ar[1], delta=ar[1]-ar[0])
c2.metric(label='A-Stock Arrived', value=aa[1], delta=aa[1]-aa[0])
c1.metric(label='Returns Scheduled', value=sr[1], delta=sr[1]-sr[0])
c2.metric(label='A-Stock Scheduled', value=sa[1], delta=sa[1]-sa[0])
