import pandas as pd
import numpy as np
import modules.config as config


class Loader:
    def __init__(self):
        self.customer_cols = [
            "id", "name", "payer_id", "payer_name", "City", "post_code", 
            "Country_Region_Code", "is_b2c", "Latitude", "Longitude", "Population"
        ]
        self.customer_dtypes = {
            'latitude': float,
            'longitude': float,
            'population': float,
            'is_b2c': bool
        }
        self.inventory_cols = [
            "sku", "descr", "category", "magento_inv", "last_cost", "unit_price", "CATEGORIES", "NLS"
        ]
        self.inventory_dtypes = {
            'magento_inv': int,
            'last_cost': float,
            'unit_price': float
        }
        self.sales_data_cols = [
            "sku", "cust_id", "payer_id", "document", "ddate", "quantity", "cost", "price", "username"
        ]
        self.sales_data_dtypes = {
            "quantity": int,
            "cost": float,
            "price": float
        }

    # -------------- CUSTOMER DATALOADER --------------

    def load_customers(self):
        df = pd.read_csv(str(config.CUSTOMER_PATH), dtype=str, sep='|')
        df_geo = self._load_customers_geo()
        df = pd.merge(df, df_geo, left_on='id', right_on='No', how='left')
        df = df[self.customer_cols]
        df.columns = map(str.lower, df.columns)
        df.is_b2c = df.is_b2c.apply(lambda x: True if x == 't' else False)
        df = df.astype(self.customer_dtypes)
        return df


    def _load_customers_geo(self):
        df = pd.read_csv(str(config.CUSTOMER_GEO_PATH), dtype=str, sep='|')
        return df


    # -------------- INVENTORY DATALOADER --------------


    def load_inventory(self):
        df = pd.read_csv(str(config.INVENTORY_PATH), dtype=str, sep='|')
        df_cat = self._load_categories()
        df = pd.merge(df, df_cat, left_on='sku', right_on='Artikel', how='left')
        df['NLS'] = df['descr'].apply(lambda x: 'NLS' in x)
        df = df[self.inventory_cols]
        df = df.astype(self.inventory_dtypes)
        df = df.rename(columns={
            'magento_inv': 'quantity',
            'category': 'brand',
            'CATEGORIES': 'category'
        })
        df = df.loc[~df['sku'].str.startswith('188R')]
        return df


    def _load_categories(self):
        df = pd.read_excel(str(config.CATEGORIES_PATH), dtype=str)
        return df


    # -------------- SALES_DATA DATALOADER --------------


    def add_nls(self, df_sal, df_inv):
        df_inv = df_inv[['sku', 'NLS']]
        df_sal = pd.merge(df_sal, df_inv, how='left', left_on='sku', right_on='sku')
        df_sal.NLS = df_sal.NLS.fillna(True)
        return df_sal


    def add_channels(self, df_sal, df_cust):
        df_cust = df_cust[['id', 'is_b2c']]
        df_sal = pd.merge(df_sal, df_cust, how='left', left_on='cust_id', right_on='id')
        df_sal['channel'] = df_sal['is_b2c'].fillna(False).apply(lambda x: 'B2C' if x else 'B2B')
        df_sal.loc[df_sal['sales_person'] == 'SHOP', 'channel'] = 'SHOP'
        df_sal = df_sal.drop(columns=['is_b2c', 'id'])
        return df_sal


    def load_sales_data(self):
        df = pd.read_csv(str(config.SALES_DATA_PATH), sep='|', dtype=str)
        df = df[self.sales_data_cols]
        df = df.astype(self.sales_data_dtypes)
        df = df.rename(columns={
            "document": "invoice_no",
            "username": "sales_person"
        })
        df.ddate = pd.to_datetime(df.ddate, format="%Y-%m-%d")
        df_old = self._load_old_sales_data()
        df = pd.concat([df_old, df], ignore_index=True)
        df['is_return'] = df['quantity'].apply(lambda x: x < 0)
        df = df.loc[~df['sku'].str.startswith('188R')]
        return df
    

    def _fix_old_sales_person(self, df):
        df['username'] = df['username'].str.replace(
            r'(B2C|B2B)', ''
        ).str.strip()
        df.loc[df['username'] == 'LEHNERJ', 'username'] = 'LEHNER'
        df.loc[df['username'] == 'JV', 'username'] = 'JOZE'
        df.loc[
            (df['SM'] == 'PC2-522') &
            (~df['username'].isin(['LEHNER', 'DAMJAN'])),
            'username'
        ] = 'SHOP'
        df.loc[
            (df['username'] == 'BENJAMIN') &
            (df['country'] == 'HR'),
            'username'
        ] = 'BENJAMIN_HR'
        return df


    def _load_old_sales_data(self):
        # This dataset needs to be cleaned up before merging with newer sales
        df = pd.read_excel(str(config.OLD_SALES_DATA_PATH), dtype=str)
        df = df.loc[df['SM'].isin(['PC2-522', 'PC2-527'])]
        df['cost'] = df['cost'].str.replace(",", ".")
        df['price'] = df['price'].str.replace(",", ".")
        df = df.loc[df['quantity'].str.lstrip('-').str.isdigit()]
        df = self._fix_old_sales_person(df)
        df['ddate'] = pd.to_datetime(df['ddate'].str.split(' ').str[0], format="%Y-%m-%d")
        df = df[self.sales_data_cols]
        df = df.astype(self.sales_data_dtypes)
        df = df.rename(columns={
            "document": "invoice_no",
            "username": "sales_person"
        })
        return df
