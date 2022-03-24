import pandas as pd
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
            'population': float
        }
        self.inventory_cols = [
            "sku", "descr", "category", "magento_inv", "last_cost", "unit_price", "CATEGORIES"
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


    def load_customers(self):
        df = pd.read_csv(str(config.CUSTOMER_PATH), dtype=str, sep='|')
        df_geo = self._load_customers_geo()
        df = pd.merge(df, df_geo, left_on='id', right_on='No', how='left')
        df = df[self.customer_cols]
        df.columns = map(str.lower, df.columns)
        df = df.astype(self.customer_dtypes)
        df.is_b2c = df.is_b2c.apply(lambda x: True if x == 't' else False)
        return df


    def _load_customers_geo(self):
        df = pd.read_csv(str(config.CUSTOMER_GEO_PATH), dtype=str, sep='|')
        return df


    def load_inventory(self):
        df = pd.read_csv(str(config.INVENTORY_PATH), dtype=str, sep='|')
        df_cat = self._load_categories()
        df = pd.merge(df, df_cat, left_on='sku', right_on='Artikel', how='left')
        df = df[self.inventory_cols]
        df = df.astype(self.inventory_dtypes)
        df = df.rename(columns={
            'magento_inv': 'quantity',
            'category': 'brand',
            'CATEGORIES': 'category'
        })
        return df


    def _load_categories(self):
        df = pd.read_excel(str(config.CATEGORIES_PATH), dtype=str)
        return df


    def load_sales_data(self):
        df = pd.read_csv(str(config.SALES_DATA_PATH), sep='|', dtype=str)
        df = df[self.sales_data_cols]
        df = df.astype(self.sales_data_dtypes)
        df = df.rename(columns={
            "document": "invoice_no",
            "username": "sales_person"
        })
        df.ddate = pd.to_datetime(df.ddate, format="%Y-%m-%d")
        return df

