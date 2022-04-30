from modules.loader import Loader

from surprise import Dataset
from surprise import Reader


class Pipeline:
    PRODUCT_CUMSUM_CUT = 0.9
    CUSTOMER_CUMSUM_CUT = 0.9

    def __init__(self):
        self.loader = Loader()
        self.df_cust = self.loader.load_customers()
        self.df_prod = self.loader.load_inventory()
        self.df_sals = self.loader.load_sales_data()
        self.df_sals = self.loader.add_nls(self.df_sals, self.df_prod)
        self._filter()
    
    def _filter(self):
        customers = self._filter_customers()
        products = self._filter_products()
        self.df_cust = self.df_cust.loc[self.df_cust['id'].isin(customers)]
        self.df_prod = self.df_prod.loc[self.df_prod['sku'].isin(products)]
        self.df_sals = self.df_sals.loc[self.df_sals['sku'].isin(products) & 
                                        self.df_sals['cust_id'].isin(customers)]
    
    def _filter_customers(self):
        df = self.df_sals[(self.df_sals.NLS == False) & (self.df_sals.price > 0)][['cust_id', 'price']] \
            .groupby('cust_id').sum(['price']).sort_values(by='price', ascending=False)
        df['sales_percentage'] = df.price / df.price.sum()
        df = df.sort_values(by='sales_percentage', ascending=False).cumsum()
        customers = list(df.loc[df['sales_percentage'] < Pipeline.CUSTOMER_CUMSUM_CUT].index)
        return customers

    def _filter_products(self):
        df = self.df_sals[(self.df_sals.NLS == False) & (self.df_sals.price > 0)][['sku', 'price']] \
            .groupby('sku').sum(['price']).sort_values(by='price', ascending=False)
        df['sales_percentage'] = df.price / df.price.sum()
        df = df.sort_values(by='sales_percentage', ascending=False).cumsum()
        products = list(df.loc[df['sales_percentage'] < Pipeline.PRODUCT_CUMSUM_CUT].index)
        return products

    def get_matrix(self, metric):
        # apply ranking here (sum for simplicity)
        if metric == 'quantity':
            df = self.df_sals[['sku', 'cust_id', 'quantity']].groupby(['sku', 'cust_id']).sum().reset_index()
            reader = Reader(rating_scale=(0, df.max()['quantity']))
        elif metric == 'binary':
            df = self.df_sals[['sku', 'cust_id', 'quantity']].groupby(['sku', 'cust_id']).sum().apply(lambda x: x > 0).reset_index()
            reader = Reader(rating_scale=(0, 1))
        data = Dataset.load_from_df(df, reader)
        return data