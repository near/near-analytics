import pandas as pd
import numpy as np
from ..sql_aggregations import SqlAggregations


class NearEcosystemEntities(SqlAggregations):
    @property
    def sql_create_table(self):
        return '''
            CREATE TABLE IF NOT EXISTS near_ecosystem_entities
            (
                slug        TEXT PRIMARY KEY,
                title       TEXT,
                oneliner    TEXT,      
                website     TEXT, 
                category    TEXT,    
                status      TEXT, 
                contract    TEXT,
                logo        TEXT,
                is_app      BOOLEAN,
                is_nft      BOOLEAN,
                is_guild    BOOLEAN,
                is_defi     BOOLEAN,
                is_dao      BOOLEAN  
                                                
            )
        '''


    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS near_ecosystem_entities
        '''
    @property
    def sql_select(self):
        pass        

    @property
    def sql_insert(self):
        return '''
            INSERT INTO near_ecosystem_entities VALUES %s
            ON CONFLICT DO NOTHING;
            UPDATE near_ecosystem_entities SET slug = NULL WHERE slug = 'NaN'
            UPDATE near_ecosystem_entities SET title  = NULL WHERE  title  = 'NaN'
            UPDATE near_ecosystem_entities SET oneliner = NULL WHERE oneliner = 'NaN'
            UPDATE near_ecosystem_entities SET website  = NULL WHERE website  = 'NaN'
            UPDATE near_ecosystem_entities SET category = NULL WHERE category = 'NaN'
            UPDATE near_ecosystem_entities SET status = NULL WHERE status  = 'NaN'
            UPDATE near_ecosystem_entities SET contract = NULL WHERE contract = 'NaN'
            UPDATE near_ecosystem_entities SET logo = NULL WHERE logo = 'NaN'
        '''  

    def collect(self, requested_timestamp: int):
        url = "https://raw.githubusercontent.com/near/ecosystem/main/entities.json"
        df = pd.read_json(url)
        df1 = df[['slug', 'title', 'oneliner', 'website', 'category', 'status', 'contract', 'logo']]

        app = df1['category'].str.contains('app')
        nft = df1['category'].str.contains('nft')
        guild = df1['category'].str.contains('guild')
        defi = df1['category'].str.contains('defi')
        dao = df1['category'].str.contains('dao')

        merged_df1 = df1.join(app, rsuffix = ('_app'))
        merged_df2 = merged_df1.join(nft, rsuffix = ('_nft'))
        merged_df3 = merged_df2.join(guild, rsuffix = ('_guild'))
        merged_df4 = merged_df3.join(guild, rsuffix = ('_defi'))
        merged_df5 = merged_df4.join(guild, rsuffix = ('_dao'))
        
        nan = merged_df5.fillna("")
        output_values = nan.values
        return output_values.tolist()