import typing

    @property
    def sql_create_table(self):
        # We're using the following .md files to build our apps table: https://github.com/near/ecosystem/tree/main/entities.
        return '''
            CREATE TABLE IF NOT EXISTS apps
            (
                slug
                , title
                , oneliner
                , website
                , category
                , status
                , contract
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS apps
        '''
