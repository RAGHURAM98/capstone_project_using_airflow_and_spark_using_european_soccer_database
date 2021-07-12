class DataQualitySqlQueries:
    
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM League", 'expected_result': 11, 'table':'League' },
        {'check_sql': "SELECT COUNT(*) FROM League Where id is  NULL", 'expected_result': 0, 'table':'League' },
        {'check_sql': "SELECT COUNT(*) FROM Country", 'expected_result': 11, 'table':'Country' },
        {'check_sql': "SELECT COUNT(*) FROM Country Where id is NULL", 'expected_result': 0, 'table':'Country' },
        {'check_sql': "SELECT COUNT(*) FROM Player_Attributes", 'expected_result': 183978 , 'table':'Player_Attributes'}
    ]
    
    