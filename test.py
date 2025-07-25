columns= [
                'artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
                'length', 'level', 'location', 'method', 'page', 'registration',
                'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'
            ]

lower_case_columns = [col.lower() for col in columns]

x = ','.join(lower_case_columns)
print(x)