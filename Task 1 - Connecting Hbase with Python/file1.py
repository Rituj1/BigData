import happybase

#establishing a connection to HBase
try:
    connection = happybase.Connection('192.168.56.101')
    print('Connection established')
except:
    print('Connection failed')


#opening a connection
#connection.open('employee')

#to list the available tables
#print(connection.tables())

#creating a new table
connection.create_table('emp', 
{'pers_data': dict(), 
'prof_data': dict()})

#table instance to work with
table = connection.table('emp')

#storing data into table
table.put(b'emp1',
 {b'pers_data:name': b'Kim',
 b'pers_data:city': b'New York', 
 b'prof_data:designation': b'D.E.', 
 b'prof_data:salary': b'50000'})

 #To scan the table
for key, data in table.scan():
    print(key, data)
