# Restful-web-service
This a Restful web data service of Amazon item information and reviews. The original data are in json format. data_loader.py tranformed  and store the data into an Amazon Postgres database.

server.py built web data service functions. Function modules include statistics, ranked items and item search. client_test.py tests the functions run on Amazon EC2. Server IP: 52.10.103.151:5000
