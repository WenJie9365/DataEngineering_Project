from kafka import KafkaAdminClient, KafkaConsumer,TopicPartition
import sys
import json
import pandas as pd
from json import loads

### Setting up the Python consumer
consumer = KafkaConsumer (
    bootstrap_servers = ['localhost:9092'],
    auto_offset_reset = 'earliest',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
                         )  
username = []
rating = []
brand = []
model = []
variation = []
description = []
delivery_service = []
product_quality = []
seller_service = []
comment = []

consumer.assign([TopicPartition('shopee', 0)])
i = 1
for message in consumer:
        print (message.value)

        content = json.dumps(message.value['username'])
        username.append(content)
        
        content = json.dumps(message.value['rating'])
        rating.append(content)
        
        content = json.dumps(message.value['brand'])
        brand.append(content)
        
        content = json.dumps(message.value['model'])
        model.append(content)
        
        content = json.dumps(message.value['variation'])
        variation.append(content)
        
        content = json.dumps(message.value['description'])
        description.append(content)
        
        content = json.dumps(message.value['delivery_service'])
        delivery_service.append(content)
        
        content = json.dumps(message.value['product_quality'])
        product_quality.append(content)

        content = json.dumps(message.value['seller_service'])
        seller_service.append(content)
        
        content = json.dumps(message.value['comment'])
        comment.append(content)

        i+=1
        if i == 3500:
            break

dict = {'username': username, 
        'rating': rating, 
        'brand': brand, 
        'model': model, 
        'variation': variation, 
        'description': description, 
        'delivery_service': delivery_service, 
        'product_quality': product_quality, 
        'seller_service': seller_service, 
        'comment': comment
       }

df = pd.DataFrame(dict)
df.to_csv('reviewData.csv')

