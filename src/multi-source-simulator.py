import pika
import json
import datetime
import random
import time

credentials = pika.PlainCredentials('user', 'password')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
)
channel = connection.channel()
channel.queue_declare(queue='log_queue', durable=True)

def send_to_rmq(data):
    channel.basic_publish(
        exchange='',
        routing_key='log_queue',
        body=json.dumps(data),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f" [x] Sent ({data.get('source_type')}): {data}")

def generate_python_log():
    return {
        "source_type": "python", # Penanda saja
        "timestamp": datetime.datetime.now().isoformat(),
        "service": "fastapi-backend",
        "level": random.choice(["INFO", "ERROR"]),
        "endpoint": "/api/v1/user",
        "message": "User request processed"
    }

def generate_golang_log():
    return {
        "source_type": "golang",
        "ts": time.time(),
        "app_name": "go-payment-service",
        "severity": random.choice(["info", "fatal"]), 
        "msg": "Payment gateway timeout connection"
    }

def generate_ruby_log():
    return {
        "source_type": "ruby",
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "component": "rails-frontend",
        "log_level": random.choice(["INFO", "CRITICAL"]),
        "details": "Render template error in dashboard"
    }

try:
    print("Mulai mengirim log acak dari berbagai bahasa...")
    while True:
    
        choice = random.choice(['python', 'golang', 'ruby'])
        
        if choice == 'python':
            log = generate_python_log()
        elif choice == 'golang':
            log = generate_golang_log()
        else:
            log = generate_ruby_log()
            
        send_to_rmq(log)
        time.sleep(1) 
except KeyboardInterrupt:
    print("Berhenti.")
    connection.close()