from send_msg import send
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaError
from time import sleep
import ssl
import json
import smtplib

remetente = "gabrielverissimo735@gmail.com"
destinatario = "your@gmail.com" #É necessário botar o email do destinatário
senha_app = "" #Aqui vai ficar a senha do app, a senha do app vai ficar relacionada ao email utilizado, nesse caso, eu apenas coloquei o meu de exemplo para testes

def envioEmail(remetente, destinatario, senha_app, dados):
   msg = EmailMessage()
   msg["to"] = destinatario
   msg["from"] = remetente
   msg["subject"] = "Chegou o email"
   msg.set_content("Este é uma mensagem de verificação")

    
   msg.set_content(
        f"Este é um e-mail de verificação.\n\nDados recebidos: {dados}"
    )

   contexto = ssl.create_default_context()

   try:
       with smtplib.SMTP('smtp.gmail.com', 587) as servidor_email:
        servidor_email.starttls(context=contexto)
        servidor_email.login(remetente, senha_app)
        servidor_email.send_message(msg)
        print('Email enviado com sucesso!')
   except smtplib.SMTPAuthenticationError:
        print('Falha na autenticação: confira o email e a senha de app.')
   except smtplib.SMTPException as e:
        print(f"Erro do servidor SMTP: {e}")
   except OSError as e:
        print(f"Erro de conexão: {e}")


c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificacao-group',
    'client.id': 'notificacao-client',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notificacao'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        elif not msg.error():
            dados = json.loads(msg.value())
            print(f"Recebido aviso do Kafka: {dados}")
            envioEmail(remetente, destinatario, senha_app, dados)
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            pass 
        else:
            print(f"Erro no Kafka: {msg.error().str()}")

except KeyboardInterrupt:
    pass
finally:
    c.close()