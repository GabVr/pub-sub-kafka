from PIL import Image, ImageDraw
from confluent_kafka import Consumer, KafkaError
import os
from time import sleep

OUT_FOLDER = '/processed/text/'
NEW = '_text'
IN_FOLDER = "/appdata/static/uploads/"


def criarText(path_file):

    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    original_image = Image.open(path_file)
    transposed = original_image.transpose(Image.Transpose.ROTATE_180)

    name, ext = os.path.splitext(filename)
    transposed.save(output_folder + name + NEW + ext)
    
    img = Image.open("imagem.jpg")
    draw = ImageDraw.Draw(img)
    draw.text((0, 0), "Olá, Mundo!", fill='white', font_size=35)
    img.save("nova_imagem.jpg")


c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'text-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['image'])