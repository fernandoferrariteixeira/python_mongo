# -*- coding: utf-8 -*-
import json
import pymongo
from pymongo import MongoClient
import os
from datetime import datetime
from insert_data import insert_product_data
from insert_data import insert_stock_data

# Conecta no mongo
client = MongoClient('mongodb+srv://vitrio-dbuser-data:OYDcIP2yTlFIdWWa@cluster0-co0pe.mongodb.net/admin')

# define o database
db = client['stockdata']

# define o collection
col_info = db['productInfo']
col_info.create_index([("_id", pymongo.ASCENDING)])

# define o collection
col_stock = db['productStock']
col_stock.create_index([("_id", pymongo.ASCENDING)])

arquivos = []

# Procura os arquivos .JSON no diretorio e nas subpastas e guarda na variavel arquivos
for root, dirs, files in os.walk("C:\Users\Fernando Teixeira\Documents\Python\untitled\Teste", topdown=False):
    for name in files:
        arquivos.append(os.path.join(root, name))

# Executa para cada arquivo encontrado
for arquivo in arquivos:

    # arquiva a data de criação do arquivo
    date = (datetime.fromtimestamp(os.path.getctime(arquivo)).strftime('%Y-%m-%d'))

    # Importa os arquivos .JSON para a variavel data na forma de dict
    with open(arquivo) as json_file:
        data = json.load(json_file)

    # Executa para cada item dentro do arquivo .JSON
    for item in data:

        # Lista os atributos que serão salvos no productInfo
        lista = ["productName", "brand", "categoryId", "clusterHighlights", "searchableClusters", "categories",
                 "categoriesIds", "link"]
        productInfo = {}
        productInfo.update({"_id": (item["productId"])})

        # Lista os atributos que serão salvos no productStock

        lista2 = ["Price", "ListPrice", "PriceWithoutDiscount", "RewardValue", "AvailableQuantity"]
        productStock = {}
        productStock.update({"_id":(item["productId"])})
        productStock.update({"date": [date]})

        # Salva as informações listadas para a variavel productInfo
        for key in lista:
            productInfo.update({key: item[key]})

        # Salva as informações listadas para a variavel productStock
        for key in lista2:
            x = item["items"]
            x = x[0]
            x = x["sellers"]
            x = x[0]
            x = x["commertialOffer"]
            productStock.update({key: [x[key]]})

        # Chama a função que importa os dados para o MongoDB
        insert_product_data(productInfo, col_info)
        insert_stock_data(productStock, col_stock)
        print(datetime.now())