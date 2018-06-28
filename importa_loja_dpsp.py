# -*- coding: utf-8 -*-
import json
import pymongo
from pymongo import MongoClient
import os
from insert_data import insert_product_data
from insert_data import insert_stock_data

# Conecta no mongo
client = MongoClient('mongodb+srv://vitrio-dbuser-data:6cskLKrfeL2jjyp6@cluster0-sovq0.mongodb.net/admin')

# define o database
db = client['stockdata']

# define o collection
col_info = db['productInfo']
col_info.create_index([("productId",pymongo.ASCENDING)])

# define o collection
col_stock = db['productStock']
col_stock.create_index([("productId",pymongo.ASCENDING), ("date",pymongo.ASCENDING), ("productName",pymongo.ASCENDING)])

# Cria e popula a lista arquivos com os arquivos .JSON no diretorio e nas subpastas
arquivos = []
for root, dirs, files in os.walk("C:\Users\Fernando Teixeira\Documents\Python\untitled\JSON\dpsp", topdown=False):
    for name in files:
        arquivos.append(os.path.join(root, name))

# Executa a limpeza e o update para cada arquivo encontrado
for arquivo in arquivos:

    # arquiva a data de criação do arquivo utilizando o nome do arquivo
    date = os.path.join(arquivo[-15:-5])

    # Tenta importar os arquivos .JSON para a variavel data, caso não consiga imprime o erro no arquivo "erros.txt"
    try:
        with open(arquivo) as json_file:
            data = json.load(json_file)
    except Exception as e:
        erro = open("erros.txt", "a")
        mensagem_erro = str(datetime.now()) + " " + (os.path.join(arquivo)) + ("{}\n".format(e))
        erro.write(mensagem_erro)
        erro.close()
        continue

    # Executa para cada item dentro do arquivo .JSON
    for item in data:

        # Lista os atributos que serão salvos no productInfo
        lista = ["productName", "brand", "categoryId", "clusterHighlights", "searchableClusters", "categories",
                 "categoriesIds", "link"]

        # Dicionario para armazenar os dados do prodctinfo
        productInfo = {}

        # Lista os atributos que serão salvos no productStock
        lista2 = ["Price", "ListPrice", "PriceWithoutDiscount", "RewardValue", "AvailableQuantity"]

        # Salva as informações listadas para a variavel productStock
        x = item["items"]

        # Tenta fazer o update dos dicionarios productInfo e productStock e chamar a função insert_stock_data
        try:
            productInfo.update({"productId": (item["productId"])})

            # Salva as informações listadas para a variavel productInfo
            for key in lista:
                productInfo.update({key: item[key]})

            # Salva as informações listadas para a variavel productStock e faz o update
            for produto in x:
                productStock = {}
                productStock.update({"productId": item["productId"]})
                productStock.update({"date": date})
                productStock.update({"nameComplete": produto["nameComplete"]})
                z = produto["sellers"]
                z = z[0]
                z = z["commertialOffer"]
                for key in lista2:
                    productStock.update({key: z[key]})
                # Chama a função que importa os dados para o MongoDB
                insert_stock_data(productStock, col_stock)

            # Chama a função que importa os dados para o MongoDB
            insert_product_data(productInfo, col_info)

        # Caso algum erro ocorra imprime o erro no arquivo "erros.txt"
        except Exception as e:
            erro = open("erros.txt", "a")
            mensagem_erro = str(datetime.now()) + " " + (os.path.join(arquivo)) + (" {} não encontrado\n".format(e))
            erro.write(mensagem_erro)
            erro.close()
            continue

    print (os.path.join(arquivo))
    # Apaga o arquivo inserido
    os.remove(os.path.join(arquivo))