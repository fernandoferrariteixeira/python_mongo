# -*- coding: utf-8 -*-

def insert_product_data(file,col):

    # grava o productId na variavel Id
    Id = (file["productId"])

    # verifica se o arquivo ja foi adicionado ou se nao existe
    if None == (col.find_one({"productId": Id})):
        # insere o arquivo
        col.insert_one(file)

    else:
        # deleta o arquivo antigo e insere o novo
        col.delete_one({"productId": Id})
        col.insert_one(file)

def insert_stock_data(file, col):
    from datetime import datetime
    # grava o productId na variavel Id
    Id = file["productId"]
    nameComplete = file["nameComplete"]
    time = file["date"]

    #verifica se o arquivo ja foi adicionado ou se nao existe
    if None == (col.find_one({"productId": Id, "nameComplete": nameComplete, "date": time})):
        col.insert_one(file)
        print datetime.now()
    else:
        print "skip"


