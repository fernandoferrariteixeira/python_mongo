# -*- coding: utf-8 -*-

def insert_product_data(file,col):

    # grava o productId na variavel Id
    Id = (file["_id"])

    # verifica se o arquivo ja foi adicionado ou se nao existe
    if None == (col.find_one({"_id": Id})):
        # insere o arquivo
        col.insert_one(file)

    else:
        # deleta o arquivo antigo e insere o novo
        col.delete_one({"_id": Id})
        col.insert_one(file)

def insert_stock_data(file, col):

    # grava o productId na variavel Id
    Id = (file["_id"])

    # verifica se o arquivo ja foi adicionado ou se nao existe
    if None == (col.find_one({"_id": Id})):
        # insere o arquivo
        col.insert_one(file)

    # verifica a ultima data inserida
    elif not (col.find_one({"date": {"$in": [file["date"]]}})):
        # atualiza arquivo antigo
        col.update_many({"_id": Id}, {"$push": {"Price" : file["Price"][0]}})
        col.update_many({"_id": Id}, {"$push": {"ListPrice": file["ListPrice"][0]}})
        col.update_many({"_id": Id}, {"$push": {"PriceWithoutDiscount": file["PriceWithoutDiscount"][0]}})
        col.update_many({"_id": Id}, {"$push": {"RewardValue": file["RewardValue"][0]}})
        col.update_many({"_id": Id}, {"$push": {"AvailableQuantity": file["AvailableQuantity"][0]}})
        col.update_many({"_id": Id}, {"$push": {"date": file["date"][0]}})