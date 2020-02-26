import pandas as pd
import numpy as np
import sqlalchemy
from sklearn import feature_extraction, decomposition, cluster

ETSY_DB = "sqlite:///../data/etsy.sqlite"

def read_etsydb(ETSY_DB):
    """
    read etsy database
    :param ETSY_DB: etsy.sqlite file location
    :return: pandas dataframe
    """
    etsy_db = sqlalchemy.create_engine(ETSY_DB)
    cards = pd.read_sql_table("cards",etsy_db)
    return cards

if __name__=="__main__":

    ## Read from db and make tfidf transform of title
    cards = read_etsydb(ETSY_DB)
    tfidf = feature_extraction.text.TfidfVectorizer()
    tdmat = tfidf.fit_transform(cards['title'])

    ## make PCA features from title
    n_pca = 3
    pca = decomposition.TruncatedSVD(n_components=n_pca)
    pca_mat = pca.fit_transform(tdmat)
    for i in range(n_pca):
        cards[f"title_pca_{i}"] = pca_mat[:,i]

    ## make k-means clustering
    n_clust = 15
    clust = cluster.KMeans(n_clusters=n_clust)
    title_clust = clust.fit_predict(tdmat)
    cards[f"title_clust"] = title_clust

    cards.to_sql("cards",etsy_db,if_exists="replace")