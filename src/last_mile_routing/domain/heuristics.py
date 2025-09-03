import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist


def calcular_num_clusters_elbow(df, max_clusters=10):
    X = df[['destino_latitude', 'destino_longitude']].to_numpy()

    distortions = []
    K = range(1, min(max_clusters, len(X)) + 1)

    for k in K:
        kmeanModel = KMeans(n_clusters=k)
        kmeanModel.fit(X)
        distortions.append(
            sum(
                np.min(cdist(X, kmeanModel.cluster_centers_, 'euclidean'), axis=1)
            ) / X.shape[0]
        )

    deltas = np.diff(distortions)
    if len(deltas) == 0:
        return 1
    k_opt = np.argmin(deltas) + 1
    return k_opt


def sequenciar_ferradura(lista_pontos):
    """
    Ordena os pontos na sequência de ferradura:
    começa em uma extremidade, percorre até o outro extremo e retorna pelo caminho oposto.
    """
    lista_pontos = sorted(lista_pontos, key=lambda x: (x[0], x[1]))  # ordena por latitude
    metade = len(lista_pontos) // 2
    ida = lista_pontos[:metade]
    volta = lista_pontos[metade:]
    volta.reverse()
    return ida + volta
