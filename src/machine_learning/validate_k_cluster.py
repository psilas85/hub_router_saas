# hub_router_1.0.1/src/machine_learning/validate_k_cluster.py

import os
import pandas as pd
import numpy as np
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    log_loss
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from machine_learning.k_cluster.infrastructure.repositories import PgResultsRepository

# ============================================================
# Configurações
# ============================================================

TENANT_ID = os.getenv("TENANT_ID", "38ed0dcc-a92b-4c07-85e7-b59e5939b84c")
START_DATE = "2024-01-01"
END_DATE = "2025-09-01"

# ============================================================
# Carregar dados
# ============================================================

repo = PgResultsRepository()
df = repo.load_resultados_simulacao(TENANT_ID, start=START_DATE, end=END_DATE)

if df.empty:
    raise RuntimeError("⚠️ Nenhum dado encontrado no simulation_db!")

print(f"✅ Dataset carregado: {df.shape[0]} linhas, {df.shape[1]} colunas")

# Features e target
X = df.drop(columns=["tenant_id", "envio_data", "k_clusters"])
y = df["k_clusters"]

# Split treino/teste
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ============================================================
# Treino
# ============================================================

model = XGBClassifier(
    n_estimators=200,
    learning_rate=0.1,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    eval_metric="mlogloss",
    use_label_encoder=False
)

model.fit(X_train, y_train)

print("✅ Modelo treinado com sucesso!")

# ============================================================
# Avaliação
# ============================================================

y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)

print("\n📊 Classification Report:")
print(classification_report(y_test, y_pred, digits=3))

print("\n📊 Matriz de confusão:")
print(confusion_matrix(y_test, y_pred))

print("\n📊 Log Loss:")
print(log_loss(y_test, y_proba))

# Distribuição da confiança
confidences = np.max(y_proba, axis=1)
print("\n📊 Distribuição de confiança (máx prob por predição):")
print(pd.Series(confidences).describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]))

# Importância das features
importances = model.feature_importances_
feat_imp = sorted(zip(X.columns, importances), key=lambda x: x[1], reverse=True)

print("\n📊 Top 10 features mais importantes:")
for feat, score in feat_imp[:10]:
    print(f" - {feat}: {score:.4f}")

print("\n🎯 Validação concluída.")
