"""
Demonstrates genuine MLflow experiment tracking + model registry - this talks to the real
MLflow server wired up in docker-compose.yml (MLFLOW_TRACKING_URI is already set in every
task's environment), not a reimplementation.

Run as a spark_submit task with: command: /jobs/train_model_example.py
"""
import os
import mlflow
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
mlflow.set_experiment("atlasflow-example")

X, y = make_classification(n_samples=500, n_features=8, random_state=42)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

with mlflow.start_run():
    params = {"C": 1.0, "max_iter": 200}
    mlflow.log_params(params)

    model = LogisticRegression(**params).fit(X_train, y_train)
    accuracy = accuracy_score(y_test, model.predict(X_test))
    mlflow.log_metric("accuracy", accuracy)

    # registers the model in MLflow's model registry, versioned automatically
    mlflow.sklearn.log_model(model, "model", registered_model_name="atlasflow-example-model")

    print(f"Logged run with accuracy={accuracy:.4f} - view it at {os.environ['MLFLOW_TRACKING_URI']}")
