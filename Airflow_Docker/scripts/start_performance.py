from airflow.api.client.local_client import Client

c = Client(None, None)
c.trigger_dag(dag_id='Stochastic_Gradient_Descent_v4', run_id='Stochastic_Gradient_Descent_v4', conf={})