from airflow.models.param import Param
from airflow.decorators import dag, task

@dag(
    params = {
        "param1": Param("default param1", description="ini adalah deskripsi param1"),
        "param2": Param("default param2", description="ini adalah deskripsi param2"),
    }
)
def run_config_parameter():
    @task
    def get_param_with_current_context(**kwargs):
        print(kwargs['params'])
        print(kwargs['params']['param1'], type(kwargs['params']['param1']))
        print(kwargs['params']['param2'], type(kwargs['params']['param2']))

    @task
    def get_param_with_template_reference(param1, param2, params):
        print(params)
        print(param1, type(param1))
        print(param2, type(param2))

    get_param_with_current_context() >> get_param_with_template_reference(
        params = "{{ params }}",
        param1 = "{{ params['param1'] }}",
        param2 = "{{ params.param2 }}"
    )

run_config_parameter()


