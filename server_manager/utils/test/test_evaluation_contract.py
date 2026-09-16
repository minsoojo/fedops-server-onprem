"""Exercise production Pydantic contracts/handler without starting MongoDB/K8s."""
import ast
from pathlib import Path
from typing import Any, Dict, Literal, Optional
import unittest
from unittest.mock import MagicMock

from pydantic import BaseModel, Field, validator


ROOT = Path(__file__).resolve().parents[3]


def definitions(path, names, namespace):
    tree = ast.parse(path.read_text())
    selected = ast.Module(body=[node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))
                               and node.name in names], type_ignores=[])
    exec(compile(selected, str(path), 'exec'), namespace)
    return namespace


class EvaluationContractTests(unittest.TestCase):
    def test_campaign_preserves_and_validates_evaluation_settings(self):
        scope = dict(BaseModel=BaseModel, Field=Field, validator=validator, Optional=Optional,
                     Dict=Dict, Any=Any, Literal=Literal)
        definitions(ROOT / 'server_manager/app.py', {'CampaignStrategy', 'ServerEvaluationConfig', 'CampaignConfig'}, scope)
        Campaign = scope['CampaignConfig']
        base = dict(rounds=2, clientsPerRound=2, strategy={'name': 'FedAvg'})
        self.assertIsNone(Campaign(**base).serverEvaluation)
        self.assertFalse(Campaign(**base, serverEvaluation={'enabled': False}).serverEvaluation.enabled)
        self.assertEqual(Campaign(**base, serverEvaluation={'enabled': True, 'dataPath': 'ecg'}).serverEvaluation.dataPath, 'ecg')
        for value in ({'enabled': 'false'}, {'enabled': True}, {'enabled': True, 'dataPath': '../secret'}):
            with self.assertRaises(ValueError):
                Campaign(**base, serverEvaluation=value)

    def test_performance_handler_preserves_nulls_source_and_campaign(self):
        scope = dict(BaseModel=BaseModel, Field=Field, Optional=Optional, app=MagicMock(), logging=MagicMock(), db=MagicMock())
        scope['app'].put = lambda *args, **kwargs: lambda fn: fn
        definitions(ROOT / 'fl_performance/app.py', {'GLModelEvaluation', 'gl_model_evaluation_put'}, scope)
        Evaluation = scope['GLModelEvaluation']
        scope['gl_model_evaluation'] = Evaluation()
        entry = Evaluation(round=2, gl_model_v=3, campaign_run_id='run', evaluation_source='client_aggregated',
                           evaluation_status='not_evaluated', evaluation_samples=0, evaluation_clients=0)
        scope['gl_model_evaluation_put']('task', entry)
        args = scope['db'].__getitem__.return_value.update_one.call_args
        self.assertEqual(args.args[0], dict(fl_task_id='task', campaign_run_id='run', round=2, gl_model_v=3))
        saved = args.args[1]['$set']
        self.assertIsNone(saved['gl_loss'])
        self.assertIsNone(saved['gl_accuracy'])
        self.assertEqual(saved['evaluation_source'], 'client_aggregated')
        self.assertEqual(saved['evaluation_samples'], 0)


if __name__ == '__main__':
    unittest.main()
