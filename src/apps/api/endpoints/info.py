from flask_restful import Resource
from . import subendpoints_help


class Info(Resource):
    """Returns subendpoints"""
    def get(self):
        return subendpoints_help([
            "credits/raw",
            "credits/agg",
            "credits/agg/daily",
            "credits/agg/monthly",
            "credits/agg/countries",
            "credits/agg/projects",
            "credits/agg/methodologies",
            "credits/agg/vintage",
            "pools/raw",
            "pools/agg",
            "pools/agg/daily",
            "pools/agg/monthly",
            "holders",
            "prices",
            "carbon_metrics/polygon",
            "carbon_metrics/eth",
            "carbon_metrics/celo",
            "retirements/all/raw",
            "retirements/all/agg",
            "retirements/all/agg/daily",
            "retirements/all/agg/monthly",
            "retirements/all/agg/beneficiaries",
            "retirements/klima/raw",
            "retirements/klima/agg",
            "retirements/klima/agg/daily",
            "retirements/klima/agg/monthly",
            "retirements/klima/agg/beneficiaries",
            "retirements/klima/agg/tokens",
            "retirements/klima/agg/tokens/daily",
            "retirements/klima/agg/tokens/monthly"
        ])
