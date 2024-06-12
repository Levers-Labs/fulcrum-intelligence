"""
This file is used to deploy the FastAPI app as a Lambda function using AWS API Gateway.
"""

from mangum import Mangum

from story_manager.main import app

handler = Mangum(app)
