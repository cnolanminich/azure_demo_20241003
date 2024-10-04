import azure.functions as func
import logging
import random

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
    
@app.route(route="random_choice")
def random_choice(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Randomly choose between 1 and 0
    choice = random.choice([1, 0])

    return func.HttpResponse(f"{choice}", status_code=200)