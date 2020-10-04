from sklearn.pipeline import Pipeline

from datahandler import DataHandler
from repository import MongoRepository
from service import FinancialStatementModellingService
from transformer import DropColumnsTransformer


if __name__ == '__main__':
    # Todo: Construct dependencies somewhere else (factory?) and inject into service

    # Repository Integration
    repository = MongoRepository("nonprod")

    # Data Handler
    datahandler = DataHandler(repository)

    # Full Transformation Pipeline
    transform_pipeline = Pipeline([
        ('drop_columns_transformer', DropColumnsTransformer(["link", "finalLink"]))
    ])

    # Create service instance w/ injected dependencies & run
    service = FinancialStatementModellingService(datahandler, transform_pipeline)

    df = service.run_service()















