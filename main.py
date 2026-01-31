import sys
from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig,DataValidationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig

if __name__ == '__main__':
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        data_ingestion_config=DataIngestionConfig(trainingpipelineconfig)
        data_validation_config = DataValidationConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info('Intiate the data ingestion')
        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        logging.info('data ingestion is completed')
        print(dataingestionartifact)
        data_validation=DataValidation(dataingestionartifact,data_validation_config)
        logging.info("Initiate Data Validation")
        datavalidationartifact=data_validation.initiate_data_validation()
        logging.info("Data Validation is Completed.")
        print(datavalidationartifact)

    except Exception as e:
        logging.exception("An exception occurred in main")
        raise CustomException(e,sys)