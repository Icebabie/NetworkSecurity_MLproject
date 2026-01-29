import sys
from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig

if __name__ == '__main__':
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        data_ingestion_config=DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info('Intiate the data ingestion')
        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        logging.info('data ingestion is completed')
        print(dataingestionartifact)

    except Exception as e:
        raise CustomException(e,sys)