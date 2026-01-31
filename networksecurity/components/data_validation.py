from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import os,sys

class DataValidation:
    def __init__(self, data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise CustomException(e,sys)
        

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e,sys)
        


    def validate_no_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            no_of_columns = len(self.schema_config["columns"])  # number of columns defined in schema
            logging.info(f"The dataframe has {len(dataframe.columns)} number of columns")
            logging.info(f"Required number of columns is {no_of_columns}")
            if len(dataframe.columns) == no_of_columns:
                return True
            else:
                return False

        except Exception as e:
            raise CustomException(e,sys)
        

    def validate_numerical_columns(self,dataframe:pd.DataFrame) -> bool:
        try:
            schema_columns = {}
            for item in self.schema_config["columns"]:
                schema_columns.update(item)

            # Expected numerical columns from schema
            schema_numerical_cols = set(self.schema_config["numerical_columns"])
            # Coluns for the dataframe
            numeric_cols_df = set(dataframe.select_dtypes(include='number').columns.to_list())

            if schema_numerical_cols == numeric_cols_df:
                return True
            else:
                return False
        except Exception as e:
            raise CustomException(e,sys)
        
    

    def detect_dataset_drift(self,base_df,current_df,threshold=0.05) -> bool:
        try:
            status = True
            report = {}
            logging.info('Detecting data drift between base and current datasets')
            for column in base_df.columns:
                d1 = base_df[column].dropna()
                d2 = current_df[column].dropna()
                samp_dist = ks_2samp(d1, d2)
                # if p-value is less than threshold -> drift detected
                if float(samp_dist.pvalue) < float(threshold):
                    is_found = True
                    status = False
                else:
                    is_found = False
                report.update({column: {
                    "p_value": float(samp_dist.pvalue),
                    "drift_status": is_found
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            # create a directory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)
            logging.info(f"Drift report saved at: {drift_report_file_path}")
            return status

        except Exception as e:
            raise CustomException(e,sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            logging.info(f"Starting data validation for train: {train_file_path} and test: {test_file_path}")

            # read the data from test and train file paths
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe  = DataValidation.read_data(test_file_path)

            # validate number of columns
            status = self.validate_no_of_columns(dataframe=train_dataframe)
            if not status:
                error_message = f"Train dataset does not contain all features!\n"

            status = self.validate_no_of_columns(dataframe=test_dataframe)
            if not status:
                error_message = f"Test dataset does not contain all the features!\n"


            # check if numerical columns exist
            cols_status = self.validate_numerical_columns(dataframe=train_dataframe)
            if not cols_status:
                error_message = f'{error_message} Train dataframe doesnot contain all numeric columns.\n'
            
            cols_status = self.validate_numerical_columns(dataframe=test_dataframe)
            if not cols_status:
                error_message = f'{error_message} Test dataframe doesnot contain all numeric columns.\n'

            # checking the datadrift
            status = self.detect_dataset_drift(base_df=train_dataframe,current_df=test_dataframe)
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)

            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path,index=False ,header=True
            )

            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path,index=False ,header=True
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            return data_validation_artifact
        
        except Exception as e:
            raise CustomException(e,sys)