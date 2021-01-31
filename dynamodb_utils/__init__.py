import boto3
import json
import logging
import re

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, AnyStr, Dict, List, Tuple


@dataclass(init=False)
class DynamoDBUtils:

    logger: logging.Logger

    def __init__(self, logger: logging.Logger = logging.getLogger()):

        self.logger = logger
        self.client = boto3.resource('dynamodb')

    def save_many(self, batch: List[Dict[AnyStr, Any]], table_name: AnyStr) -> None:
        """Saves a batch of objects into a table

        :param batch:       Batch of objects to save
        :param table_name:  Name of the table
        """

        with self.client.Table(table_name).batch_writer() as batch_writer:
            for object_ in batch:
                batch_writer.put_item(Item=object_)

    def retrieve_many(self,
                      table_name: AnyStr,
                      max_batch_size: int = 100,
                      use_decimal: bool = False) -> List[Dict[AnyStr, Any]]:
        """Retrieves a batch of objects from a table

        :param table_name:      Name of the table
        :param max_batch_size:  Max number of objects to retrieve
        :param use_decimal:     If False Decimal values are cast into int or float
        :return:                The retrieved batch
        """

        batch = [
            item for item in self.client.Table(table_name).scan(Select='ALL_ATTRIBUTES', Limit=max_batch_size)['Items']
        ]

        if not use_decimal:
            batch = [self.__replace_decimals(object_=item) for item in batch]

        return batch

    def load_object(self, dynamodb_object: Dict[AnyStr, Any]) -> Dict[AnyStr, Any]:
        """Loads a DynamoDB JSON object into a dictionary

        :param dynamodb_object: DynamoDB JSON object
        :return:                The object as a dictionary
        """

        return json.loads(json.dumps(dynamodb_object), object_hook=self.__object_hook)

    @staticmethod
    def __replace_decimals(object_: Any) -> Any:

        if isinstance(object_, list):
            for i in range(len(object_)):
                object_[i] = DynamoDBUtils.__replace_decimals(object_[i])
            return object_
        elif isinstance(object_, dict):
            for k in object_.keys():
                object_[k] = DynamoDBUtils.__replace_decimals(object_[k])
            return object_
        elif isinstance(object_, Decimal):
            if object_ % 1 == 0:
                return int(object_)
            else:
                return float(object_)
        else:
            return object_

    @staticmethod
    def __object_hook(dict_: Dict[AnyStr, Any]) -> Any:

        is_dynamodb_object, value = DynamoDBUtils.__parse_dynamodb_object(dict_=dict_)
        if is_dynamodb_object:
            return value
        else:
            for key, value in dict_.items():
                if isinstance(value, str):
                    try:
                        dict_[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f')
                    except ValueError:
                        pass

                if isinstance(value, Decimal):
                    if value % 1 > 0:
                        dict_[key] = float(value)
                    else:
                        dict_[key] = int(value)

            return dict_

    @staticmethod
    def __parse_dynamodb_object(dict_: Dict[AnyStr, Any]) -> Tuple[bool, Any]:

        is_dynamodb_object = True
        value = None

        if 'BOOL' in dict_:
            value = dict_['BOOL']
        elif 'S' in dict_:
            value = dict_['S']
            try:
                value = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                value = str(value)
        elif 'SS' in dict_:
            value = list(dict_['SS'])
        elif 'N' in dict_:
            value = dict_['N']
            if re.match("^-?\\d+?\\.\\d+?$", value) is not None:
                value = float(value)
            else:
                value = int(value)
        elif 'B' in dict_:
            value = str(dict_['B'])
        elif 'NS' in dict_:
            value = set(dict_['NS'])
        elif 'BS' in dict_:
            value = set(dict_['BS'])
        elif 'M' in dict_:
            value = dict_['M']
        elif 'L' in dict_:
            value = dict_['L']
        elif 'NULL' in dict_ and dict_['NULL'] is True:
            value = None
        else:
            is_dynamodb_object = False

        return is_dynamodb_object, value
