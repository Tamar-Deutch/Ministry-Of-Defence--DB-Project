
#=============================== imports ====================================
import csv
import json
import linecache
import os
from typing import Any, Dict, List, Type
import db_api
import datetime as dt
import operator

ops = {
    '<': operator.lt,
    '<=': operator.le,
    '=': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt
}

#========================== Auxiliary function ================================

#-------------------------- work-with-json-file -------------------------------

def read_json_file(file_name):
    print("read")
    with open(f"{db_api.DB_ROOT}\\{file_name}.json", "r", encoding="utf8") as file:
        table = json.load(file)
        print(type(table))

    return table


def write_to_json_file(file_name, table):
    with open(f"{db_api.DB_ROOT}\\{file_name}.json", "w+", encoding="utf8") as file:
        json.dump(table, file, default=str)


def update_json_file(file_name, key, update = ""):
    with open(f"{db_api.DB_ROOT}\\{file_name}.json", "r") as jsonFile:
        data = json.load(jsonFile)
        if update == "":
            del data[key]
        else:
            data[key] = update
    with open(f"{db_api.DB_ROOT}\\{file_name}.json", "w") as jsonFile:
        json.dump(data, jsonFile)


#-------------------------- work-with-csv-file --------------------------------

def write_to_csv(file_name, rows:List[List]):
    with open(f'{db_api.DB_ROOT}\\{file_name}.csv', 'a', newline='') as file:
        csv_writer = csv.writer(file)
        for row in rows:
            csv_writer.writerow(row)

def exist_in_csv(file_name, key):
    with open(f'{db_api.DB_ROOT}\\{file_name}.csv', 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if key in row:
                return 1
    return 0


# ====================== class DB fields ====================================

class DBField(db_api.DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type

# =========================class DB table ===================================

class DBTable(db_api.DBTable):

    def query_table(self, criteria: List[db_api.SelectionCriteria]) -> List[Dict[str, Any]]:
        list = []
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                print (row)
                flag = 0
                for item in criteria:
                    operation = ops.get(item.operator)
                    if not operation (row[item.field_name], item.value):
                        flag = 1
            if flag == 0:
                list.append(row)

        return list


    def count(self) -> int:
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', 'r') as file:
            table = csv.reader(file)
            return len(list(table))-1


    def insert_record(self, values: Dict[str, Any]) -> None:

        if exist_in_csv(self.name, str(values[self.key_field_name])):
            raise ValueError

        values_to_insert = []
        for value in values.values():
            values_to_insert.append(value)
        write_to_csv(self.name, [values_to_insert])


    def delete_record(self, key: Any) -> None:
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', 'r') as file:
            csv_reader = csv.reader(file)
            records_to_remain = [next(csv_reader)]
            flag = 0
            for row in csv_reader:
                if str(key) in row:
                    flag = 1
                try:
                    if (row[0]) != str(key):
                        records_to_remain.append(row)
                except:
                    pass
            if flag == 0:
                raise ValueError

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', 'w',newline= "") as file:
            writer = csv.writer(file)
            for record in records_to_remain:
                writer.writerow(record)
        file.close()


    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                flag = 0
                for item in criteria:
                    if item.operator == "=":
                        item.operator = "=="
                    check = row[item.field_name]+ item.operator + str(item.value)
                    if not eval(check):
                        flag = 1
                if flag == 0:
                    self.delete_record(row[self.key_field_name])


    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if str(key) == row[self.key_field_name]:
                    return row


    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        item_to_update = self.get_record(key)
        for i in values:
              item_to_update[i] = values[i]
        self.delete_record(key)
        self.insert_record(item_to_update)


# ========================= class DataBase =================================

#------------------------- Auxiliary function ------------------------------
META_DATA = "meta"
def check_bad_key_error(fields: List[db_api.DBField], key: str):
    for field in fields:
        if key == field.name:
            return
    raise ValueError("Bad Key")

def check_bad_table_name_error(table_name):
    meta_data_table = read_json_file(META_DATA)
    if table_name in meta_data_table.keys():
        raise ValueError("table name exists")

#---------------------------- The class ------------------------------------

class DataBase(db_api.DataBase):

    def create_table(self, table_name: str, fields: List[db_api.DBField], key_field_name: str) -> DBTable:
        check_bad_key_error(fields,key_field_name)

        if not os.path.exists(f"{db_api.DB_ROOT}\\{META_DATA}.json"):
            write_to_json_file(META_DATA, {})

        check_bad_table_name_error(table_name)

        write_to_csv(table_name,[[field.name for field in fields]])
        new_table = DBTable(table_name, fields, key_field_name)
        write_to_json_file(META_DATA, {table_name:key_field_name})

        return new_table


    def num_tables(self) -> int:
        if not os.path.exists(f"{db_api.DB_ROOT}\\{META_DATA}.json"):
            return 0
        return len(read_json_file(META_DATA))


    def get_table(self, table_name: str) -> DBTable:
        meta_data_table = read_json_file(META_DATA)
        if table_name not in meta_data_table.keys():
            raise ValueError("table name does not exist")
        fields = linecache.updatecache(f'{db_api.DB_ROOT}\\{table_name}.csv')[0][:-1].split(",")
        table = DBTable(table_name, fields, meta_data_table[table_name])
        return table


    def delete_table(self, table_name: str) -> None:
        os.remove(f'{db_api.DB_ROOT}\\{table_name}.csv')
        update_json_file(META_DATA,table_name)


    def get_tables_names(self) -> List[Any]:
        tables = read_json_file(META_DATA)
        return list(tables.keys())


    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

