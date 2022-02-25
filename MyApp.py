from core.Setting import Setting
from db.DbOperation import DbOperation
import csv
import logging




class MyApp(Setting):

    def __init__(self, log_file_name):
        super().__init__(log_file_name)
        db_operation = DbOperation(db_name="carbon_nano_tube")
        #db_operation.create_collection("carbons_nano_coll")
        db_operation.connect_current_collection("carbons_nano_coll")

        #db_operation.insert_bulk_data(self.get_kv_carbon_file())
        #db_operation.insert_only_one({"user":"self","age":25})
        #db_operation.get_all_data()
        #db_operation.drop_current_coll()
        #db_operation.delete_all_document()
        #db_operation.filter_documents({"user":"self"})
        db_operation.update(match={"user":"John"},set_data={"user":"Self"},is_many=False)





    def get_kv_carbon_file(self):
        """parsing data from carbon nano tube csv file
         source from uci """

        try:
            logging.info("try to parse data from csv file")
            with open("asset//carbon_nanotubes.csv") as f:
                data = csv.reader(f,delimiter="\n")
                k = data.__next__()[0].split(";")
                keys = []
                for i in k:
                    keys.append( i.replace(" ","_").lower())
                documents = []
                for i in data:
                    d = {}
                    v = i[0].split(";")
                    for index,j in enumerate(v):
                        d[keys[index]] = j
                    documents.append(d)
                logging.info("Successfully import data from csv file")
                return documents
        except Exception as e:
            logging.error(e)



if __name__=='__main__':
    MyApp("mongo_db.log")