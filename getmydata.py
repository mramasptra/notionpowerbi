import requests
import pandas as pd

#Your token here
token ='YOUR_TOKEN'

payload_dname = {
    "filter": {
        "value": "database",
        "property": "object"
    },
    "page_size": 100
}


headers = {
    "Authorization": "Bearer " + token,
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}


class NotionSync:
    def __init__(self):
        pass


    # search database name
    def  notion_search(self,integration_token = token):
        url = "https://api.notion.com/v1/search"
        results = []
        next_cursor = None

        while True:
            if next_cursor:
                payload_dname["start_cursor"] = next_cursor

            response = requests.post(url, json=payload_dname, headers=headers)

            if response.status_code != 200:
                return 'Error: ' + str(response.status_code)
                exit(0)
            else:
                response_json = response.json()
                results.extend(response_json["results"])
                next_cursor = response_json.get("next_cursor")

            if not next_cursor:
                break

        return {"results": results}


    # query database details
    def  notion_db_details(self,database_id,integration_token = token):
        url = f"https://api.notion.com/v1/databases/" + database_id + "/query"
        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            return 'Error: ' + str(response.status_code)
            exit(0)
        else:
            return response.json()

    # to get databases id and name
    def get_databases(self,data_json):
        databaseinfo = {}
        databaseinfo["database_id"] = [data_json["results"][i]["id"].replace("-","")
                                                for i in range(len(data_json["results"])) ]

        databaseinfo["database_name"] = [data_json["results"][i]["title"][0]["plain_text"]
                                                  if data_json["results"][i]["title"]
                                                  else ""
                                                  for i in range(len(data_json["results"])) ]

        databaseinfo["url"] = [ data_json["results"][i]["url"]
                                         if data_json["results"][i]["url"]
                                         else ""
                                         for i in range(len(data_json["results"])) ]
        return databaseinfo


    # to get column title of the table
    def get_tablecol_titles(self,data_json):
        return list(data_json["results"][0]["properties"].keys())
    
    # to get column data type for processing by type due to data structure is different by column type
    def get_tablecol_type(self,data_json,columns_title):
        type_data = {}
        for t in columns_title:
            type_data[t] = data_json["results"][0]["properties"][t]["type"]
        return type_data

    # to get table data by column type
    def get_table_data(self,data_json,columns_type):
        table_data = {}
        for k, v in columns_type.items():
            # to check column type and process by type
            if v in ["checkbox","number","email","phone_number"]:
                table_data[k] = [ data_json["results"][i]["properties"][k][v]
                                    if data_json["results"][i]["properties"][k][v]
                                    else ""
                                    for i in range(len(data_json["results"]))]
            elif v == "date":
                table_data[k] = [ data_json["results"][i]["properties"][k]["date"]["start"]
                                    if data_json["results"][i]["properties"][k]["date"]
                                    else ""
                                    for i in range(len(data_json["results"])) ]
            elif v == "rich_text" or v == 'title':
                table_data[k] = [ data_json["results"][i]["properties"][k][v][0]["plain_text"]
                                    if data_json["results"][i]["properties"][k][v]
                                    else ""
                                    for i in range(len(data_json["results"])) ]
            elif v == "files":
                table_data[k + "_FileName"] = [ data_json["results"][i]["properties"][k][v][0]["name"]
                                                if data_json["results"][i]["properties"][k][v]
                                                else ""
                                                for i in range(len(data_json["results"])) ]
                table_data[k + "_FileUrl"] = [ data_json["results"][i]["properties"][k][v][0]["file"]["url"]
                                           if data_json["results"][i]["properties"][k][v]
                                                else ""
                                           for i in range(len(data_json["results"])) ]
            elif v == "select":
                table_data[k] = [data_json["results"][i]["properties"][k][v]["name"]
                                    if data_json["results"][i]["properties"][k][v]
                                    else ""
                                    for i in range(len(data_json["results"]))]
            elif v == "people":
                table_data[k + "_Name"] = [ [data_json["results"][i]["properties"][k][v][j]["name"]
                                                if data_json["results"][i]["properties"][k][v]
                                                # to check if key 'name' exists in the list
                                                and "name" in data_json["results"][i]["properties"][k][v][j].keys()
                                                else ""
                                                for j in range(len(data_json["results"][i]["properties"][k][v]))]
                                                for i in range(len(data_json["results"])) ]
            elif v == "multi_select":
                table_data[k] = [ [data_json["results"][i]["properties"][k][v][j]["name"]
                                  if data_json["results"][i]["properties"][k][v]
                                  else ""
                                  for j in range(len(data_json["results"][i]["properties"][k][v]))]
                                  for i in range(len(data_json["results"])) 
                                ]

        return table_data    


if __name__=='__main__':
    nsync = NotionSync()

    # to search all databases.
    data = nsync.notion_search()

    # to get database id and name.
    dbid_name = nsync.get_databases(data)

    #convert dictionary to dataframe.
    df = pd.DataFrame.from_dict(dbid_name)

    # convert to bool and then drop record with empty databasae name.
    df = df[df['database_name'].astype(bool)]
    print (df)


    # to loop through database id and get the database details.
    for d in dbid_name["database_id"]:
        # notion given another API to get the details of databases by database id. search API does not return databases details.
        dbdetails = nsync.notion_db_details(d)

        # get column title
        columns_title = nsync.get_tablecol_titles(dbdetails)

        # get column type
        columns_type = nsync.get_tablecol_type(dbdetails,columns_title)

        # get table data
        table_data = nsync.get_table_data(dbdetails,columns_type)

        #convert dictionary to dataframe
        globals()[f"df{d}"] = pd.DataFrame.from_dict(table_data)
        print(globals()[f"df{d}"])
