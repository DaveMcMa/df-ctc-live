import requests
import base64
import json
import pandas as pd

class HBaseRest():
  def __init__(self, user, password, rest_node, rest_node_ip, rest_node_port):
    self.user = user
    self.password = password
    self.host = rest_node
    self.ip = rest_node_ip
    self.port = rest_node_port
    self.url = "https://" + rest_node + ":" + rest_node_port
    requests = __import__('requests')
    
  def get_version(self):
    '''
    Version of HBase running on this cluster
    Parameters
        ----------
    '''
    url = self.url + "/version/cluster"
    return(requests.get(url, auth = (self.user, self.password), verify=False))
    
  def get_cluster_status(self):
    '''
    HBase Cluster status
    Parameters
        ----------
    '''
    url = self.url + "/status/cluster"
    return(requests.get(url, auth = (self.user, self.password), verify=False))
  
  def get_nonsystem_tables(self):
    '''
    List of all HBase nonsystem tables - does NOT list Data Fabric tables
    Returns a json dict as a string with a list of nonsystem tables -> {"table":[{"name":"<namespace>:<tablename>"}]}
    Convert string to dict with "<class instance var>.get_nonsystem_tables().json()" when calling the function

    Parameters
        ----------
    '''
    url = self.url + "/"
    return(requests.get(url, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))
  
  def get_namespaces(self):
    '''
    List all HBase namespaces.
    Returns a json dict as a string with a list of namespace -> {"Namespace":["<ns1>","<ns2>","<ns3>"]}
    Convert string to dict with "<class instance var>.get_namespaces().json()" when calling the function

    Parameters
        ----------
    '''
    url = self.url + "/namespaces"
    return(requests.get(url, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))

  def get_namespace_description(self, namespace):
    '''
    Describe a specific namespace.

    Parameters
        ----------
    '''
    url = self.url + "/namespaces/" + namespace
    return(requests.get(url, auth = (self.user, self.password), verify=False))
  
  def get_tables_in_namespace(self, namespace):
    '''
    Describe a specific namespace.

    Parameters
        ----------
    '''
    url = self.url + "/namespaces/" + namespace + '/tables'
    return(requests.get(url, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))
  
class HBaseRestTable(HBaseRest):
    def __init__(self, user, password, rest_node, rest_node_ip, rest_node_port, table):
        requests = __import__('requests')
        base64 = requests = __import__('base64')
        json = requests = __import__('json')
        pandas = requests = __import__('pandas')
        super().__init__(user, password, rest_node, rest_node_ip, rest_node_port)
        self.table = table.replace("/", "%2F").replace(":", "%3A")

    def get_table_schema(self):
      url = self.url + "/" + self.table + "/schema"
      return(requests.get(url, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))
    
    def get_table_regions(self):
      url = self.url + "/" + self.table + "/regions"
      return(requests.get(url, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))

    def insert(self, data):
      '''
      Write rows to a table. 
      The row, column qualifier, and value must each be Base-64 encoded.

      Parameters
          ----------
          table_name : str
              name of the HBase or Data Fabric (binary) table
          data : str
              Data to write to a table.
              The string needs to be in the following format:
                {"Row":[<row definition>,<row definition>,<row definition>,< more row definitions ....>]}
              A row definition needs to be in the following format:
                {"key":"<row key>","Cell":[<column definition>,<column definition>, <more column definition ....>},
                {"column":"<cf:column qualifier>","$":"<value>"},{"column":"<column qualifier>","$":"<value>"}, <add more columns.....>}, {"key":"<row key>","Cell":[{"column":"<column qualifier>","$":"<value>"},{"column":"<column qualifier>","$":"<value>"}, <add more columns.....>},
      '''
      url = self.url + "/" + self.table + "/dummyrowkey"
      return(requests.put(url, auth = (self.user, self.password), data=data, headers={'Accept': 'application/json', 'Content-Type': 'application/json'}, verify=False))
    
    def read_batch(self, scanner):
      return(requests.get(scanner, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))
    
    def create_scanner(self, filter):
       url = self.url + "/" + self.table + "/scanner"
       res = requests.put(url, data = filter, auth = (self.user, self.password), headers={'Accept': 'application/xml', 'Content-Type': 'text/xml'}, verify=False)
       return(res.headers["Location"])

    def delete_scanner(self, scanner):
       return(requests.delete(scanner, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False))

    def read_full_table(self, scanner):
      all_data = []
      status_code = 200
      while status_code == 200:
          res = self.read_batch(scanner)
          #res = requests.get(scanner, auth = (self.user, self.password), headers={'Accept': 'application/json'}, verify=False)
          status_code = res.status_code
          if status_code == 200:
              all_data.append(res.text)
      return(all_data)
    
    def scan_table_by_prefix(self, row_prefix, column_family=None):
        """
        Scan table for rows matching a prefix pattern.
        Returns dict of {row_key: {column_name: value}}
        
        Parameters:
            row_prefix: str - prefix to match row keys (e.g., "file:")
            column_family: str - optional column family to filter (e.g., "file_metadata")
        """
        # Create scanner filter XML
        if column_family:
            filter_xml = f'<Scanner batch="100"><filter>{{"type":"PrefixFilter","value":"{base64.b64encode(row_prefix.encode()).decode()}"}}</filter><column>{base64.b64encode(column_family.encode()).decode()}</column></Scanner>'
        else:
            filter_xml = f'<Scanner batch="100"><filter>{{"type":"PrefixFilter","value":"{base64.b64encode(row_prefix.encode()).decode()}"}}</filter></Scanner>'
        
        # Create scanner
        scanner_url = self.create_scanner(filter_xml)
        
        # Read all data
        all_data = self.read_full_table(scanner_url)
        
        # Delete scanner
        self.delete_scanner(scanner_url)
        
        # Parse results into dict
        results = {}
        for batch in all_data:
            if batch:  # Check if batch is not empty
                batch_json = json.loads(batch)
                if "Row" in batch_json:
                    for row in batch_json["Row"]:
                        row_key = base64.b64decode(row["key"]).decode('utf-8')
                        results[row_key] = {}
                        
                        for cell in row["Cell"]:
                            # Decode column family:qualifier
                            col_full = base64.b64decode(cell["column"]).decode('utf-8')
                            # Get just the qualifier (after the colon)
                            col_name = col_full.split(":", 1)[1] if ":" in col_full else col_full
                            # Decode value
                            value = base64.b64decode(cell["$"]).decode('utf-8')
                            results[row_key][col_name] = value
        
        return results

    def hbase_to_df(self, columns_of_interest, all_data, weekenddata_column_family, telemetry_column_family):
        # add uuid to columns_of_interest
        columns_of_interest = "uuid," + columns_of_interest
        # add TrackID to columns_of_interest
        columns_of_interest = "TrackID," + columns_of_interest
        # initalize dict for columns of interest
        data_dict = {}
        for index, col in enumerate(columns_of_interest.replace(" ", "").split(',')):
            data_dict[col] = []
            
        # loop over all batch returns
        for batch in all_data:
            for row in json.loads(batch)["Row"]:
                key = base64.b64decode(row["key"]).decode('utf-8')
                for column in row["Cell"]:
                    # filter weekenddata
                    if base64.b64decode(column["column"]).decode('utf-8')[:len(weekenddata_column_family)] != weekenddata_column_family:
                        column_name = base64.b64decode(column["column"]).decode('utf-8')[len(telemetry_column_family)+1:]
                        value = base64.b64decode(column["$"]).decode('utf-8')
                        data_dict[column_name].insert(int(key.split(":")[1]),value)

        #df = pd.DataFrame(data_dict).sort_values("SessionTick", key=pd.to_numeric)
        df = pd.DataFrame(data_dict)
        #df["SessionTick"] = pd.to_numeric(df["SessionTick"])
        #df = df.sort_values(["uuid", "SessionTick"], ascending=[True, True])
        return(df)