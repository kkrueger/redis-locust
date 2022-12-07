from locust import User, task, events
from locust.runners import MasterRunner
import boto3
import logging
import time
import random
import string
import numpy

global myDynamoDb

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--table_name", type=str, env_var="RED_LOCUST_TABLE_NAME", default="Log", help="DynamoDB table name")
    parser.add_argument("--key_name_prefix", type=str, env_var="RED_LOCUST_KEY_NAME_PREFIX", default="rloc:", help="Prefix for key names")
    parser.add_argument("--key_name_length", type=int, env_var="RED_LOCUST_KEY_NAME_LENGTH", default=20, help="Length (ie digits) of key name (not including prefix)")
    parser.add_argument("--number_of_keys", type=int, env_var="RED_LOCUST_NUM_OF_KEYS", default=1000000, help="Number of keys")
    parser.add_argument("--value_min_chars", type=int, env_var="RED_LOCUST_VALUE_MIN_BYTES", default=15, help="Minimum characters to store in key value")
    parser.add_argument("--value_max_chars", type=int, env_var="RED_LOCUST_VALUE_MAX_BYTES", default=15, help="Maximum characters to store in key value")
    parser.add_argument("--zipf_shape", type=float, env_var="RED_LOCUST_ZIPF_SHAPE", default=1.01, help="Zipf shape")
    parser.add_argument("--zipf_direction", type=int, env_var="RED_LOCUST_ZIPF_DIRECTION", default=1, help="Zipf direction [1|-1]")
    parser.add_argument("--zipf_max_keys", type=int, env_var="RED_LOCUST_ZIPF_MAX_KEYS", default=10000000, help="Zipf max keys")
    parser.add_argument("--zipf_offset", type=int, env_var="RED_LOCUST_ZIPF_OFFSET", default=0, help="Zipf Offset")
    parser.add_argument("--zrem_seconds", type=int, env_var="RED_LOCUST_ZREM_SECONDS", default=300, help="Seconds to keep when trimming zsets")
    parser.add_argument("--pipeline_size", type=int, env_var="RED_LOCUST_PIPELINE_SIZE", default=100, help="Commands per Redis pipeline")
    parser.add_argument("--zcount_seconds", type=int, env_var="RED_LOCUST_ZCOUNT_SECONDS", default=150, help="Number of seconds to query for zcount")
    parser.add_argument("--jumbo_frequency", type=int, env_var="RED_LOCUST_JUMBO_FREQUENCY", default=50, help="Frequency of jumbo zadd logic")
    parser.add_argument("--jumbo_initial_exclude", type=int, env_var="RED_LOCUST_JUMBO_INITIAL_EXCLUDE", default=100, help="Number of initial keys to exclude from jumbo logic")
    parser.add_argument("--jumbo_size", type=str, env_var="RED_LOCUST_JUMBO_SIZE", default="25,25,50,100,1000", help="Array representing the extra members for jumbo zadds")
    parser.add_argument("--version_display", type=str, env_var="RED_VERSION_DISPLAY", default="0.2", help="Just used to show locust file version in UI")

class DynamoDbDataLayer():

    def __init__(self, environment):
        self.environment = environment

    def get_key_int(self):
        """
        Function to generate pick integer to use for creation of key name(s)
        Implements zipf distribution, with shape, direction, and offset controlled by locust params
        """

        x = self.environment.parsed_options.zipf_max_keys + 1
        while x > self.environment.parsed_options.zipf_max_keys:
            x = numpy.random.zipf(a=self.environment.parsed_options.zipf_shape, size=1)[0]

        return(self.environment.parsed_options.zipf_offset + (x * self.environment.parsed_options.zipf_direction))

    def get_key_name_from_int(self, key_int):
        """
        Function to generate a key name string from an integer
        Implements zero filling based on locust parameter
        """

        return(''.join((self.environment.parsed_options.key_name_prefix, str(key_int).zfill(self.environment.parsed_options.key_name_length))))

    def record_request_meta(self, request_type, name, start_time, end_time, response_length, response, exception):
        """
        Function to record locust request, based on standard locust request meta data
        repsonse time is calculated form inputs and is expressed in microseconds
        """

        request_meta = {
            "request_type": request_type,
            "name": name,
            "start_time": start_time,
            "response_time": (time.perf_counter() - start_time) * 1000 * 1000,
            "response_length": response_length,
            "response": response,
            "context": {},
            "exception": exception }

        if exception:
            events.request_failure.fire(**request_meta)
        else:
            events.request_success.fire(**request_meta)

    def count(self,dynamoClient):
        """
        Function to count items in a Redis sorted Set.
        Will count against active-active setup and stand-alone setup, recording requests to locust.
        """

        # Prepare data for below sections
        transtime = time.time()
        keyint = self.get_key_int()
        keyname = self.get_key_name_from_int(keyint)
        
        myResponse = None
        myException = None
        trans_start_time = time.perf_counter()
        try:
            myResponse = dynamoClient.query(TableName=self.environment.parsed_options.table_name, Select='COUNT', 
                KeyConditionExpression='Id = :id AND EventDate BETWEEN :startdate AND :enddate',
                ExpressionAttributeValues={":id": {"S":keyname}, ":startdate": {"S":str(transtime-self.environment.parsed_options.zcount_seconds)}, ":enddate": {"S":str(transtime)}})            
            if "LastEvaluatedKey" in myResponse:
                raise Exception("Looks like we need to implement paging for the count query")
        except Exception as e:
            myException = e

        self.record_request_meta(
            request_type = "",
            name = "count",
            start_time = trans_start_time,
            end_time = time.perf_counter(),
            response_length = 0,
            response = myResponse,
            exception = myException)

    def add(self,dynamoClient):
        """
        Function that will add recent transactions to sorted set, and then delete older transactions from the same sorted set.  Will
        pick keys for actions and implements jumbo adds according to locust parameters.
        Functions against active-active Redis and against a pair of stand-alone Redis instances, sending writes to all three locations.
        """

        # Build keys and member logic for use in later commands
        baseRequestName = "add"
        keyint = self.get_key_int()
        transtime = time.time()

        transaction_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(self.environment.parsed_options.value_min_chars, self.environment.parsed_options.value_max_chars)))

        #TODO: Do we want jumbo logic?
        # orig_keyint = (keyint - self.environment.parsed_options.zipf_offset ) * self.environment.parsed_options.zipf_direction
        # if ((orig_keyint > self.environment.parsed_options.jumbo_initial_exclude)  and (keyint % self.environment.parsed_options.jumbo_frequency == 0) ):
        #     baseRequestName = "zadd_jumbo"
        #     count = int(random.choice(self.environment.parsed_options.jumbo_size.split(',')))
        #     for i in range(count):
        #         members.update( {''.join(str(i)).join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(self.environment.parsed_options.value_min_chars, self.environment.parsed_options.value_max_chars))): time.time()})

        myResponse = None
        myException = None
        trans_start_time = time.perf_counter()
        try:
            #TODO: Include member value?
            myResponse = dynamoClient.put_item(TableName=self.environment.parsed_options.table_name,
                Item={"Id": {"S":self.get_key_name_from_int(keyint)},"EventDate":{"S":str(transtime)}, "TransactionId":{"S":transaction_id}})

        except Exception as e:
            myException = e

        self.record_request_meta(
            request_type = "",
            name = baseRequestName,
            start_time = trans_start_time,
            end_time = time.perf_counter(),
            response_length = 0,
            response = myResponse,
            exception = myException)


        #TODO: Implement delete, or use Dynamo TTL?

        # Active-active zrem section
        # myResponse = None
        # myException = None
        # trans_start_time = time.perf_counter()
        # try:
        #     myResponse = localRedis.zremrangebyscore( \
        #         ''.join((self.environment.parsed_options.key_name_prefix, str(keyint).zfill(self.environment.parsed_options.key_name_length))), \
        #         0, transtime - self.environment.parsed_options.zrem_seconds)
        # except Exception as e:
        #     myException = e

        # self.record_request_meta(
        #     request_type = "aa",
        #     name = "zrem",
        #     start_time = trans_start_time,
        #     end_time = time.perf_counter(),
        #     response_length = 0,
        #     response = myResponse,
        #     exception = myException)

    

class DynamoDbUser(User):
    """
    Locust user class that defines tasks and weights for test runs.
    """

    global myDynamoDb

    def on_start(self):
        self.myDataLayer = DynamoDbDataLayer(self.environment)

    @task(1)
    def add(self):
        self.myDataLayer.add(myDynamoDb)
    
    @task(1)
    def count(self):
        self.myDataLayer.count(myDynamoDb)

@events.test_start.add_listener
def _(environment, **kw):
    """
    Function tagged in locust just to log some information on start-up
    """
    logging.info("Locust parameters for test run")
    logging.info((vars(environment.parsed_options)))

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """
    Function to initialize Redis connections on startup of locust workers.
    """
    global myDynamoDb    

    if isinstance(environment.runner, MasterRunner):
        logging.info("Locust master node test start")        

    else:        
        logging.info("Locust worker or stand-alone node test start")

        #TODO: need TLS?
        #myTls = (environment.parsed_options.tls == "Y")            

        #TODO: Make local mode configurable
        myDynamoDb = boto3.client('dynamodb', endpoint_url='http://localhost:8000')       

        try:
            myDynamoDb.create_table(TableName=environment.parsed_options.table_name, 
                AttributeDefinitions=[{"AttributeName":"Id","AttributeType":"S"},{"AttributeName":"EventDate","AttributeType":"S"}], 
                KeySchema=[{"AttributeName":"Id","KeyType":"HASH"}, {"AttributeName":"EventDate", "KeyType":"RANGE"}],
                ProvisionedThroughput={"ReadCapacityUnits":5, "WriteCapacityUnits":5})
        except myDynamoDb.exceptions.ResourceInUseException:
            #NOOP: table already exists
            pass 