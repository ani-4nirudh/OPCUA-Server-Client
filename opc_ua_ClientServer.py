import sys
import time
from opcua import ua, Server, Client
import threading
import logging
import random

SERVER_ENDPOINT = "opc.tcp://localhost:5555"
NAMESPACE_URL = "OPC_SIMULATION"
NODE_NAME = "Parameters" 
TEMP_VARIABLE = "Temperature" 
PRESS_VARIABLE = "Pressure" 
TIME_VARIABLE = "Time" 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

stop_event = threading.Event()

'''Setting up the server'''
def opcuaServer():
    # Create a server object 
    server = Server()
    try:
        server.set_endpoint(SERVER_ENDPOINT) # Give the URL to the server

        # Setup a namespace
        address_space = server.register_namespace(NAMESPACE_URL)

        # Get root node which defines our address space
        node = server.get_objects_node()

        # Defining nodes which store parameters
        Params = node.add_object(address_space, NODE_NAME)
        Temp = Params.add_variable(address_space, TEMP_VARIABLE, 0)
        Press = Params.add_variable(address_space, PRESS_VARIABLE, 0)
        Time = Params.add_variable(address_space, TIME_VARIABLE, 0)
        
        # Set myVar to be writable by clients
        Temp.set_writable() 
        Press.set_writable()
        Time.set_writable()

        server.start()
        logging.info("OPC UA Server started")

        count = 0
        while not stop_event.is_set(): # After stop_event is set, the while loop terminates in the next iteration
            time.sleep(1)
            count += 1
            try:
                Temp.set_value(random.randint(20, 50))
                Press.set_value(random.randint(1, 20))
                Time.set_value(count)
            except Exception as e:
                logging.error(f"Error setting value: {e}")
    except Exception as e:
        logging.error(f"Server error: {e}")
    finally:
        server.stop() # Stop server thread and log
        logging.info("OPC UA Server stopped.")


'''Setup the client'''
def opcuaClient():
    time.sleep(2)  # Allow time for the server to start
    
    client = Client(SERVER_ENDPOINT)
    
    try:
        client.connect()
        logging.info(f"OPC UA client connected to {SERVER_ENDPOINT}")
        client.load_type_definitions()

        root = client.get_root_node()
        logging.info(f"Root node is: {root}")

        objects = client.get_objects_node()
        logging.info(f"Objects node is: {objects}")

        logging.info(f"Children of root are: {root.get_children()}")

        idx = client.get_namespace_index(NAMESPACE_URL)
        logging.info(f"NAMESPACE Index: {idx}")

        while not stop_event.is_set():
            time.sleep(1)

            TempVar = root.get_child(["0:Objects", "{}:Parameters".format(idx), "{}:Temperature".format(idx)])
            TempObj = root.get_child(["0:Objects", "{}:Parameters".format(idx)])
            Temp = client.get_node(TempVar)
            Temperature = Temp.get_value()
            print("#######################################################")
            print(f"############ Temperature: {Temperature} ##############")
            print("#######################################################")

            PressVar = root.get_child(["0:Objects", "{}:Parameters".format(idx), "{}:Pressure".format(idx)])
            PressObj = root.get_child(["0:Objects", "{}:Parameters".format(idx)])
            Press = client.get_node(PressVar)
            Pressure = Press.get_value()
            print("#######################################################")
            print(f"############## Pressure: {Pressure} ###############")
            print("#######################################################")

            TimeVar = root.get_child(["0:Objects", "{}:Parameters".format(idx), "{}:Time".format(idx)])
            TimeObj = root.get_child(["0:Objects", "{}:Parameters".format(idx)])
            Time_var = client.get_node(TimeVar)
            Time = Time_var.get_value()
            print("#######################################################")
            print(f"############## Time: {Time} ###############")
            print("#######################################################")

    except Exception as e:
        logging.error(f"OPC UA client error: {e}")

    finally:
        try:
            client.disconnect() # Disconnect and log
            logging.info("OPC UA Client disconnected.")
        except Exception as e:
            logging.error(f"Error disconnecting the client: {e}")

def main():
    '''
    Termination with Daemon Threads: If you had kept daemon=True, the main thread would continue executing until it reached the end of the main() function. 
    At that point, the Python interpreter would terminate, and any remaining daemon threads would be abruptly stopped, regardless of their current state. 
    This is why there is a potential for the finally blocks not to complete their logging.
    '''
    server_thread = threading.Thread(target=opcuaServer, daemon=False, name="OPCUA_Server_Thread")
    client_thread = threading.Thread(target=opcuaClient, daemon=False, name="OPCUA_Client_Thread")

    server_thread.start()
    time.sleep(2)
    client_thread.start()

    try:
        while server_thread.is_alive() or client_thread.is_alive():
            time.sleep(0.1) # Small time to avoid busy waiting
    except KeyboardInterrupt:
        logging.info("Stopping threads...")
        stop_event.set() # Set a flag when Ctrl+C is pressed
        
        '''The following is needed for daemon=True'''
        # time.sleep (1) # Give time for threads to detect stop_event flag, execute finally blocks and log

    logging.info("Waiting for threads to finish...")
    # join() ensures main thread waits for server and client thread to finish their while loops and execute their finally blocks
    server_thread.join()
    client_thread.join()
    logging.info("All threads have finished.")
    logging.info("Exiting main thread...")

if __name__ == "__main__":
    main()