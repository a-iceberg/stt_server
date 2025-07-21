from init_queue import stt_server
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("Initializing queue cleaning")
    server = stt_server()
    server.clean_queue()
    logging.info("Queue has been cleaned successfully") 