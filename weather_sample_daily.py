"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
from queue import Queue
from threading import Thread
from time import time, sleep
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
from urllib.request import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
# SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/daily.py?"

def download_data(site, service):

    faaid = site['properties']['sid']
    sitename = site['properties']['sname']
    uri = '%s&stations=%s' % (service, faaid)
    print(('Downloading: %s [%s]'
           ) % (sitename, faaid))
    print(uri)
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode('utf-8')
            if data is not None and not data.startswith('ERROR'):
                outfn = 'Daily/%s.txt' % faaid
                out = open(outfn, 'w')
                out.write(data)
                out.close()
                return
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            count = 0
            sleep(5)
        attempt += 1
    print("failed to download %s" % faaid)
    return


class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            site, service = self.queue.get()
            download_data(site, service)
            self.queue.task_done()


def main():
    """Our main method"""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2005, 1, 31)
    endts = datetime.datetime(2018, 1, 31)

    states = """KS NE OK"""
    networks = []
    for state in states.split():
        networks.append("%s_ASOS" % (state,))

    for network in networks:
        # Get metadata
        uri = ("https://mesonet.agron.iastate.edu/"
               "geojson/network/%s.geojson") % (network,)

        service = SERVICE + "network=%s&" % network

        service += startts.strftime('year1=%Y&month1=%m&day1=%d&')
        service += endts.strftime('year2=%Y&month2=%m&day2=%d&')

        data = urlopen(uri)
        jdict = json.load(data)

        ts = time()
        # Create a queue to communicate with the worker threads
        queue = Queue()
        # Create 8 worker threads
        for x in range(10):
            worker = DownloadWorker(queue)
            # Setting daemon to True will let the main thread exit even though the workers are blocking
            worker.daemon = True
            worker.start()
        # Put the tasks into the queue as a tuple
        print(len(jdict['features']))
        for site in jdict['features']:
            logger.info('Queueing {}'.format(site['properties']['sid']))
            queue.put((site, service))

        # Causes the main thread to wait for the queue to finish processing all the tasks
        queue.join()
        print('Took {}'.format(time() - ts))


if __name__ == '__main__':
    main()
